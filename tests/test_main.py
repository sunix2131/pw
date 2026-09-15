import io
import json
import contextlib
import shutil
import subprocess
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch
import uuid

import psycopg
import requests
from eth_abi import encode

import main as app

OTHER = '0x' + '11' * 20
THIRD = '0x' + '22' * 20
BLOCK_HASH = '0x' + 'ab' * 32


def topic(address):
    return '0x' + address[2:].zfill(64)


def log(address, topics, types, values, index=0, tx=1, block=2):
    return {'address': address, 'topics': topics, 'data': '0x' + encode(types, values).hex(),
            'transactionHash': '0x' + format(tx, '064x'), 'blockHash': BLOCK_HASH,
            'blockNumber': hex(block), 'logIndex': hex(index), 'transactionIndex': hex(tx - 1), 'removed': False}


def cash(sender=app.ZERO, receiver=app.WALLET, amount=100, index=0, tx=1, address=app.USDC_E):
    return log(address, [app.TRANSFER, topic(sender), topic(receiver)], ['uint256'], [amount], index, tx)


def single(sender=app.ZERO, receiver=app.WALLET, token=7, amount=100, index=1, tx=1):
    return log(app.CTF, [app.SINGLE, topic(OTHER), topic(sender), topic(receiver)],
               ['uint256', 'uint256'], [token, amount], index, tx)


def transfer_batch(ids, amounts, sender=app.ZERO, receiver=app.WALLET, index=0):
    return log(app.CTF, [app.BATCH, topic(OTHER), topic(sender), topic(receiver)],
               ['uint256[]', 'uint256[]'], [ids, amounts], index)


def fill(maker, taker, index, version=1, side=0, token=7, making=40, taking=100, order=1):
    address = sorted(app.EXCHANGE_V1 if version == 1 else app.EXCHANGE_V2)[0]
    topics = [app.FILL_V1 if version == 1 else app.FILL_V2, '0x' + format(order, '064x'), topic(maker), topic(taker)]
    if version == 1:
        return log(address, topics, ['uint256'] * 5,
                   [0 if side == 0 else token, token if side == 0 else 0, making, taking, 2], index)
    return log(address, topics, ['uint8', 'uint256', 'uint256', 'uint256', 'uint256', 'bytes32', 'bytes32'],
               [side, token, making, taking, 2, bytes(32), bytes(32)], index)


def matched(maker, index, order=1):
    return log(sorted(app.EXCHANGE_V1)[0], [app.MATCH_V1, '0x' + format(order, '064x'), topic(maker)],
               ['uint256'] * 4, [0, 7, 40, 100], index)


def collateral_event(caller, recipient, amount, index, wrapping=True):
    return log(app.PUSD, [app.WRAPPED if wrapping else app.UNWRAPPED,
                          topic(caller), topic(app.USDC_E), topic(recipient)], ['uint256'], [amount], index)


def ctf_action(actor, kind, index, amount=100):
    topics = [kind, topic(actor), '0x' + '00' * 32, '0x' + '01' * 32]
    if kind == app.REDEEM:
        topics = [kind, topic(actor), topic(app.USDC_E), '0x' + '00' * 32]
        return log(app.CTF, topics, ['bytes32', 'uint256[]', 'uint256'], [bytes([1]) * 32, [1, 2], amount], index)
    return log(app.CTF, topics, ['address', 'uint256[]', 'uint256'], [app.USDC_E, [1, 2], amount], index)


def metadata():
    return {'format_version': app.FORMAT_VERSION, 'run_id': 'test', 'chain_id': 137, 'wallet': app.WALLET,
            'snapshot': {'number': 2, 'hash': BLOCK_HASH, 'timestamp': 1000},
            'verification_sources': ['one.example', 'two.example'],
            'cash_contracts': sorted(app.CASH), 'position_contracts': list(app.POSITIONS),
            'protocol_contracts': sorted(app.PROTOCOL)}


def receipt(logs):
    return {'transactionHash': logs[0]['transactionHash'], 'blockNumber': '0x2', 'blockHash': BLOCK_HASH,
            'transactionIndex': logs[0]['transactionIndex'], 'status': '0x1', 'from': OTHER,
            'to': app.CTF, 'logs': logs}


class RpcTests(unittest.TestCase):
    def test_invalid_arguments_exit_one_without_starting_database(self):
        for args in (['--workers', '0'], ['--unknown'], ['--fresh', '--replay-only']):
            with self.subTest(args=args), contextlib.redirect_stderr(io.StringIO()):
                with patch.object(app, 'LocalPostgres') as database:
                    self.assertEqual(app.main(args), 1)
                    database.assert_not_called()

    def client(self, handler, attempts=3):
        client = app.Rpc(['https://one.example', 'https://two.example'], attempts=attempts, sleep=lambda _: None)
        session = Mock()
        session.post.side_effect = lambda url, **kwargs: handler(url, kwargs['json'], kwargs['timeout'])
        client.local.session = session
        return client

    @staticmethod
    def response(body, results, status=200):
        response = Mock(status_code=status)
        response.raise_for_status.return_value = None
        response.json.return_value = results
        response.iter_content.side_effect = lambda **kwargs: iter([json.dumps(response.json.return_value).encode()])
        return response

    def test_partial_failure_retries_only_unresolved(self):
        requests_seen = []
        def handler(url, json, timeout):
            rows = json if isinstance(json, list) else [json]
            requests_seen.append([item['params'][0] for item in rows])
            result = [{'jsonrpc': '2.0', 'id': item['id'],
                       **({'error': {'code': -32000}} if item['params'][0] == 'B' and len(requests_seen) == 1
                          else {'result': item['params'][0]})} for item in rows]
            return self.response(json, result if isinstance(json, list) else result[0])
        client = self.client(handler)
        self.assertEqual(client.batch([('test', ['A']), ('test', ['B'])]), ['A', 'B'])
        self.assertEqual(requests_seen, [['A', 'B'], ['B']])

    def test_out_of_order(self):
        def handler(url, json, timeout):
            return self.response(json, [{'jsonrpc': '2.0', 'id': item['id'], 'result': item['params']} for item in reversed(json)])
        self.assertEqual(self.client(handler).batch([('x', [1]), ('x', [2])]), [[1], [2]])

    def test_partial_http_500_keeps_confirmed_results(self):
        seen = []
        def handler(url, body, timeout):
            rows = body if isinstance(body, list) else [body]
            seen.append([row['params'][0] for row in rows])
            answers = [{'jsonrpc': '2.0', 'id': row['id'],
                **({'error': {'code': -32000}} if len(seen) == 1 and row['params'][0] == 'B'
                   else {'result': row['params'][0]})} for row in rows]
            response = self.response(body, answers if isinstance(body, list) else answers[0],
                                     500 if len(seen) == 1 else 200)
            if len(seen) == 1:
                response.raise_for_status.side_effect = requests.HTTPError('500')
            return response
        self.assertEqual(self.client(handler).batch([('x', ['A']), ('x', ['B'])]), ['A', 'B'])
        self.assertEqual(seen, [['A', 'B'], ['B']])

    def test_provider_batch_limit_is_remembered(self):
        seen = []
        def handler(url, body, timeout):
            rows = body if isinstance(body, list) else [body]
            seen.append(len(rows))
            limited = len(rows) > 3
            answers = [{'jsonrpc': '2.0', 'id': row['id'],
                **({'error': {'code': 31, 'message': 'Batch of more than 3 requests are not allowed on free plan'}}
                   if limited else {'result': row['params'][0]})} for row in rows]
            response = self.response(body, answers if isinstance(body, list) else answers[0], 500 if limited else 200)
            if limited:
                response.raise_for_status.side_effect = requests.HTTPError('500')
            return response
        client = self.client(handler)
        client.urls = ['https://one.example']
        calls = [('x', [number]) for number in range(10)]
        self.assertEqual(client.batch(calls), list(range(10)))
        self.assertEqual(seen, [10, 3, 3, 3, 1])
        seen.clear()
        self.assertEqual(client.batch(calls), list(range(10)))
        self.assertEqual(seen, [3, 3, 3, 1])

    def test_missing_duplicate_and_null_results(self):
        for kind in ('missing', 'duplicate', 'null'):
            with self.subTest(kind=kind):
                count = 0
                def handler(url, json, timeout):
                    nonlocal count
                    count += 1
                    rows = json if isinstance(json, list) else [json]
                    out = [{'jsonrpc': '2.0', 'id': item['id'], 'result': item['params'][0]} for item in rows]
                    if count == 1:
                        if kind == 'missing': out = out[:1]
                        if kind == 'duplicate': out = [out[0], out[0], out[1]]
                        if kind == 'null': out[1]['result'] = None
                    return self.response(json, out if isinstance(json, list) else out[0])
                self.assertEqual(self.client(handler).batch([('x', ['A']), ('x', ['B'])]), ['A', 'B'])

    def test_foreign_id_is_rejected(self):
        def handler(url, json, timeout):
            return self.response(json, {'jsonrpc': '2.0', 'id': 999999, 'result': 'wrong'})
        with self.assertRaises(app.RpcError): self.client(handler).call('x', [])

    def test_timeout_and_429_failover(self):
        for error in (requests.Timeout(), requests.HTTPError('429')):
            seen = []
            def handler(url, json, timeout):
                seen.append(url)
                if len(seen) == 1: raise error
                return self.response(json, {'jsonrpc': '2.0', 'id': json['id'], 'result': 'ok'})
            self.assertEqual(self.client(handler).call('x', []), 'ok')
            self.assertEqual(len(set(seen)), 2)

    def test_persistent_error_is_bounded(self):
        count = 0
        def handler(url, json, timeout):
            nonlocal count
            count += 1
            return self.response(json, {'jsonrpc': '2.0', 'id': json['id'],
                                       'error': {'code': -32000, 'message': 'historical state unavailable'}})
        with self.assertRaises(app.RpcError): self.client(handler).call('eth_call', [])
        self.assertEqual(count, 3)

    def test_large_batch_is_split(self):
        def handler(url, json, timeout):
            if isinstance(json, list): return self.response(json, None, 413)
            return self.response(json, {'jsonrpc': '2.0', 'id': json['id'], 'result': json['params'][0]})
        self.assertEqual(self.client(handler).batch([('x', ['A']), ('x', ['B'])]), ['A', 'B'])

    def test_single_range_limit_is_not_retried_forever(self):
        client = self.client(lambda url, json, timeout: self.response(json, None, 413))
        with self.assertRaises(app.RangeError): client.call('eth_getLogs', [{}])
        self.assertEqual(client.session().post.call_count, 1)

    def test_http_error_with_range_limit_shrinks_the_scan(self):
        response = Mock(status_code=400)
        response.json.return_value = {'jsonrpc': '2.0', 'id': 1, 'error': {'code': 35,
            'message': 'ranges over 10000 blocks are not supported on free plan'}}
        response.iter_content.return_value = [json.dumps(response.json.return_value).encode()]
        response.raise_for_status.side_effect = requests.HTTPError()
        client = self.client(lambda *args, **kwargs: response)
        with self.assertRaises(app.RangeError): client.call('eth_getLogs', [{}])
        self.assertEqual(client.session().post.call_count, 1)

    def test_slow_stream_and_oversized_response_are_bounded(self):
        client = app.Rpc(['https://one.example'], timeout=10)
        response = Mock()
        response.iter_content.return_value = [b'{}']
        with patch.object(app.time, 'monotonic', return_value=11):
            with self.assertRaises(requests.Timeout): client.read_response(response, 0)
        response.iter_content.return_value = [b'x' * (1024 * 1024)] * 65
        with patch.object(app.time, 'monotonic', return_value=1):
            with self.assertRaises(app.RangeError): client.read_response(response, 0)

    def test_receipt_headers_are_requested_in_bounded_batches(self):
        rows = [('0x' + format(index, '064x'), index, BLOCK_HASH, 0) for index in range(1, 121)]
        reads = iter([rows, []])
        def execute(query, params=None):
            if 'select number from blocks' in query:
                return []
            cursor = Mock()
            if 'count(*)' in query:
                cursor.fetchone.return_value = (120,)
            else:
                cursor.fetchall.return_value = next(reads)
            return cursor
        calls_seen = []
        def batch(calls):
            calls_seen.append(calls)
            return [{'transactionHash': params[0], 'blockNumber': hex(int(params[0], 16))}
                    if method == 'eth_getTransactionReceipt' else
                    {'number': params[0], 'hash': BLOCK_HASH, 'timestamp': '0x3e8'} for method, params in calls]
        store, rpc = Mock(), Mock()
        store.conn.execute.side_effect = execute
        rpc.batch.side_effect = batch
        with patch.object(app, 'commit_record') as commit:
            app.process_receipts(rpc, store, Mock(), 4)
        self.assertEqual(len(commit.call_args.args[2]['blocks']), 120)
        self.assertEqual(sum(len(calls) for calls in calls_seen if calls[0][0] == 'eth_getBlockByNumber'), 120)
        self.assertTrue(all(len(calls) <= 50 for calls in calls_seen))


class AccountingTests(unittest.TestCase):
    def test_receipt_block_timestamps_and_missing_field(self):
        source = receipt([cash()])
        self.assertEqual(app.receipt_block_headers([source]), {})
        source['logs'][0]['blockTimestamp'] = '0x3e8'
        self.assertEqual(app.receipt_block_headers([source]), {
            2: {'number': '0x2', 'hash': BLOCK_HASH, 'timestamp': '0x3e8'}})
        source['logs'].append(dict(source['logs'][0], blockTimestamp='0x3e9'))
        with self.assertRaises(app.DataError): app.receipt_block_headers([source])
        source['logs'] = [dict(source['logs'][0], blockTimestamp='0x0')]
        self.assertEqual(app.receipt_block_headers([source]), {})

    def test_registered_contract_addresses_are_valid(self):
        for address in app.PROTOCOL | set(app.CASH):
            self.assertEqual(app.hex_value(address, 20), address)

    def test_module_split_for_second_recipient(self):
        source = log(sorted(app.MODULES)[0],
            [app.event('PositionsSplit(address,bytes31,address,address,uint256)'), topic(OTHER),
             '0x' + '12' * 31 + '00', topic(THIRD)], ['address', 'uint256'], [app.WALLET, 100])
        rows = app.operation_rows([source])
        self.assertEqual([(row[3], row[6]) for row in rows], [('SPLIT', 100)])
        self.assertEqual(rows[0][7].obj['recipient1'], app.WALLET)

    def test_historical_combo_abi_preserves_recipient_arrays(self):
        source = log('0x30000034706c7d8e12009dab006be20000c031a8',
            [app.event('SplitOnEvent(address,bytes31,bytes29,address[],uint256)'), topic(OTHER),
             '0x' + '12' * 31 + '00', '0x' + '34' * 29 + '000000'],
            ['address[]', 'uint256'], [[THIRD, app.WALLET], 100])
        rows = app.operation_rows([source])
        self.assertEqual([(row[3], row[6]) for row in rows], [('SPLIT', 100)])
        self.assertEqual(rows[0][7].obj['recipients'], [THIRD, app.WALLET])

    def test_auto_redeemer_keeps_multiple_redemptions_without_internal_duplicates(self):
        module = sorted(app.MODULES)[-1]
        sources = [log(module, [app.event('PositionRedeemed(address,uint256,address,uint256,uint256)'),
            topic(app.AUTO_REDEEMER), '0x' + format(7, '064x'), topic(app.AUTO_REDEEMER)],
            ['uint256', 'uint256'], [100, 100], 1)]
        for index, owner, position, payout in [(2, app.WALLET, 7, 100), (3, OTHER, 8, 90), (4, app.WALLET, 9, 0)]:
            sources.append(log(app.AUTO_REDEEMER, [app.event('Redemption(address,uint256,uint256)'),
                topic(owner), '0x' + format(position, '064x')], ['uint256'], [payout], index))
        rows = app.operation_rows(sources)
        self.assertEqual([(row[3], row[6]) for row in rows], [('REDEEM', 100), ('REDEEM', 0)])
        self.assertEqual([row[7].obj['position_id'] for row in rows], ['7', '9'])

    def test_money_contracts_and_directions(self):
        for address in app.CASH:
            for sender, receiver, delta in [(app.ZERO, app.WALLET, 10), (app.WALLET, app.ZERO, -10),
                                           (app.WALLET, app.WALLET, 0), (OTHER, THIRD, None)]:
                rows = app.movement_rows(cash(sender, receiver, 10, address=address))
                self.assertEqual([row[-1] for row in rows], [] if delta is None else [delta])

    def test_single_zero_and_max_uint256(self):
        for amount in (0, 2**256 - 1):
            row = app.movement_rows(single(amount=amount))[0]
            self.assertEqual(row[-1], amount)

    def test_batch_repeated_ids_are_preserved(self):
        rows = app.movement_rows(transfer_batch([7, 7, 9], [10, 20, 0]))
        self.assertEqual([row[2] for row in rows], [0, 1, 2])
        self.assertEqual(sum(row[-1] for row in rows if row[4] == 7), 30)
        self.assertEqual(rows[-1][4], 9)

    def test_batch_lengths_must_match(self):
        with self.assertRaises(app.DataError): app.movement_rows(transfer_batch([7], [1, 2]))

    def test_bad_abi_and_topics(self):
        source = single()
        source['topics'] = source['topics'][:-1]
        with self.assertRaises(app.DataError): app.movement_rows(source)
        source = cash()
        source['data'] += '00' * 32
        with self.assertRaises(app.DataError): app.movement_rows(source)

    def test_removed_and_out_of_range(self):
        source = cash(); source['removed'] = True
        with self.assertRaises(app.DataError): app.validate_log(source, 2)
        with self.assertRaises(app.DataError): app.validate_log(cash(), 1)

    def test_multiple_operations_and_fill_versions(self):
        sources = [fill(app.WALLET, OTHER, 1), fill(app.WALLET, OTHER, 2, version=2, side=1)]
        split = log(app.CTF, [app.SPLIT, topic(app.WALLET), '0x' + '00' * 32, '0x' + '01' * 32],
                    ['address', 'uint256[]', 'uint256'], [app.USDC_E, [1, 2], 50], 3)
        sources.append(split)
        self.assertEqual([row[3] for row in app.operation_rows(sources)], ['BUY', 'SELL', 'SPLIT'])

    def test_trade_and_independent_transfer_keep_separate_records(self):
        sources = [fill(app.WALLET, OTHER, 1), cash(app.WALLET, OTHER, 40, 2),
                   single(OTHER, app.WALLET, amount=100, index=3),
                   cash(app.WALLET, THIRD, 19, 4)]
        rows = app.operation_rows(sources)
        economic = [row for row in rows if row[7].obj['record'] == 'operation']
        transfers = [row for row in rows if row[7].obj['record'] == 'movement']
        self.assertEqual([row[3] for row in economic], ['BUY'])
        self.assertEqual([(row[1], row[6]) for row in transfers], [(2, 40), (3, 100), (4, 19)])

    def test_match_summary_does_not_duplicate_taker(self):
        exchange = sorted(app.EXCHANGE_V1)[0]
        sources = [fill(OTHER, app.WALLET, 1, side=1), fill(app.WALLET, exchange, 2), matched(app.WALLET, 3)]
        rows = app.operation_rows(sources)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][3:7], ('BUY', app.CTF, 7, 100))

    def test_equal_fills_are_separate_operations(self):
        rows = app.operation_rows([fill(app.WALLET, OTHER, 1), fill(app.WALLET, OTHER, 2)])
        self.assertEqual(len(rows), 2)

    def test_direct_counterparty_after_matched_group(self):
        exchange = sorted(app.EXCHANGE_V1)[0]
        sources = [fill(OTHER, app.WALLET, 1, side=1), fill(app.WALLET, exchange, 2), matched(app.WALLET, 3),
                   fill(OTHER, app.WALLET, 4, side=1)]
        self.assertEqual(len(app.operation_rows(sources)), 2)

    def test_negative_risk_conversion_and_redemption(self):
        adapter = sorted(app.ADAPTERS)[0]
        conversion = log(adapter, [app.CONVERT, topic(app.WALLET), '0x' + '12' * 32, '0x' + format(3, '064x')],
                         ['uint256'], [100], 1)
        redemption = log(adapter, [app.NR_REDEEM, topic(app.WALLET), '0x' + '12' * 32],
                         ['uint256[]', 'uint256'], [[0, 100], 100], 2)
        self.assertEqual([row[3] for row in app.operation_rows([conversion, redemption])], ['CONVERT', 'REDEEM'])

    def test_wrap_unwrap(self):
        sources = [cash(app.WALLET, app.PUSD, 100, 0), cash(app.ZERO, app.WALLET, 100, 1, address=app.PUSD),
                   collateral_event(app.ONRAMP, app.WALLET, 100, 2),
                   cash(app.WALLET, app.PUSD, 20, 3, address=app.PUSD),
                   cash(OTHER, app.WALLET, 20, 4), cash(app.PUSD, app.ZERO, 20, 5, address=app.PUSD),
                   collateral_event(app.OFFRAMP, app.WALLET, 20, 6, wrapping=False)]
        rows = app.operation_rows(sources)
        kinds = [row[3] for row in rows if row[7].obj['record'] == 'operation']
        self.assertEqual(kinds, ['WRAP', 'UNWRAP'])

    def test_wrap_different_beneficiary_and_unrelated_mint(self):
        sources = [cash(app.WALLET, app.PUSD, 100, 0), cash(app.ZERO, THIRD, 100, 1, address=app.PUSD),
                   collateral_event(app.ONRAMP, THIRD, 100, 2),
                   cash(app.ZERO, app.WALLET, 200, 3, address=app.PUSD)]
        rows = app.operation_rows(sources)
        wraps = [row for row in rows if row[3] == 'WRAP']
        self.assertEqual(len(wraps), 1)
        self.assertEqual(wraps[0][7].obj['recipient'], THIRD)
        self.assertTrue(wraps[0][7].obj['wallet_funded'])
        self.assertEqual(len([row for row in rows if row[3] == 'MINT']), 1)

    def test_adapter_split_merge_redeem_without_wallet_action_event(self):
        adapter = sorted(app.COLLATERAL_ADAPTERS)[0]
        for kind, operation in [(app.SPLIT, 'SPLIT'), (app.MERGE, 'MERGE'), (app.REDEEM, 'REDEEM')]:
            with self.subTest(kind=operation):
                if kind == app.SPLIT:
                    sources = [cash(app.WALLET, app.PUSD, 100, 0, address=app.PUSD),
                               collateral_event(adapter, adapter, 100, 1, wrapping=False),
                               ctf_action(adapter, kind, 2), single(adapter, app.WALLET, index=3)]
                else:
                    sources = [single(app.WALLET, adapter, index=0), ctf_action(adapter, kind, 1),
                               cash(app.ZERO, app.WALLET, 100, 2, address=app.PUSD),
                               collateral_event(adapter, app.WALLET, 100, 3)]
                rows = [row for row in app.operation_rows(sources) if row[7].obj['record'] == 'operation']
                self.assertEqual([row[3] for row in rows], [operation])
                self.assertEqual(rows[0][7].obj['actor_onchain'], adapter)
                self.assertEqual(rows[0][4], app.PUSD)

    def test_internal_exchange_split_is_not_wallet_split(self):
        adapter = sorted(app.COLLATERAL_ADAPTERS)[0]
        exchange = sorted(app.EXCHANGE_V2)[0]
        sources = [ctf_action(adapter, app.SPLIT, 1), single(adapter, exchange, index=2),
                   single(exchange, app.WALLET, index=3), fill(app.WALLET, OTHER, 4, version=2)]
        rows = app.operation_rows(sources)
        self.assertEqual(len([row for row in rows if row[3] == 'BUY']), 1)
        self.assertFalse(any(row[3] == 'SPLIT' for row in rows))

    def test_adapter_convert_without_cash_payout(self):
        adapter = sorted(app.COLLATERAL_ADAPTERS)[0]
        inner = '0xd91e80cf2e7be2e162c6513ced06f1dd0da35296'
        sources = [single(app.WALLET, adapter, index=0),
                   log(inner, [app.CONVERT, topic(adapter), '0x'+'12'*32, '0x'+format(3,'064x')], ['uint256'], [100], 1),
                   single(adapter, app.WALLET, token=9, index=2)]
        self.assertEqual(len([row for row in app.operation_rows(sources) if row[3] == 'CONVERT']), 1)

    def test_matched_summary_must_agree_with_fill(self):
        exchange = sorted(app.EXCHANGE_V1)[0]
        with self.assertRaises(app.DataError):
            app.operation_rows([fill(app.WALLET, exchange, 1, making=41), matched(app.WALLET, 2)])

    def test_wrong_network_and_snapshot(self):
        rpc = Mock()
        rpc.call.return_value = '0x1'
        with self.assertRaises(app.DataError): app.checked_header(rpc, metadata()['snapshot'])
        rpc.call.side_effect = ['0x89', {'number': '0x2', 'hash': '0x' + 'cd' * 32}]
        with self.assertRaises(app.DataError): app.checked_header(rpc, metadata()['snapshot'])

    def test_unavailable_historical_state_is_not_a_zero_balance(self):
        client = Mock()
        def call(method, params):
            if method == 'eth_chainId': return '0x89'
            if method == 'eth_getBlockByNumber': return {'number': '0x2', 'hash': BLOCK_HASH}
            if method == 'eth_getCode': return '0x6000'
            raise app.RpcError('historical state is unavailable')
        client.call.side_effect = call
        with patch.object(app, 'Rpc', return_value=client):
            with self.assertRaises(app.RpcError):
                app.archive_nodes(['https://one.example', 'https://two.example'], metadata()['snapshot'])


class DatabaseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix='pw-tests-')
        cls.pg = app.LocalPostgres(Path(cls.temp.name) / 'postgres')
        cls.pg.start()

    @classmethod
    def tearDownClass(cls):
        cls.pg.close()
        cls.temp.cleanup()

    def setUp(self):
        self.name = 'test_' + uuid.uuid4().hex
        self.conn = self.pg.database(self.name)
        self.store = app.Store(self.conn, metadata())
        self.root = Path(self.temp.name) / self.name
        self.journal = app.Journal(self.root / 'journal.jsonl')
        app.commit_record(self.journal, self.store, {'kind': 'header', 'metadata': metadata()})

    def tearDown(self):
        self.journal.close()
        self.conn.close()

    def scan(self, logs):
        for stream, (addresses, topics) in app.scan_streams().items():
            flt = {'address': addresses, 'topics': topics, 'fromBlock': '0x0', 'toBlock': '0x2'}
            selected = []
            for source in logs:
                try: selected.append(app.validate_log(source, 2, flt))
                except app.DataError: pass
            app.commit_record(self.journal, self.store, {'kind': 'range', 'stream': stream, 'from': 0, 'to': 2, 'logs': selected})

    def receipts(self, logs):
        grouped = {}
        for source in logs: grouped.setdefault(source['transactionHash'], []).append(source)
        app.commit_record(self.journal, self.store, {'kind': 'receipts',
            'items': [receipt(items) for items in grouped.values()],
            'blocks': [{'number': '0x2', 'hash': BLOCK_HASH, 'timestamp': '0x3e8'}]})

    def complete(self, logs=None):
        logs = logs if logs is not None else [cash(), single()]
        self.scan(logs)
        self.receipts(logs)
        with self.conn.transaction(): self.store.prepare_balances()
        values = [[address, str(token), str(value)] for address, token, value in
                  self.conn.execute('select token_address,token_id,calculated from balances order by token_address,token_id')]
        for source in metadata()['verification_sources']:
            app.commit_record(self.journal, self.store, {'kind': 'balances', 'source': source, 'values': values})
            app.commit_record(self.journal, self.store, {'kind': 'snapshot_checked', 'source': source, 'hash': BLOCK_HASH})

    def test_full_pipeline_zero_positions_self_transfers_and_large_amount(self):
        logs = [cash(amount=2**256 - 1), single(amount=10), single(app.WALLET, OTHER, amount=10, index=2),
                single(app.WALLET, app.WALLET, token=9, amount=5, index=3), transfer_batch([11, 11], [3, 4], index=4)]
        self.complete(logs)
        self.assertEqual(app.result_summary(self.store)['status'], 'complete')
        balances = dict(self.conn.execute('select token_id,calculated from balances where token_address=%s', (app.CTF,)))
        self.assertEqual(balances, {7: 0, 9: 0, 11: 7})
        self.assertEqual(self.conn.execute('select calculated from balances where token_address=%s', (app.USDC_E,)).fetchone()[0], 2**256 - 1)

    def test_batched_scanner_shrinks_ranges_and_resumes_without_duplicates(self):
        sources = [cash(), single()]
        client = Mock()
        def batch(calls):
            result = []
            for method, params in calls:
                flt = params[0]
                if app.quantity(flt['toBlock']) - app.quantity(flt['fromBlock']) > 0:
                    raise app.RangeError('block range limit')
                found = []
                for source in sources:
                    try: found.append(app.validate_log(source, 2, flt))
                    except app.DataError: pass
                result.append(found)
            return result
        client.batch.side_effect = batch
        with contextlib.redirect_stdout(io.StringIO()): app.scan_history(client, self.store, self.journal, 16)
        self.assertEqual(self.conn.execute('select count(*) from movements').fetchone()[0], 2)
        calls = client.batch.call_count
        app.scan_history(client, self.store, self.journal, 16)
        self.assertEqual(client.batch.call_count, calls)

    def test_batched_scanner_reduces_batch_width_after_timeout(self):
        client = Mock()
        widths = []
        def batch(calls):
            widths.append(len(calls))
            if len(calls) > 1:
                raise app.RpcError('timeout')
            return [[]]
        client.batch.side_effect = batch
        with contextlib.redirect_stdout(io.StringIO()):
            app.scan_history(client, self.store, self.journal, 1, workers=4)
        self.assertIn(3, widths)
        self.assertIn(1, widths)
        self.store.require_history()

    def test_partial_import_resumes(self):
        self.scan([cash(), single()])
        second = self.pg.database('replay_' + uuid.uuid4().hex)
        try:
            store = app.Store(second, metadata())
            self.journal.file.seek(0)
            header = json.loads(self.journal.file.readline())['record']
            store.apply(header, self.journal.file.tell())
            self.assertGreater(self.journal.replay(store), 0)
            self.assertEqual(second.execute('select count(*) from movements').fetchone()[0], 2)
        finally: second.close()

    def test_restart_after_file_flush_before_database_commit(self):
        source = {'kind': 'range', 'stream': 'cash_in', 'from': 0, 'to': 2, 'logs': [cash()]}
        self.journal.append(source)
        self.assertEqual(self.store.next_block('cash_in'), 0)
        self.journal.replay(self.store)
        self.assertEqual(self.store.next_block('cash_in'), 3)
        self.assertEqual(self.conn.execute('select count(*) from movements').fetchone()[0], 1)
        self.assertEqual(self.journal.replay(self.store), 0)

    def test_database_commit_failure_replays_without_duplicates(self):
        record = {'kind': 'range', 'stream': 'cash_in', 'from': 0, 'to': 2, 'logs': [cash()]}
        original = self.store.apply
        with patch.object(self.store, 'apply', side_effect=psycopg.OperationalError('database unavailable')):
            with self.assertRaises(psycopg.OperationalError): app.commit_record(self.journal, self.store, record)
        self.journal.replay(self.store)
        self.assertEqual(self.conn.execute('select count(*) from movements').fetchone()[0], 1)
        self.assertEqual(self.store.next_block('cash_in'), 3)

    def test_incomplete_tail_is_recovered(self):
        offset = self.store.offset()
        self.journal.file.write(b'{"kind":')
        self.journal.file.flush()
        self.journal.replay(self.store)
        self.assertEqual(self.journal.path.stat().st_size, offset)

    def test_receipt_journal_records_are_compressed_and_replayable(self):
        self.scan([cash()])
        record = {'kind': 'receipts', 'items': [receipt([cash()])],
                  'blocks': [{'number': '0x2', 'hash': BLOCK_HASH, 'timestamp': '0x3e8'}]}
        line = app.journal_line(record)
        self.assertLess(len(line), len(json.dumps(record)))
        app.commit_record(self.journal, self.store, record)
        self.assertTrue(self.conn.execute('select receipt_done from transactions').fetchone()[0])
        self.assertEqual(self.journal.replay(self.store), 0)

    def test_corrupt_complete_record_stops(self):
        self.journal.file.write(b'{bad}\n'); self.journal.file.flush()
        with self.assertRaises(app.DataError): self.journal.replay(self.store)

    def test_missing_committed_file_bytes_stop(self):
        self.journal.file.truncate(0)
        with self.assertRaises(app.DataError): self.journal.replay(self.store)

    def test_wrong_checkpoint_and_journal_header(self):
        wrong = metadata(); wrong['wallet'] = OTHER
        with self.assertRaises(app.DataError): app.Store(self.conn, wrong)
        new = self.pg.database('wrong_' + uuid.uuid4().hex)
        try:
            store = app.Store(new, metadata())
            with self.assertRaises(app.DataError): store.apply({'kind': 'header', 'metadata': wrong}, 100)
        finally: new.close()

    def test_empty_ranges_and_gaps(self):
        self.scan([])
        self.store.require_history()
        self.assertEqual(self.store.next_block('cash_in'), 3)
        with self.assertRaises(app.DataError):
            app.commit_record(self.journal, self.store, {'kind': 'range', 'stream': 'cash_in', 'from': 4, 'to': 4, 'logs': []})

    def test_conflicting_duplicate_fails(self):
        self.scan([cash()])
        bad = cash(amount=99)
        with self.assertRaises(app.DataError):
            with self.conn.transaction(): self.store.save_logs([bad], True)
        self.assertEqual(self.conn.execute('select delta_raw from movements').fetchone()[0], 100)

    def test_receipt_missing_log_and_unscanned_transfer(self):
        self.scan([cash(), single()])
        with self.assertRaises(app.DataError): self.receipts([cash()])
        with self.assertRaises(app.DataError): self.receipts([cash(), single(), cash(index=10)])
        self.assertEqual(self.conn.execute('select count(*) from transactions where receipt_done').fetchone()[0], 0)
        self.receipts([cash(), single()])

    def test_matched_summary_is_saved_as_raw_evidence_without_extra_trade(self):
        exchange = sorted(app.EXCHANGE_V1)[0]
        sources = [fill(OTHER, app.WALLET, 1, side=1), fill(app.WALLET, exchange, 2), matched(app.WALLET, 3)]
        self.scan(sources)
        self.receipts(sources)
        self.assertEqual(self.conn.execute('select count(*) from raw_logs').fetchone()[0], 3)
        self.assertEqual(self.conn.execute("select count(*) from operations where operation_type='BUY'").fetchone()[0], 1)

    def test_prefetched_receipts_resume_after_a_batch_failure(self):
        sources = [dict(cash(tx=index), blockTimestamp='0x3e8') for index in range(1, 161)]
        self.scan(sources)
        by_hash = {source['transactionHash']: receipt([source]) for source in sources}
        rpc = Mock()
        def batch(calls):
            self.assertTrue(all(method == 'eth_getTransactionReceipt' for method, _ in calls))
            return [by_hash[params[0]] for _, params in calls]
        rpc.batch.side_effect = batch
        commit, batches = app.commit_record, 0
        def fail_between_batches(journal, store, record):
            nonlocal batches
            batches += 1
            if batches == 2:
                journal.append(record)
                raise OSError('interrupted after journal flush before commit')
            return commit(journal, store, record)
        with patch.object(app, 'commit_record', side_effect=fail_between_batches):
            with self.assertRaises(OSError): app.process_receipts(rpc, self.store, self.journal, 1)
        self.assertEqual(self.conn.execute('select count(*) from transactions where receipt_done').fetchone()[0], 50)
        self.journal.replay(self.store)
        self.assertEqual(self.conn.execute('select count(*) from transactions where receipt_done').fetchone()[0], 100)
        app.process_receipts(rpc, self.store, self.journal, 1)
        self.store.require_history()
        self.assertEqual(self.conn.execute('select count(*) from transactions where receipt_done').fetchone()[0], 160)
        self.assertEqual(self.conn.execute('select count(*),sum(delta_raw) from movements').fetchone(), (160, 16000))
        self.assertEqual(self.conn.execute('select count(*) from operations').fetchone()[0], 160)

    def test_receipt_timestamp_must_match_cached_header(self):
        source = dict(cash(), blockTimestamp='0x3e9')
        self.scan([source])
        with self.assertRaises(app.DataError): self.receipts([source])
        self.assertFalse(self.conn.execute('select receipt_done from transactions').fetchone()[0])

    def test_failed_or_wrong_receipt_never_marks_done(self):
        self.scan([cash()])
        for field, value in [('status', '0x0'), ('blockHash', '0x'+'cd'*32), ('from', None)]:
            bad = receipt([cash()]); bad[field] = value
            with self.assertRaises((app.DataError, TypeError)):
                app.commit_record(self.journal, self.store, {'kind': 'receipts', 'items': [bad],
                    'blocks': [{'number': '0x2', 'hash': BLOCK_HASH, 'timestamp': '0x3e8'}]})
        self.assertFalse(self.conn.execute('select receipt_done from transactions').fetchone()[0])

    def test_mismatch_and_incomplete_results(self):
        self.complete()
        self.conn.execute('update verification set onchain=onchain+1 where source=%s', ('one.example',))
        self.assertEqual(app.result_summary(self.store)['status'], 'mismatch')
        self.conn.execute('delete from verification where source=%s', ('two.example',))
        with self.assertRaises(app.DataError): app.result_summary(self.store)

    def test_negative_balance_is_not_accepted(self):
        self.scan([cash(app.WALLET, OTHER)])
        self.receipts([cash(app.WALLET, OTHER)])
        with self.assertRaises(app.DataError):
            with self.conn.transaction(): self.store.prepare_balances()

    def test_zero_redemption_discovers_position_without_transfers(self):
        source = log(app.AUTO_REDEEMER, [app.event('Redemption(address,uint256,uint256)'),
            topic(app.WALLET), '0x' + format(9, '064x')], ['uint256'], [0])
        self.complete([source])
        row = self.conn.execute('select calculated from balances where token_address=%s and token_id=9',
                                (app.POSITION_MANAGER,)).fetchone()
        self.assertEqual(row, (0,))
        self.assertEqual(self.conn.execute('select count(*) from movements').fetchone()[0], 0)
        self.assertEqual(app.result_summary(self.store)['mismatches'], 0)

    def test_journal_rebuild_matches_all_tables(self):
        self.complete()
        second = self.pg.database('rebuild_' + uuid.uuid4().hex)
        try:
            store = app.Store(second, metadata()); self.journal.replay(store)
            for table in ('transactions','raw_logs','movements','operations','balances','verification','scan_progress','source_checks'):
                first = self.conn.execute(f'select * from {table} order by 1,2').fetchall()
                replay = second.execute(f'select * from {table} order by 1,2').fetchall()
                self.assertEqual(first, replay, table)
            self.assertEqual(app.result_summary(store), app.result_summary(self.store))
        finally: second.close()

    def test_report_contains_zero_balances_and_exact_numbers(self):
        self.complete([cash(), single(amount=0)])
        output = self.root / 'result'
        summary = app.write_results(self.store, output)
        report = json.loads((output / 'result.json').read_text())
        self.assertEqual(summary['status'], 'complete')
        self.assertEqual(report['assets'][3]['balances'][0]['calculated'], '0')
        self.assertEqual(report['assets'][0]['balances'][0]['onchain'], ['100', '100'])

    def test_report_validator_detects_changed_and_missing_balances(self):
        from validate_run import verify_report
        self.complete([cash(), single(amount=0)])
        output = self.root / 'result'
        app.write_results(self.store, output)
        path = output / 'result.json'
        report = json.loads(path.read_text())
        self.assertEqual(verify_report(self.conn, path, metadata()), 4)
        report['assets'][3]['balances'][0]['calculated'] = '1'
        app.atomic_json(path, report)
        with self.assertRaises(app.DataError): verify_report(self.conn, path, metadata())
        report['assets'][3]['balances'] = []
        app.atomic_json(path, report)
        with self.assertRaises(app.DataError): verify_report(self.conn, path, metadata())

    def test_database_comparison_detects_changed_amount_and_keeps_session_settings(self):
        from validate_run import table_state
        self.complete([cash(), single(amount=0)])
        settings = [self.conn.execute('show ' + name).fetchone()[0]
                    for name in ('enable_indexscan', 'work_mem')]
        before = table_state(self.conn, 'balances')
        self.conn.execute('update balances set calculated=calculated+1 where token_address=%s', (app.USDC_E,))
        after = table_state(self.conn, 'balances')
        self.assertEqual(before[0], after[0])
        self.assertNotEqual(before, after)
        self.assertEqual(settings, [self.conn.execute('show ' + name).fetchone()[0]
                                   for name in ('enable_indexscan', 'work_mem')])

    def test_report_write_failure_does_not_publish_success(self):
        self.complete()
        output = self.root / 'result'
        app.atomic_json(output / 'status.json', {'status': 'running'})
        with patch.object(app.os, 'replace', side_effect=OSError('disk full')):
            with self.assertRaises(OSError): app.write_results(self.store, output)
        self.assertEqual(json.loads((output / 'status.json').read_text())['status'], 'running')

    def test_existing_server_is_not_stopped_by_client(self):
        another = app.LocalPostgres(self.pg.root)
        another.start()
        self.assertFalse(another.started)
        another.close()
        with self.pg.connect('postgres') as conn: self.assertEqual(conn.execute('select 1').fetchone()[0], 1)

    def test_cluster_without_ownership_marker_is_rejected(self):
        marker = self.pg.data / 'pw-owner.json'
        content = marker.read_bytes()
        marker.unlink()
        try:
            another = app.LocalPostgres(self.pg.root)
            with self.assertRaisesRegex(app.DataError, 'not created'):
                another.start()
            another.close()
            with self.pg.connect('postgres') as conn:
                self.assertEqual(conn.execute('select 1').fetchone()[0], 1)
        finally:
            marker.write_bytes(content)

    def test_replacement_postgres_process_is_not_stopped(self):
        with tempfile.TemporaryDirectory(prefix='pw-restart-') as directory:
            server = app.LocalPostgres(Path(directory))
            server.start()
            ctl = server.binary('pg_ctl')
            try:
                subprocess.run([ctl, '-D', str(server.data), '-m', 'fast', '-w', 'stop'],
                               check=True, capture_output=True)
                options = f'-h 127.0.0.1 -p {server.port}'
                if app.os.name != 'nt':
                    options += " -k ''"
                subprocess.run([ctl, '-D', str(server.data), '-l', str(server.root / 'postgres.log'),
                                '-o', options, '-w', 'start'], check=True, capture_output=True)
                self.assertNotEqual(server.server_identity(), server.started_identity)
                server.close()
                with server.connect('postgres') as conn:
                    self.assertEqual(conn.execute('select 1').fetchone()[0], 1)
            finally:
                subprocess.run([ctl, '-D', str(server.data), '-m', 'fast', '-w', 'stop'], capture_output=True)

    def test_work_directory_lock(self):
        lock = app.WorkLock(self.root / 'lock_test')
        try:
            with self.assertRaises(app.DataError): app.WorkLock(self.root / 'lock_test')
        finally: lock.close()

    def test_committed_journal_corruption_is_detected(self):
        self.scan([cash()])
        with self.journal.path.open('r+b') as file:
            file.seek(25)
            old = file.read(1)
            file.seek(25)
            file.write(b'X' if old != b'X' else b'Y')
        with self.assertRaises(app.DataError): self.journal.replay(self.store)

    def test_uncommitted_valid_json_corruption_is_detected(self):
        record = {'kind': 'range', 'stream': 'cash_in', 'from': 0, 'to': 2, 'logs': []}
        envelope = json.loads(app.journal_line(record))
        envelope['crc32'] ^= 1
        line = json.dumps(envelope).encode() + b'\n'
        self.journal.file.write(line); self.journal.file.flush()
        with self.assertRaises(app.DataError): self.journal.replay(self.store)

    def test_sql_transaction_failure_rolls_back_progress_and_rows(self):
        record = {'kind': 'range', 'stream': 'cash_in', 'from': 0, 'to': 2, 'logs': [cash()]}
        save = self.store.save_logs
        def fail(*args):
            save(*args)
            raise psycopg.OperationalError('connection lost before commit')
        with patch.object(self.store, 'save_logs', side_effect=fail):
            with self.assertRaises(psycopg.OperationalError): app.commit_record(self.journal, self.store, record)
        self.assertEqual(self.conn.execute('select count(*) from movements').fetchone()[0], 0)
        self.assertEqual(self.store.next_block('cash_in'), 0)
        self.journal.replay(self.store)
        self.assertEqual(self.conn.execute('select count(*) from movements').fetchone()[0], 1)

    def test_real_connection_loss_after_journal_flush(self):
        record = {'kind': 'range', 'stream': 'cash_in', 'from': 0, 'to': 2, 'logs': [cash()]}
        append = self.journal.append
        def terminate(record):
            offset = append(record)
            with self.pg.connect('postgres', autocommit=True) as admin:
                admin.execute('select pg_terminate_backend(%s)', (self.conn.info.backend_pid,))
            return offset
        with patch.object(self.journal, 'append', side_effect=terminate):
            with self.assertRaises(psycopg.OperationalError): app.commit_record(self.journal, self.store, record)
        self.conn.close()
        self.conn = self.pg.database(self.name)
        self.store = app.Store(self.conn, metadata())
        self.journal.replay(self.store)
        self.assertEqual(self.conn.execute('select count(*) from movements').fetchone()[0], 1)

    def test_disk_failure_keeps_database_checkpoint(self):
        before = self.store.offset()
        record = {'kind': 'range', 'stream': 'cash_in', 'from': 0, 'to': 2, 'logs': [cash()]}
        with patch.object(app.os, 'fsync', side_effect=OSError('disk full')):
            with self.assertRaises(OSError): app.commit_record(self.journal, self.store, record)
        self.assertEqual(self.store.offset(), before)
        self.journal.replay(self.store)
        self.assertEqual(self.conn.execute('select count(*) from movements').fetchone()[0], 1)

    def cli_fixture(self):
        work = self.root / 'cli'
        run = work / 'runs' / 'test'
        run.mkdir(parents=True)
        app.atomic_json(work / 'current.json', metadata())
        shutil.copyfile(self.journal.path, run / 'journal.jsonl')
        return ['--work-dir', str(work), '--output-dir', str(work / 'output'), '--replay-only']

    def test_cli_exit_codes_success_and_mismatch(self):
        self.complete()
        for mismatch in (False, True):
            with self.subTest(mismatch=mismatch):
                if mismatch:
                    app.commit_record(self.journal, self.store, {'kind': 'balances', 'source': 'one.example',
                        'values': [[app.USDC_E, '-1', '99']]})
                args = self.cli_fixture() if not mismatch else self.cli_fixture_for_repeat()
                with contextlib.redirect_stdout(io.StringIO()): code = app.main(args)
                self.assertEqual(code, 2 if mismatch else 0)

    def cli_fixture_for_repeat(self):
        work = self.root / 'cli'
        shutil.copyfile(self.journal.path, work / 'runs' / 'test' / 'journal.jsonl')
        return ['--work-dir', str(work), '--output-dir', str(work / 'output'), '--replay-only']

    def test_cli_interrupt_returns_130(self):
        self.complete()
        args = self.cli_fixture()
        with patch.object(app.Journal, 'replay', side_effect=KeyboardInterrupt):
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(app.main(args), 130)


if __name__ == '__main__':
    unittest.main()
