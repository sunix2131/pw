import argparse
import base64
import contextlib
import getpass
import itertools
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
import uuid
import zlib
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlsplit

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb
import requests
from eth_abi import decode, encode
from eth_abi.exceptions import DecodingError, EncodingError
from eth_utils import keccak

BASE = Path(__file__).resolve().parent
WALLET = '0x46b353667fd7d846af3bbeda6584b0e5b883d3de'
ZERO = '0x' + '0' * 40
CTF = '0x4d97dcd97ec945f40cf65f87097ace5ea0476045'
POSITION_MANAGER = '0x006f54f7f9a22e0000cc2ab60031000000ae9fef'
USDC_E = '0x2791bca1f2de4661ed88a30c99a7a9449aa84174'
USDC = '0x3c499c542cef5e3811e1192ce70d8cc03d5c3359'
PUSD = '0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb'
CASH = {USDC_E: 'USDC.e', USDC: 'USDC', PUSD: 'pUSD'}
POSITIONS = (CTF, POSITION_MANAGER)
EXCHANGE_V1 = {'0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e',
               '0xc5d563a36ae78145c45a50134d48a1215220f80a'}
EXCHANGE_V2 = {'0xe111180000d2663c0091e4f400237545b87b996b',
               '0xe2222d279d744050d28e00520010520000310f59'}
COMBO_EXCHANGE = '0xe3333700ca9d93003f00f0f71f8515005f6c00aa'
ADAPTERS = {
    '0xd91e80cf2e7be2e162c6513ced06f1dd0da35296',
    '0xada100874d00e3331d00f2007a9c336a65009718',
    '0xada200001000ef00d07553cee7006808f895c6f1',
    '0xada100db00ca00073811820692005400218fce1f',
    '0xada2005600dec949baf300f4c6120000bdb6eaab',
}
ONRAMP = '0x93070a847efef7f70739046a929d47a521f5b8ee'
OFFRAMP = '0x2957922eb93258b93368531d39facca3b4dc5854'
PERMISSIONED_RAMP = '0xebc2459ec962869ca4c0bd1e06368272732bcb08'
AUTO_REDEEMER = '0xa1200000d0002264c9a1698e001292d00e1b00af'
MODULES = {'0x1000008dd9001b968442c1000017eae6e0da00ba',
           '0x200000900045e3b6259600682756002200028933',
           '0x30000034706c7d8e12009dab006be20000c031a8'}
PROTOCOL = ADAPTERS | EXCHANGE_V1 | EXCHANGE_V2 | {
    CTF, POSITION_MANAGER, COMBO_EXCHANGE, ONRAMP, OFFRAMP, PERMISSIONED_RAMP,
    '0x1000008dd9001b968442c1000017eae6e0da00ba',
    '0x200000900045e3b6259600682756002200028933',
    '0x30000034706c7d8e12009dab006be20000c031a8',
    '0xa1200000d0002264c9a1698e001292d00e1b00af',
}
RPC_URLS = ['https://tenderly.rpc.polygon.community', 'https://polygon.drpc.org']
FORMAT_VERSION = 3


def event(signature):
    return '0x' + keccak(text=signature).hex()


TRANSFER = event('Transfer(address,address,uint256)')
SINGLE = event('TransferSingle(address,address,address,uint256,uint256)')
BATCH = event('TransferBatch(address,address,address,uint256[],uint256[])')
SPLIT = event('PositionSplit(address,address,bytes32,bytes32,uint256[],uint256)')
MERGE = event('PositionsMerge(address,address,bytes32,bytes32,uint256[],uint256)')
REDEEM = event('PayoutRedemption(address,address,bytes32,bytes32,uint256[],uint256)')
NR_SPLIT = event('PositionSplit(address,bytes32,uint256)')
NR_MERGE = event('PositionsMerge(address,bytes32,uint256)')
NR_REDEEM = event('PayoutRedemption(address,bytes32,uint256[],uint256)')
CONVERT = event('PositionsConverted(address,bytes32,uint256,uint256)')
FILL_V1 = event('OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)')
FILL_V2 = event('OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,bytes32)')
MATCH_V1 = event('OrdersMatched(bytes32,address,uint256,uint256,uint256,uint256)')
MATCH_V2 = event('OrdersMatched(bytes32,address,uint8,uint256,uint256,uint256)')
ACTION_EVENTS = [SPLIT, MERGE, REDEEM, NR_SPLIT, NR_MERGE, NR_REDEEM, CONVERT]
WRAPPED = event('Wrapped(address,address,address,uint256)')
UNWRAPPED = event('Unwrapped(address,address,address,uint256)')
COLLATERAL_ADAPTERS = ADAPTERS - {'0xd91e80cf2e7be2e162c6513ced06f1dd0da35296'}
MODULE_EVENTS = {}
for name, kind, indexed, plain in [
    ('PositionsSplit', 'SPLIT', 'initiator:address condition_id:bytes31 recipient0:address', 'recipient1:address amount:uint256'),
    ('PositionsMerged', 'MERGE', 'initiator:address condition_id:bytes31 recipient:address', 'amount:uint256'),
    ('PositionRedeemed', 'REDEEM', 'initiator:address position_id:uint256 recipient:address', 'amount:uint256 payout:uint256'),
    ('HorizontalSplit', 'SPLIT', 'initiator:address event_id:bytes29 recipient:address', 'amount:uint256'),
    ('HorizontalMerge', 'MERGE', 'initiator:address event_id:bytes29 recipient:address', 'amount:uint256'),
    ('PositionConverted', 'CONVERT', 'initiator:address event_id:bytes29 recipient:address', 'condition_index:uint256 amount:uint256'),
    ('PositionMigrated', 'MIGRATE', 'from:address condition_id:bytes31 position_id:uint256', 'outcome_index:uint256 amount:uint256'),
    ('Compressed', 'CONVERT', 'user:address old_position_id:uint256 new_position_id:uint256', 'amount:uint256 position_amount:uint256 collateral_out:uint256'),
    ('ConvertedToYesBasket', 'CONVERT', 'user:address condition_id:bytes31', 'amount:uint256'),
    ('MergedFromYesBasket', 'MERGE', 'user:address condition_id:bytes31', 'amount:uint256'),
    ('Extracted', 'SPLIT', 'user:address condition_id:bytes31', 'reduced_condition_id:bytes31 residual_condition_id:bytes31 amount:uint256'),
    ('Injected', 'MERGE', 'user:address condition_id:bytes31', 'reduced_condition_id:bytes31 residual_condition_id:bytes31 amount:uint256'),
    ('SplitOnCondition', 'SPLIT', 'user:address parent_condition_id:bytes31', 'child_yes_condition_id:bytes31 child_no_condition_id:bytes31 amount:uint256'),
    ('MergedOnCondition', 'MERGE', 'user:address parent_condition_id:bytes31', 'child_yes_condition_id:bytes31 child_no_condition_id:bytes31 amount:uint256'),
    ('Wrapped', 'WRAP', 'user:address', 'underlying_position_id:uint256 combinatorial_position_id:uint256 amount:uint256'),
    ('Unwrapped', 'UNWRAP', 'user:address', 'combinatorial_position_id:uint256 underlying_position_id:uint256 amount:uint256'),
    ('Compressed', 'CONVERT', 'user:address old_position_id:uint256 new_position_id:uint256', 'recipient:address amount:uint256 position_amount:uint256 collateral_out:uint256'),
    ('ConvertedOnEvent', 'CONVERT', 'user:address parent_condition_id:bytes31 event_id:bytes29', 'condition_index:uint256 recipients:address[] amount:uint256'),
    ('ConvertedToYesBasket', 'CONVERT', 'user:address condition_id:bytes31', 'recipients:address[] amount:uint256'),
    ('Extracted', 'SPLIT', 'user:address condition_id:bytes31', 'reduced_condition_id:bytes31 residual_condition_id:bytes31 recipient0:address recipient1:address amount:uint256'),
    ('Injected', 'MERGE', 'user:address condition_id:bytes31', 'reduced_condition_id:bytes31 residual_condition_id:bytes31 recipient:address amount:uint256'),
    ('MergedFromYesBasket', 'MERGE', 'user:address condition_id:bytes31', 'recipient:address amount:uint256'),
    ('MergedOnCondition', 'MERGE', 'user:address parent_condition_id:bytes31', 'child_yes_condition_id:bytes31 child_no_condition_id:bytes31 recipient:address amount:uint256'),
    ('MergedOnEvent', 'MERGE', 'user:address parent_condition_id:bytes31 event_id:bytes29', 'recipient:address amount:uint256'),
    ('SplitOnCondition', 'SPLIT', 'user:address parent_condition_id:bytes31', 'child_yes_condition_id:bytes31 child_no_condition_id:bytes31 recipient0:address recipient1:address amount:uint256'),
    ('SplitOnEvent', 'SPLIT', 'user:address parent_condition_id:bytes31 event_id:bytes29', 'recipients:address[] amount:uint256'),
    ('Wrapped', 'WRAP', 'user:address', 'underlying_position_id:uint256 combinatorial_position_id:uint256 recipient:address amount:uint256'),
    ('Unwrapped', 'UNWRAP', 'user:address', 'combinatorial_position_id:uint256 underlying_position_id:uint256 recipient:address amount:uint256'),
    ('Redemption', 'REDEEM', 'from:address position_id:uint256', 'payout:uint256'),
    ('BinaryRedemption', 'REDEEM', 'from:address condition_id:bytes32', 'payout:uint256'),
    ('NegRiskRedemption', 'REDEEM', 'from:address condition_id:bytes32', 'payout:uint256'),
]:
    indexed_fields = [tuple(field.split(':')) for field in indexed.split()]
    data_fields = [tuple(field.split(':')) for field in plain.split()]
    signature = event(name + '(' + ','.join(typ for _, typ in indexed_fields + data_fields) + ')')
    MODULE_EVENTS[signature] = (name, kind, indexed_fields, data_fields)


class DataError(RuntimeError):
    pass


class RpcError(RuntimeError):
    pass


class RangeError(RpcError):
    pass


class ResponseLimit(RangeError):
    pass


def hex_value(value, size=None):
    if not isinstance(value, str) or not re.fullmatch(r'0x(?:[0-9a-fA-F]{2})*', value):
        raise DataError('Invalid hex data')
    if size is not None and len(value) != 2 + size * 2:
        raise DataError(f'Expected {size} bytes')
    return value.lower()


def quantity(value):
    if not isinstance(value, str) or not re.fullmatch(r'0x[0-9a-fA-F]+', value):
        raise DataError('Invalid RPC quantity')
    return int(value, 16)


def topic_address(value):
    value = hex_value(value, 32)
    if value[2:26] != '0' * 24:
        raise DataError('Invalid address topic')
    return '0x' + value[-40:]


def abi(types, data):
    raw = bytes.fromhex(hex_value(data)[2:])
    return decode_abi(tuple(types), raw)


@lru_cache(maxsize=2048)
def decode_abi(types, raw):
    try:
        values = decode(types, raw)
        if encode(types, values) != raw:
            raise DataError('Noncanonical ABI data')
    except (DecodingError, EncodingError) as exc:
        raise DataError('Invalid ABI data') from exc
    return values


def rpc_source(url):
    return urlsplit(url).hostname or 'rpc'


class Rpc:
    def __init__(self, urls, attempts=6, timeout=40, sleep=time.sleep):
        self.urls = list(dict.fromkeys(urls))
        if not self.urls:
            raise ValueError('At least one RPC URL is required')
        self.attempts, self.timeout, self.sleep = attempts, timeout, sleep
        self.local = threading.local()
        self.ids = itertools.count(1)
        self.lock = threading.Lock()
        self.preferred = 0
        self.no_batch = set()
        self.batch_limits = {}

    def session(self):
        if not hasattr(self.local, 'session'):
            self.local.session = requests.Session()
        return self.local.session

    def call(self, method, params):
        return self.batch([(method, params)])[0]

    def read_response(self, response, started):
        chunks, size = [], 0
        for chunk in response.iter_content(chunk_size=65536):
            if time.monotonic() - started > self.timeout:
                raise requests.Timeout('RPC response exceeded its time budget')
            size += len(chunk)
            if size > 64 * 1024 * 1024:
                raise ResponseLimit('RPC response exceeds size limit')
            chunks.append(chunk)
        return json.loads(b''.join(chunks))

    def batch(self, calls):
        if not calls:
            return []
        results = [None] * len(calls)
        pending = list(range(len(calls)))
        errors = []
        start_index = self.preferred
        for attempt in range(self.attempts):
            url_index = (start_index + attempt) % len(self.urls)
            url = self.urls[url_index]
            limit = self.batch_limits.get(url, len(pending))
            if len(pending) > limit:
                for start in range(0, len(pending), limit):
                    part = pending[start:start + limit]
                    values = self.batch([calls[index] for index in part])
                    for index, value in zip(part, values, strict=True):
                        results[index] = value
                return results
            if url in self.no_batch and len(pending) > 1:
                for index in pending:
                    results[index] = self.call(*calls[index])
                return results
            with self.lock:
                ids = {next(self.ids): index for index in pending}
            body = [{'jsonrpc': '2.0', 'id': ident, 'method': calls[index][0],
                     'params': calls[index][1]} for ident, index in ids.items()]
            response = None
            try:
                started = time.monotonic()
                response = self.session().post(url, json=body if len(body) > 1 else body[0],
                                               timeout=self.timeout, stream=True)
                if response.status_code == 413:
                    raise ResponseLimit('RPC response exceeds size limit')
                payload = self.read_response(response, started)
                errors_in_response = payload if isinstance(payload, list) else [payload]
                limited = False
                for item in errors_in_response:
                    if isinstance(item, dict) and isinstance(item.get('error'), dict):
                        match = re.search(r'batch of more than ([1-9]\d*) requests', str(item['error']), re.I)
                        if match:
                            self.batch_limits[url] = int(match[1])
                            limited = len(pending) > int(match[1])
                has_results = any(isinstance(item, dict) and item.get('result') is not None
                                  for item in errors_in_response)
                if limited and not has_results:
                    raise ResponseLimit('RPC batch exceeds the node limit')
                if response.status_code >= 400 and not has_results:
                    for item in errors_in_response:
                        if isinstance(item, dict) and isinstance(item.get('error'), dict):
                            message = str(item['error'])
                            if re.search(r'block range|ranges? over \d+ blocks|too many (?:results|logs)|response size|range.*limit', message, re.I):
                                raise RangeError(message)
                    response.raise_for_status()
                if len(body) > 1 and isinstance(payload, dict):
                    error = payload.get('error', {})
                    if isinstance(error, dict) and error.get('code') in (-32600, -32601):
                        self.no_batch.add(url)
                    raise RpcError('Invalid batch envelope')
                items = payload if isinstance(payload, list) else [payload]
                counts = Counter(item.get('id') for item in items if isinstance(item, dict))
                if any(not isinstance(item, dict) or item.get('jsonrpc') != '2.0'
                       or type(item.get('id')) is not int or item['id'] not in ids for item in items):
                    raise RpcError('Invalid response id or envelope')
                for item in items:
                    ident = item['id']
                    if counts[ident] != 1:
                        errors.append(f'{rpc_source(url)}: Duplicate response id')
                        continue
                    if 'error' in item:
                        message = str(item['error'])
                        if re.search(r'block range|too many (?:results|logs)|response size|range.*limit', message, re.I):
                            raise RangeError(message)
                        errors.append(f'{rpc_source(url)}: {message[:240]}')
                        continue
                    if 'result' not in item or item['result'] is None:
                        errors.append(f'{rpc_source(url)}: Missing result')
                        continue
                    results[ids[ident]] = item['result']
                pending = [index for index in pending if results[index] is None]
                if not pending:
                    self.preferred = url_index
                    return results
                if limited:
                    raise ResponseLimit('RPC batch exceeds the node limit')
                response.raise_for_status()
            except ResponseLimit:
                if len(pending) == 1:
                    raise
                width = min(self.batch_limits.get(url, len(pending)), max(1, len(pending) // 2))
                for start in range(0, len(pending), width):
                    part = pending[start:start + width]
                    values = self.batch([calls[i] for i in part])
                    for index, value in zip(part, values, strict=True):
                        results[index] = value
                return results
            except RangeError:
                raise
            except (requests.RequestException, ValueError, TypeError, RpcError) as exc:
                errors.append(f'{rpc_source(url)}: {type(exc).__name__}')
            finally:
                if response is not None:
                    response.close()
            if attempt + 1 < self.attempts:
                self.sleep(min(0.25 * 2 ** attempt, 3))
        raise RpcError(f'{len(pending)} unresolved RPC requests: ' + '; '.join(errors[-3:]))


def sync_directory(path):
    if os.name != 'nt':
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def atomic_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + '.tmp')
    with temp.open('w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, separators=(',', ':'))
        file.write('\n')
        file.flush()
        os.fsync(file.fileno())
    os.replace(temp, path)
    sync_directory(path.parent)


class WorkLock:
    def __init__(self, root):
        root.mkdir(parents=True, exist_ok=True)
        self.file = (root / 'lock').open('a+b')
        try:
            if os.name == 'nt':
                import msvcrt
                self.file.seek(0)
                self.file.write(b'0')
                self.file.flush()
                self.file.seek(0)
                msvcrt.locking(self.file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.file.close()
            raise DataError('Another process is using this work directory') from None

    def close(self):
        self.file.close()


class LocalPostgres:
    def __init__(self, root, binary_dir=None):
        self.root = root.resolve()
        self.data = self.root / 'data'
        self.started = False
        self.started_identity = None
        self.binary_dir = Path(binary_dir) if binary_dir else None
        self.user = getpass.getuser()
        self.port = None

    def binary(self, name):
        if self.binary_dir:
            path = self.binary_dir / (name + ('.exe' if os.name == 'nt' else ''))
            if path.is_file():
                return str(path)
        path = shutil.which(name)
        if not path:
            raise DataError(f'{name} was not found; put PostgreSQL bin in PATH or use --pg-bin')
        return path

    def start(self):
        self.root.mkdir(parents=True, exist_ok=True)
        ctl = self.binary('pg_ctl')
        owner = {'application': 'pw', 'version': 1}
        if not (self.data / 'PG_VERSION').exists():
            if self.data.exists():
                raise DataError('PostgreSQL data directory exists without PG_VERSION')
            staging = self.root / ('initializing_' + uuid.uuid4().hex[:8])
            result = subprocess.run([self.binary('initdb'), '-D', str(staging), '-U', self.user,
                                     '-A', 'trust', '--encoding=UTF8', '--no-locale'],
                                    capture_output=True, text=True)
            if result.returncode:
                raise DataError('initdb failed: ' + result.stderr[-1500:])
            atomic_json(staging / 'pw-owner.json', owner)
            os.replace(staging, self.data)
            sync_directory(self.root)
        marker = self.data / 'pw-owner.json'
        if not marker.exists() or json.loads(marker.read_text()) != owner:
            raise DataError('PostgreSQL cluster was not created by this application')
        status = subprocess.run([ctl, '-D', str(self.data), 'status'], capture_output=True)
        if status.returncode == 0:
            self.port = int((self.data / 'postmaster.pid').read_text().splitlines()[3])
        else:
            with socket.socket() as sock:
                sock.bind(('127.0.0.1', 0))
                self.port = sock.getsockname()[1]
            options = f'-h 127.0.0.1 -p {self.port}'
            if os.name != 'nt':
                options += " -k ''"
            result = subprocess.run([ctl, '-D', str(self.data), '-l', str(self.root / 'postgres.log'),
                                     '-o', options, '-w', '-t', '30', 'start'], capture_output=True, text=True)
            if result.returncode:
                log = (self.root / 'postgres.log').read_text(errors='replace')[-1500:]
                raise DataError('PostgreSQL did not start: ' + log)
            self.started = True
            self.started_identity = self.server_identity()
        with self.connect('postgres', autocommit=True) as conn:
            actual = conn.execute('show data_directory').fetchone()[0]
            if Path(actual).resolve() != self.data.resolve():
                raise DataError('Port belongs to a different PostgreSQL cluster')

    def connect(self, database, **kwargs):
        return psycopg.connect(host='127.0.0.1', port=self.port, user=self.user,
                              dbname=database, connect_timeout=5, **kwargs)

    def database(self, name):
        if not re.fullmatch(r'[a-z][a-z0-9_]{0,62}', name):
            raise DataError('Invalid database name')
        with self.connect('postgres', autocommit=True) as conn:
            if not conn.execute('select 1 from pg_database where datname=%s', (name,)).fetchone():
                conn.execute(sql.SQL('create database {}').format(sql.Identifier(name)))
        return self.connect(name, autocommit=True)

    def server_identity(self):
        try:
            return tuple((self.data / 'postmaster.pid').read_text().splitlines()[:4])
        except OSError:
            return None

    def close(self):
        if self.started and self.started_identity is not None and self.server_identity() == self.started_identity:
            subprocess.run([self.binary('pg_ctl'), '-D', str(self.data), '-m', 'fast',
                            '-w', '-t', '30', 'stop'], capture_output=True)
        self.started = False


def validate_log(log, snapshot, flt=None):
    if not isinstance(log, dict) or log.get('removed', False) is not False:
        raise DataError('Invalid or removed log')
    log = dict(log)
    for key, size in [('address', 20), ('transactionHash', 32), ('blockHash', 32)]:
        log[key] = hex_value(log[key], size)
    for key in ('blockNumber', 'transactionIndex', 'logIndex'):
        number = quantity(log[key])
        log[key] = hex(number)
    if quantity(log['blockNumber']) > snapshot:
        raise DataError('Log is newer than the snapshot')
    if not isinstance(log.get('topics'), list):
        raise DataError('Missing log topics')
    log['topics'] = [hex_value(topic, 32) for topic in log['topics']]
    log['data'] = hex_value(log['data'])
    if flt:
        if not quantity(flt['fromBlock']) <= quantity(log['blockNumber']) <= quantity(flt['toBlock']):
            raise DataError('Log is outside the requested range')
        addresses = flt['address'] if isinstance(flt['address'], list) else [flt['address']]
        if log['address'] not in addresses:
            raise DataError('Unexpected log contract')
        for index, expected in enumerate(flt['topics']):
            if expected is None:
                continue
            expected = expected if isinstance(expected, list) else [expected]
            if index >= len(log['topics']) or log['topics'][index] not in expected:
                raise DataError('Log does not match the requested topics')
    return log


def movement_rows(log, wallet=WALLET):
    address, topics = log['address'], log['topics']
    if not topics:
        return []
    if address in CASH and topics[0] == TRANSFER:
        if len(topics) != 3:
            raise DataError('ERC20 Transfer must have three topics')
        sender, receiver = map(topic_address, topics[1:3])
        ids, amounts = [-1], abi(['uint256'], log['data'])
    elif address in POSITIONS and topics[0] in (SINGLE, BATCH):
        if len(topics) != 4:
            raise DataError('ERC1155 transfer must have four topics')
        topic_address(topics[1])
        sender, receiver = map(topic_address, topics[2:4])
        if topics[0] == SINGLE:
            token, amount = abi(['uint256', 'uint256'], log['data'])
            ids, amounts = [token], [amount]
        else:
            ids, amounts = abi(['uint256[]', 'uint256[]'], log['data'])
            if len(ids) != len(amounts):
                raise DataError('TransferBatch arrays have different lengths')
    else:
        return []
    if wallet not in (sender, receiver):
        return []
    sign = int(receiver == wallet) - int(sender == wallet)
    return [(log['transactionHash'], quantity(log['logIndex']), index, address, token,
             sender, receiver, amount, sign * amount)
            for index, (token, amount) in enumerate(zip(ids, amounts, strict=True))]


def decode_action(log, wallet=WALLET):
    topics = log['topics']
    if log['address'] not in PROTOCOL or not topics or topics[0] not in ACTION_EVENTS:
        return None
    if len(topics) < 2:
        raise DataError('Missing operation actor')
    if topic_address(topics[1]) != wallet:
        return None
    signature = topics[0]
    details = {'record': 'operation', 'contract': log['address'], 'actor': wallet}
    token_address = None
    if signature in (SPLIT, MERGE):
        if len(topics) != 4:
            raise DataError('Invalid CTF operation topics')
        collateral, partition, amount = abi(['address', 'uint256[]', 'uint256'], log['data'])
        details.update(collateral=collateral, parent_collection_id=topics[2],
                       condition_id=topics[3], partition=[str(x) for x in partition])
        kind = 'SPLIT' if signature == SPLIT else 'MERGE'
        token_address = collateral
    elif signature == REDEEM:
        if len(topics) != 4:
            raise DataError('Invalid CTF redemption topics')
        condition, sets, amount = abi(['bytes32', 'uint256[]', 'uint256'], log['data'])
        token_address = topic_address(topics[2])
        details.update(collateral=token_address, parent_collection_id=topics[3],
                       condition_id='0x' + condition.hex(), index_sets=[str(x) for x in sets])
        kind = 'REDEEM'
    elif signature in (NR_SPLIT, NR_MERGE):
        if len(topics) != 3:
            raise DataError('Invalid adapter operation topics')
        amount, = abi(['uint256'], log['data'])
        details['condition_id'] = topics[2]
        kind = 'SPLIT' if signature == NR_SPLIT else 'MERGE'
    elif signature == NR_REDEEM:
        if len(topics) != 3:
            raise DataError('Invalid adapter redemption topics')
        amounts, amount = abi(['uint256[]', 'uint256'], log['data'])
        details.update(condition_id=topics[2], amounts_raw=[str(x) for x in amounts])
        kind = 'REDEEM'
    else:
        if len(topics) != 4:
            raise DataError('Invalid conversion topics')
        amount, = abi(['uint256'], log['data'])
        details.update(market_id=topics[2], index_set=str(int(topics[3], 16)))
        kind = 'CONVERT'
    return (log['transactionHash'], quantity(log['logIndex']), 0, kind,
            token_address, None, amount, Jsonb(details))


def decode_fill(log):
    topics, address = log['topics'], log['address']
    if not topics:
        return None
    version = 1 if address in EXCHANGE_V1 else 2 if address in EXCHANGE_V2 | {COMBO_EXCHANGE} else None
    if version is None or topics[0] not in (FILL_V1, FILL_V2):
        return None
    if len(topics) != 4 or topics[0] != (FILL_V1 if version == 1 else FILL_V2):
        raise DataError('Unexpected OrderFilled ABI')
    result = {'order_hash': topics[1], 'maker': topic_address(topics[2]),
              'taker': topic_address(topics[3]), 'exchange': address, 'version': version}
    if version == 1:
        maker_asset, taker_asset, making, taking, fee = abi(['uint256'] * 5, log['data'])
        if (maker_asset == 0) == (taker_asset == 0):
            raise DataError('OrderFilled must exchange collateral and a position')
        side, token = (0, taker_asset) if maker_asset == 0 else (1, maker_asset)
    else:
        side, token, making, taking, fee, builder, metadata = abi(
            ['uint8', 'uint256', 'uint256', 'uint256', 'uint256', 'bytes32', 'bytes32'], log['data'])
        if side not in (0, 1):
            raise DataError('Unknown order side')
        result.update(builder='0x' + builder.hex(), metadata='0x' + metadata.hex())
    result.update(side=side, token_id=str(token), making_raw=str(making), taking_raw=str(taking),
                  fee_raw=str(fee), fee_asset=('position' if version == 1 and side == 0 else 'collateral'))
    return result


def adapter_actions(logs, wallet):
    candidates = []
    movements = [row for log in logs for row in movement_rows(log, wallet)]
    for log in logs:
        if log['address'] not in PROTOCOL or not log['topics'] or log['topics'][0] not in ACTION_EVENTS:
            continue
        if len(log['topics']) < 2:
            raise DataError('Missing adapter action actor')
        actor = topic_address(log['topics'][1])
        if actor in COLLATERAL_ADAPTERS:
            candidates.append((log, actor, decode_action(log, actor)))
    result = []
    for log, actor, action in candidates:
        index, kind = action[1], action[3]
        same_actor = [other[2][1] for other in candidates if other[1] == actor]
        previous = max((n for n in same_actor if n < index), default=-1)
        following = min((n for n in same_actor if n > index), default=2**63)
        if kind == 'SPLIT':
            related = [row for row in movements if row[3] in POSITIONS and row[5] == actor
                       and row[6] == wallet and index < row[1] < following]
        else:
            related = [row for row in movements if row[3] in POSITIONS and row[5] == wallet
                       and row[6] == actor and previous < row[1] < index]
        if not related:
            continue
        details = dict(action[7].obj)
        details.update(actor=wallet, actor_onchain=actor, via_adapter=actor,
                       movement_logs=sorted({row[1] for row in related}))
        token_address = action[4]
        if kind != 'CONVERT':
            if 'collateral' in details:
                details['underlying_collateral'] = details['collateral']
            details['collateral'] = PUSD
            token_address = PUSD
        result.append(action[:4] + (token_address,) + action[5:7] + (Jsonb(details),))
    return result


def collateral_operations(logs, wallet):
    result, previous = [], -1
    for log in logs:
        topics = log['topics']
        if log['address'] != PUSD or not topics or topics[0] not in (WRAPPED, UNWRAPPED):
            continue
        if len(topics) != 4:
            raise DataError('Invalid collateral event topics')
        caller, asset, recipient = map(topic_address, topics[1:])
        amount, = abi(['uint256'], log['data'])
        index, wrapping = quantity(log['logIndex']), topics[0] == WRAPPED
        funding_asset = asset if wrapping else PUSD
        funding = []
        for candidate in logs:
            parts = candidate['topics']
            if (candidate['address'] == funding_asset and len(parts) == 3 and parts[0] == TRANSFER
                    and previous < quantity(candidate['logIndex']) < index
                    and topic_address(parts[2]) == PUSD
                    and abi(['uint256'], candidate['data'])[0] == amount):
                funding.append(candidate)
        previous = index
        payer_logs = [source for source in funding if topic_address(source['topics'][1]) == wallet]
        if recipient != wallet and caller != wallet and not payer_logs:
            continue
        internal = caller in COLLATERAL_ADAPTERS or any(
            topic_address(source['topics'][1]) == AUTO_REDEEMER for source in funding)
        details = {'record': 'settlement' if internal else 'operation',
                   'caller': caller, 'underlying_asset': asset, 'recipient': recipient,
                   'funding_logs': [quantity(source['logIndex']) for source in funding],
                   'wallet_funded': bool(payer_logs)}
        result.append((log['transactionHash'], index, 0, 'WRAP' if wrapping else 'UNWRAP',
                       PUSD, -1, amount, Jsonb(details)))
    return result


def module_operations(logs, wallet):
    result = []
    for log in logs:
        topics, address = log['topics'], log['address']
        if address not in MODULES | {AUTO_REDEEMER} or not topics or topics[0] not in MODULE_EVENTS:
            continue
        name, kind, indexed, plain = MODULE_EVENTS[topics[0]]
        if (address == AUTO_REDEEMER) != (name in ('Redemption', 'BinaryRedemption', 'NegRiskRedemption')):
            raise DataError('Unexpected module event contract')
        if len(topics) != len(indexed) + 1:
            raise DataError('Invalid module event topics')
        values = [abi([typ], topic)[0] for (_, typ), topic in zip(indexed, topics[1:], strict=True)]
        values += list(abi([typ for _, typ in plain], log['data']))
        fields = dict(zip((field for field, _ in indexed + plain), values, strict=True))
        if not any((typ == 'address' and value == wallet) or (typ == 'address[]' and wallet in value)
                   for (_, typ), value in zip(indexed + plain, values, strict=True)):
            continue
        details = {key: '0x' + value.hex() if isinstance(value, bytes)
                   else list(value) if isinstance(value, tuple) else str(value)
                   for key, value in fields.items()}
        actor = fields.get('initiator', fields.get('user', fields.get('from')))
        internal = actor in EXCHANGE_V1 | EXCHANGE_V2 | {COMBO_EXCHANGE}
        details.update(record='settlement' if internal else 'operation', event=name, actor=actor)
        if 'position_id' in fields:
            details['position_contract'] = POSITION_MANAGER
        amount = fields.get('payout', fields.get('amount'))
        cash = name in ('PositionsSplit', 'PositionsMerged', 'PositionRedeemed',
                        'HorizontalSplit', 'HorizontalMerge', 'Redemption', 'BinaryRedemption', 'NegRiskRedemption')
        result.append((log['transactionHash'], quantity(log['logIndex']), 0, kind,
                       PUSD if cash else POSITION_MANAGER, -1 if cash else fields.get('position_id'),
                       amount, Jsonb(details)))
    return result


def operation_rows(logs, wallet=WALLET):
    logs = sorted(logs, key=lambda log: quantity(log['logIndex']))
    fills = [(log, decode_fill(log)) for log in logs]
    fills = [(log, fill) for log, fill in fills if fill]
    suppressed = set()
    boundaries = defaultdict(lambda: -1)
    for log in logs:
        topics, address = log['topics'], log['address']
        if not topics or topics[0] not in (MATCH_V1, MATCH_V2):
            continue
        if address not in EXCHANGE_V1 | EXCHANGE_V2 | {COMBO_EXCHANGE}:
            continue
        if len(topics) != 3:
            raise DataError('Invalid OrdersMatched topics')
        maker = topic_address(topics[2])
        end, start = quantity(log['logIndex']), boundaries[address]
        summary = [(source, fill) for source, fill in fills
                   if source['address'] == address and start < quantity(source['logIndex']) < end
                   and fill['order_hash'] == topics[1] and fill['maker'] == maker
                   and fill['taker'] == address]
        if len(summary) != 1:
            raise DataError('OrdersMatched has no unique OrderFilled summary')
        if topics[0] == MATCH_V1:
            maker_asset, taker_asset, making, taking = abi(['uint256'] * 4, log['data'])
            summary_fill = summary[0][1]
            expected = (0, int(summary_fill['token_id'])) if summary_fill['side'] == 0 else (int(summary_fill['token_id']), 0)
            if (maker_asset, taker_asset, making, taking) != expected + (int(summary_fill['making_raw']), int(summary_fill['taking_raw'])):
                raise DataError('OrdersMatched contradicts its OrderFilled summary')
        else:
            side, token, making, taking = abi(['uint8', 'uint256', 'uint256', 'uint256'], log['data'])
            summary_fill = summary[0][1]
            if (side, token, making, taking) != (summary_fill['side'], int(summary_fill['token_id']), int(summary_fill['making_raw']), int(summary_fill['taking_raw'])):
                raise DataError('OrdersMatched contradicts its OrderFilled summary')
        if maker == wallet:
            suppressed.update(quantity(source['logIndex']) for source, fill in fills
                              if source['address'] == address and start < quantity(source['logIndex']) < end
                              and fill['taker'] == wallet and fill['maker'] != wallet)
        boundaries[address] = end
    operations = []
    for log, fill in fills:
        index = quantity(log['logIndex'])
        if fill['maker'] == wallet:
            side, role = fill['side'], 'maker'
        elif fill['taker'] == wallet and index not in suppressed:
            side, role = 1 - fill['side'], 'counterparty'
        else:
            continue
        details = dict(fill, record='operation', role=role)
        amount = int(fill['taking_raw'] if fill['side'] == 0 else fill['making_raw'])
        details['cash_raw'] = fill['making_raw'] if fill['side'] == 0 else fill['taking_raw']
        operations.append((log['transactionHash'], index, 0, 'BUY' if side == 0 else 'SELL',
                           POSITION_MANAGER if log['address'] == COMBO_EXCHANGE else CTF,
                           int(fill['token_id']), amount, Jsonb(details)))
    for log in logs:
        action = decode_action(log, wallet)
        if action:
            operations.append(action)
    extra = adapter_actions(logs, wallet) + collateral_operations(logs, wallet) + module_operations(logs, wallet)
    operations.extend(extra)
    for log in logs:
        for movement in movement_rows(log, wallet):
            tx, index, item, address, token, sender, receiver, amount, delta = movement
            kind = 'MINT' if sender == ZERO else 'BURN' if receiver == ZERO else 'TRANSFER'
            details = {'record': 'movement', 'from': sender, 'to': receiver,
                       'delta_raw': str(delta)}
            operations.append((tx, index, item, kind, address, token, amount, Jsonb(details)))
    return operations


def scan_streams(wallet=WALLET):
    topic = '0x' + wallet[2:].zfill(64)
    return {
        'cash_out': (sorted(CASH), [TRANSFER, topic]),
        'cash_in': (sorted(CASH), [TRANSFER, None, topic]),
        'positions_out': (list(POSITIONS), [[SINGLE, BATCH], None, topic]),
        'positions_in': (list(POSITIONS), [[SINGLE, BATCH], None, None, topic]),
        'actions': (sorted(PROTOCOL - EXCHANGE_V1 - EXCHANGE_V2 - {COMBO_EXCHANGE}), [ACTION_EVENTS, topic]),
        'orders_maker': (sorted(EXCHANGE_V1 | EXCHANGE_V2 | {COMBO_EXCHANGE}), [[FILL_V1, FILL_V2], None, topic]),
        'orders_taker': (sorted(EXCHANGE_V1 | EXCHANGE_V2 | {COMBO_EXCHANGE}), [[FILL_V1, FILL_V2], None, None, topic]),
        'module_actions': (sorted(MODULES | {AUTO_REDEEMER}), [list(MODULE_EVENTS), topic]),
    }


def journal_line(record):
    payload = json.dumps(record, separators=(',', ':'), ensure_ascii=False).encode()
    checksum = zlib.crc32(payload)
    if record.get('kind') in ('range', 'receipts'):
        compressed = base64.b85encode(zlib.compress(payload, 6)).decode()
        return json.dumps({'crc32': checksum, 'zlib': compressed}, separators=(',', ':')).encode() + b'\n'
    return b'{"crc32":' + str(checksum).encode() + b',"record":' + payload + b'}\n'


class Journal:
    def __init__(self, path):
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        self.file = path.open('a+b')

    def append(self, record):
        data = journal_line(record)
        self.file.write(data)
        self.file.flush()
        os.fsync(self.file.fileno())
        return self.file.tell()

    def replay(self, store):
        offset = store.offset()
        self.file.seek(0, 2)
        if self.file.tell() < offset:
            raise DataError('Journal is shorter than the committed database offset')
        self.file.seek(0)
        checksum, remaining = 0, offset
        while remaining:
            chunk = self.file.read(min(4 * 1024 * 1024, remaining))
            checksum = zlib.crc32(chunk, checksum)
            remaining -= len(chunk)
        if checksum != store.checksum():
            raise DataError('Committed journal data has changed')
        count = 0
        while True:
            start = self.file.tell()
            line = self.file.readline()
            if not line:
                break
            if not line.endswith(b'\n'):
                self.file.seek(start)
                self.file.truncate()
                self.file.flush()
                os.fsync(self.file.fileno())
                break
            try:
                envelope = json.loads(line)
                if 'record' in envelope:
                    record = envelope['record']
                    payload = json.dumps(record, separators=(',', ':'), ensure_ascii=False).encode()
                else:
                    payload = zlib.decompress(base64.b85decode(envelope['zlib']))
                    if len(payload) > 256 * 1024 * 1024:
                        raise DataError(f'Compressed record is too large at byte {start}')
                    record = json.loads(payload)
                    if json.dumps(record, separators=(',', ':'), ensure_ascii=False).encode() != payload:
                        raise DataError(f'Noncanonical compressed record at byte {start}')
                expected = zlib.crc32(payload)
                if envelope['crc32'] != expected:
                    raise DataError(f'Journal checksum mismatch at byte {start}')
            except (ValueError, UnicodeDecodeError, KeyError, TypeError, zlib.error) as exc:
                raise DataError(f'Corrupt journal record at byte {start}') from exc
            store.apply(record, self.file.tell(), zlib.crc32(line, store.checksum()))
            count += 1
            if count % 500 == 0:
                print(f'journal: replayed {count:,} records', flush=True)
        self.file.seek(0, 2)
        return count

    def close(self):
        self.file.close()


class Store:
    def __init__(self, conn, metadata):
        self.conn, self.metadata = conn, metadata
        self.wallet, self.snapshot = metadata['wallet'], metadata['snapshot']['number']
        conn.execute((BASE / 'postgresql/schema.sql').read_text())
        if not conn.execute('select pg_try_advisory_lock(137, 46)').fetchone()[0]:
            raise DataError('Database is already in use')
        row = conn.execute('select metadata from run').fetchone()
        if row and row[0] != metadata:
            raise DataError('Database belongs to another run or format')
        if not row:
            conn.execute('insert into run (metadata) values (%s)', (Jsonb(metadata),))
        conn.execute('create temporary table stage_logs (like raw_logs including defaults) on commit delete rows')
        conn.execute('create temporary table stage_transactions (like transactions including defaults) on commit delete rows')

    def offset(self):
        return self.conn.execute('select journal_offset from run').fetchone()[0]

    def checksum(self):
        return self.conn.execute('select journal_crc32 from run').fetchone()[0]

    def next_block(self, stream):
        row = self.conn.execute('select next_block from scan_progress where stream=%s', (stream,)).fetchone()
        return row[0] if row else 0

    def save_logs(self, logs, scanned):
        if not logs:
            return
        seen, rows, transactions, movements = {}, [], {}, []
        for log in logs:
            log = validate_log(log, self.snapshot)
            tx, index = log['transactionHash'], quantity(log['logIndex'])
            identity = (tx, index)
            fields = (tx, index, quantity(log['blockNumber']), log['blockHash'], log['address'],
                      log['topics'], log['data'], scanned)
            if identity in seen:
                if seen[identity] != fields:
                    raise DataError('Conflicting duplicate logs')
                continue
            seen[identity] = fields
            rows.append(fields[:5] + (Jsonb(fields[5]),) + fields[6:])
            txrow = (tx, quantity(log['blockNumber']), log['blockHash'], quantity(log['transactionIndex']))
            if tx in transactions and transactions[tx] != txrow:
                raise DataError('Transaction appears in different blocks')
            transactions[tx] = txrow
            movements.extend(movement_rows(log, self.wallet))
        with self.conn.cursor() as cur:
            with cur.copy('copy stage_transactions (tx_hash,block_number,block_hash,transaction_index) from stdin') as copy:
                for row in transactions.values():
                    copy.write_row(row)
            if cur.execute('''select 1 from stage_transactions s join transactions t using(tx_hash)
                where (s.block_number,s.block_hash,s.transaction_index) <>
                      (t.block_number,t.block_hash,t.transaction_index) limit 1''').fetchone():
                raise DataError('Conflicting transaction block')
            cur.execute('insert into transactions select * from stage_transactions on conflict do nothing')
            cur.execute('truncate stage_transactions')
            with cur.copy('copy stage_logs from stdin') as copy:
                for row in rows:
                    copy.write_row(row)
            if cur.execute('''select 1 from stage_logs s join raw_logs r using(tx_hash,log_index)
                where (s.block_number,s.block_hash,s.address,s.topics,s.data) <>
                      (r.block_number,r.block_hash,r.address,r.topics,r.data) limit 1''').fetchone():
                raise DataError('Conflicting raw log')
            cur.execute('''insert into raw_logs select * from stage_logs
                on conflict (tx_hash,log_index) do update set scanned=true
                where excluded.scanned and not raw_logs.scanned''')
            cur.execute('truncate stage_logs')
            cur.executemany('insert into movements values (%s,%s,%s,%s,%s,%s,%s,%s,%s) on conflict do nothing', movements)

    def apply(self, record, offset, checksum=None):
        with self.conn.transaction():
            if offset <= self.offset():
                raise DataError('Journal offset did not advance')
            if checksum is None:
                checksum = zlib.crc32(journal_line(record), self.checksum())
            kind = record.get('kind')
            if kind == 'header':
                if self.offset() != 0 or record.get('metadata') != self.metadata:
                    raise DataError('Journal belongs to another run or format')
            elif self.offset() == 0:
                raise DataError('Journal header is missing')
            elif kind == 'range':
                stream = record['stream']
                if stream not in scan_streams(self.wallet):
                    raise DataError('Unknown scan stream')
                if record['from'] != self.next_block(stream) or not record['from'] <= record['to'] <= self.snapshot:
                    raise DataError('Gap or overlap in scan journal')
                addresses, topics = scan_streams(self.wallet)[stream]
                flt = {'address': addresses, 'topics': topics,
                       'fromBlock': hex(record['from']), 'toBlock': hex(record['to'])}
                logs = [validate_log(log, self.snapshot, flt) for log in record['logs']]
                self.save_logs(logs, True)
                self.conn.execute('''insert into scan_progress values (%s,%s)
                    on conflict(stream) do update set next_block=excluded.next_block''',
                                  (stream, record['to'] + 1))
            elif kind == 'receipts':
                self.save_receipts(record)
            elif kind == 'balances':
                if record['source'] not in self.metadata['verification_sources']:
                    raise DataError('Unexpected verification source')
                self.prepare_balances()
                self.conn.cursor().executemany('''insert into verification values (%s,%s,%s,%s)
                    on conflict(token_address,token_id,source) do update set onchain=excluded.onchain''',
                    [(address, int(token), record['source'], int(value)) for address, token, value in record['values']])
            elif kind == 'snapshot_checked':
                if record['source'] not in self.metadata['verification_sources'] or record['hash'] != self.metadata['snapshot']['hash']:
                    raise DataError('Unexpected verification source or snapshot')
                self.conn.execute('''insert into source_checks values (%s,%s)
                    on conflict(source) do update set block_hash=excluded.block_hash''',
                                  (record['source'], record['hash']))
            else:
                raise DataError(f'Unknown journal record: {kind}')
            self.conn.execute('update run set journal_offset=%s,journal_crc32=%s', (offset, checksum))

    def save_receipts(self, record):
        for block in record['blocks']:
            number, block_hash = quantity(block['number']), hex_value(block['hash'], 32)
            timestamp = quantity(block['timestamp'])
            existing = self.conn.execute('select hash,timestamp from blocks where number=%s', (number,)).fetchone()
            if existing and existing != (block_hash, timestamp):
                raise DataError('Conflicting block header')
            self.conn.execute('insert into blocks values (%s,%s,%s) on conflict do nothing',
                              (number, block_hash, timestamp))
        receipts = record['items']
        hashes = [hex_value(receipt['transactionHash'], 32) for receipt in receipts]
        if len(hashes) != len(set(hashes)):
            raise DataError('Duplicate receipt in a journal batch')
        expected_rows = self.conn.execute('''select tx_hash,block_number,block_hash,transaction_index,receipt_done
            from transactions where tx_hash=any(%s)''', (hashes,)).fetchall()
        expected_by_hash = {row[0]: row[1:] for row in expected_rows}
        if len(expected_by_hash) != len(hashes):
            raise DataError('A receipt was not requested')
        scanned_by_hash = defaultdict(list)
        for row in self.conn.execute('''select tx_hash,log_index,address,topics,data from raw_logs
            where tx_hash=any(%s) and scanned''', (hashes,)):
            scanned_by_hash[row[0]].append(row[1:])
        relevant_logs, operations, updates = [], [], []
        for receipt in record['items']:
            tx = hex_value(receipt['transactionHash'], 32)
            expected = expected_by_hash[tx]
            if expected[3]:
                raise DataError('Receipt was already committed')
            actual = (quantity(receipt['blockNumber']), hex_value(receipt['blockHash'], 32),
                      quantity(receipt['transactionIndex']))
            if actual != expected[:3] or quantity(receipt['status']) != 1:
                raise DataError('Receipt contradicts scanned logs')
            header = self.conn.execute('select hash,timestamp from blocks where number=%s', (actual[0],)).fetchone()
            if not header or header[0] != actual[1]:
                raise DataError('Receipt block header is missing or changed')
            sender = hex_value(receipt['from'], 20)
            receiver = hex_value(receipt['to'], 20) if receipt.get('to') is not None else None
            logs = [validate_log(log, self.snapshot) for log in receipt['logs']]
            if any('blockTimestamp' in log and quantity(log['blockTimestamp']) not in (0, header[1]) for log in logs):
                raise DataError('Receipt timestamp contradicts the cached block')
            if any(log['transactionHash'] != tx or log['blockHash'] != actual[1]
                   or quantity(log['blockNumber']) != actual[0]
                   or quantity(log['transactionIndex']) != actual[2] for log in logs):
                raise DataError('Receipt contains a log from another transaction')
            if len({quantity(log['logIndex']) for log in logs}) != len(logs):
                raise DataError('Duplicate receipt log')
            by_index = {quantity(log['logIndex']): log for log in logs}
            scanned = scanned_by_hash[tx]
            for index, address, topics, data in scanned:
                log = by_index.get(index)
                if not log or (log['address'], log['topics'], log['data']) != (address, topics, data):
                    raise DataError('A scanned log is missing from the receipt')
            scanned_ids = {row[0] for row in scanned}
            if any(movement_rows(log, self.wallet) and quantity(log['logIndex']) not in scanned_ids for log in logs):
                raise DataError('Receipt reveals a wallet transfer missing from the scan')
            protocol_logs = [log for log in logs if log['address'] in PROTOCOL | set(CASH)]
            rows = operation_rows(protocol_logs, self.wallet)
            keys = {(row[0], row[1]) for row in rows}
            for row in rows:
                for field in ('movement_logs', 'funding_logs'):
                    keys.update((tx, index) for index in row[7].obj.get(field, []))
            relevant_logs.extend(log for log in protocol_logs
                                 if movement_rows(log, self.wallet)
                                 or (log['topics'] and log['topics'][0] in (MATCH_V1, MATCH_V2))
                                 or (log['transactionHash'], quantity(log['logIndex'])) in keys)
            operations.extend(rows)
            updates.append((sender, receiver, tx))
        self.save_logs(relevant_logs, False)
        self.conn.cursor().executemany(
            'insert into operations values (%s,%s,%s,%s,%s,%s,%s,%s) on conflict do nothing', operations)
        self.conn.cursor().executemany('''update transactions set from_address=%s,to_address=%s,
            status=1,receipt_done=true where tx_hash=%s''', updates)

    def prepare_balances(self):
        if self.conn.execute('select 1 from balances limit 1').fetchone():
            return
        self.require_history()
        self.conn.execute('''insert into balances select token_address,token_id,sum(delta_raw)
            from movements group by token_address,token_id''')
        self.conn.execute('''insert into balances
            select distinct token_address,token_id,0 from operations
            where token_address=any(%s) and token_id>=0 on conflict do nothing''', (list(POSITIONS),))
        self.conn.execute('''insert into balances
            select distinct details->>'position_contract',(details->>'position_id')::numeric,0 from operations
            where details->>'position_contract'=any(%s) and details ? 'position_id'
            on conflict do nothing''', (list(POSITIONS),))
        for address in CASH:
            self.conn.execute('insert into balances values (%s,-1,0) on conflict do nothing', (address,))
        if self.conn.execute('select 1 from balances where calculated<0 limit 1').fetchone():
            raise DataError('A reconstructed balance is negative')

    def require_history(self):
        for stream in scan_streams(self.wallet):
            if self.next_block(stream) != self.snapshot + 1:
                raise DataError(f'Incomplete scan: {stream}')
        if self.conn.execute('select 1 from transactions where not receipt_done limit 1').fetchone():
            raise DataError('Receipts are incomplete')


def commit_record(journal, store, record):
    start = store.offset()
    offset = journal.append(record)
    try:
        store.apply(record, offset)
    except DataError:
        atomic_json(journal.path.with_name('rejected.json'), record)
        journal.file.seek(start)
        journal.file.truncate()
        journal.file.flush()
        os.fsync(journal.file.fileno())
        raise


def scan_history(rpc, store, journal, chunk_size, workers=4):
    for stream, (addresses, topics) in scan_streams(store.wallet).items():
        block, chunk, batch_width, last_print = store.next_block(stream), chunk_size, min(workers * 4, 16), 0
        if block > store.snapshot:
            continue
        print(f'{stream}: block {block:,} / {store.snapshot:,}', flush=True)
        while block <= store.snapshot:
            filters = [{'address': addresses, 'topics': topics, 'fromBlock': hex(start),
                        'toBlock': hex(min(start + chunk - 1, store.snapshot))}
                       for start in range(block, min(block + chunk * batch_width, store.snapshot + 1), chunk)]
            try:
                batches = rpc.batch([('eth_getLogs', [flt]) for flt in filters])
            except RangeError:
                if chunk == 1:
                    raise
                chunk = max(1, chunk // 2)
                continue
            except RpcError:
                if batch_width == 1:
                    raise
                batch_width = max(1, batch_width // 2)
                print(f'{stream}: RPC batch width reduced to {batch_width}', flush=True)
                continue
            found = 0
            for flt, logs in zip(filters, batches, strict=True):
                if not isinstance(logs, list):
                    raise DataError('eth_getLogs returned a non-list result')
                logs = [validate_log(log, store.snapshot, flt) for log in logs]
                end = quantity(flt['toBlock'])
                commit_record(journal, store, {'kind': 'range', 'stream': stream,
                                              'from': quantity(flt['fromBlock']), 'to': end, 'logs': logs})
                block = end + 1
                found += len(logs)
            if time.monotonic() - last_print > 5 or block > store.snapshot:
                print(f'{stream}: {block / (store.snapshot + 1):.1%}, block {end:,}, logs +{found:,}', flush=True)
                last_print = time.monotonic()


def receipt_block_headers(receipts):
    headers = {}
    for receipt in receipts:
        timestamps = {quantity(log['blockTimestamp']) for log in receipt.get('logs', []) if 'blockTimestamp' in log}
        timestamps.discard(0)
        if len(timestamps) > 1:
            raise DataError('Conflicting block timestamps in a receipt')
        if not timestamps:
            continue
        number = quantity(receipt['blockNumber'])
        header = {'number': hex(number), 'hash': hex_value(receipt['blockHash'], 32),
                  'timestamp': hex(timestamps.pop())}
        if number in headers and headers[number] != header:
            raise DataError('Conflicting receipt block headers')
        headers[number] = header
    return headers


def process_receipts(rpc, store, journal, workers):
    conn = store.conn
    pending = conn.execute('select count(*) from transactions where not receipt_done').fetchone()[0]
    if not pending:
        return
    processed, started = 0, time.monotonic()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        def schedule(rows):
            parts = [rows[i:i + 50] for i in range(0, len(rows), 50)]
            return parts, [pool.submit(rpc.batch, [('eth_getTransactionReceipt', [row[0]]) for row in part])
                           for part in parts]
        rows = conn.execute('''select tx_hash,block_number,block_hash,transaction_index from transactions
            where not receipt_done order by block_number,transaction_index,tx_hash limit %s''', (workers * 50,)).fetchall()
        parts, futures = schedule(rows)
        while rows:
            batches = [future.result() for future in futures]
            last = rows[-1]
            next_rows = conn.execute('''select tx_hash,block_number,block_hash,transaction_index from transactions
                where not receipt_done and (block_number,transaction_index,tx_hash)>(%s,%s,%s)
                order by block_number,transaction_index,tx_hash limit %s''',
                (last[1], last[3], last[0], workers * 50)).fetchall()
            next_parts, next_futures = schedule(next_rows)
            all_receipts = []
            for part, receipts in zip(parts, batches, strict=True):
                for expected, receipt in zip(part, receipts, strict=True):
                    if not isinstance(receipt, dict) or hex_value(receipt.get('transactionHash'), 32) != expected[0]:
                        raise DataError('RPC returned the wrong transaction receipt')
                all_receipts.extend(receipts)
            numbers = sorted({quantity(receipt['blockNumber']) for receipt in all_receipts})
            cached = {row[0] for row in conn.execute('select number from blocks where number=any(%s)', (numbers,))}
            provided = receipt_block_headers(all_receipts)
            missing = [number for number in numbers if number not in cached and number not in provided]
            header_calls = [[('eth_getBlockByNumber', [hex(number), False]) for number in missing[i:i + 50]]
                            for i in range(0, len(missing), 50)]
            blocks = [header for batch in pool.map(rpc.batch, header_calls) for header in batch]
            for number, header in zip(missing, blocks, strict=True):
                if not isinstance(header, dict) or quantity(header.get('number')) != number:
                    raise DataError('RPC returned the wrong block header')
            headers = [{key: header[key] for key in ('number', 'hash', 'timestamp')} for header in blocks]
            headers.extend(header for number, header in provided.items() if number not in cached)
            headers.sort(key=lambda header: quantity(header['number']))
            commit_record(journal, store, {'kind': 'receipts', 'items': all_receipts, 'blocks': headers})
            processed += len(all_receipts)
            print(f'receipts: {processed:,}/{pending:,}, {processed / max(1, time.monotonic() - started):.1f} tx/s', flush=True)
            rows, parts, futures = next_rows, next_parts, next_futures


def checked_header(rpc, snapshot):
    if quantity(rpc.call('eth_chainId', [])) != 137:
        raise DataError('RPC is not Polygon mainnet')
    header = rpc.call('eth_getBlockByNumber', [hex(snapshot['number']), False])
    if not isinstance(header, dict) or quantity(header.get('number')) != snapshot['number']:
        raise DataError('Snapshot block was not returned')
    if hex_value(header.get('hash'), 32) != snapshot['hash']:
        raise DataError('Snapshot block hash changed')
    return header


def available_nodes(urls):
    def check(url):
        try:
            client = Rpc([url], attempts=2, timeout=12)
            if quantity(client.call('eth_chainId', [])) == 137:
                return url
        except (RpcError, DataError):
            pass
        return None
    with ThreadPoolExecutor(max_workers=min(4, len(urls))) as pool:
        result = [url for url in pool.map(check, urls) if url]
    if not result:
        raise RpcError('No available Polygon RPC nodes')
    return result


def archive_nodes(urls, snapshot):
    selected, seen = [], set()
    for url in urls:
        source = rpc_source(url)
        if source in seen:
            continue
        client = Rpc([url], attempts=2, timeout=20)
        try:
            checked_header(client, snapshot)
            code = hex_value(client.call('eth_getCode', [PUSD, hex(snapshot['number'])]))
            if code != '0x':
                value = client.call('eth_call', [{'to': PUSD, 'data': '0x70a08231' + WALLET[2:].zfill(64)}, hex(snapshot['number'])])
                abi(['uint256'], value)
        except (RpcError, DataError, ValueError):
            continue
        selected.append(url)
        seen.add(source)
        if len(selected) == 2:
            return selected
    raise RpcError('Two independent RPC hosts with historical state are required')


def verify_balances(store, journal, urls):
    with store.conn.transaction():
        store.prepare_balances()
    conn = store.conn
    total = conn.execute('select count(*) from balances').fetchone()[0]
    for url in urls:
        source, client = rpc_source(url), Rpc([url])
        checked_header(client, store.metadata['snapshot'])
        print(f'verification: {source}, {total:,} assets', flush=True)
        for address in list(CASH) + list(POSITIONS):
            code = hex_value(client.call('eth_getCode', [address, hex(store.snapshot)]))
            if address in CASH and code != '0x':
                decimals = client.call('eth_call', [{'to': address, 'data': '0x313ce567'}, hex(store.snapshot)])
                if abi(['uint8'], decimals)[0] != 6:
                    raise DataError('Unexpected cash token decimals')
            while True:
                rows = conn.execute('''select b.token_id from balances b where b.token_address=%s
                    and not exists (select 1 from verification v where v.token_address=b.token_address
                      and v.token_id=b.token_id and v.source=%s) order by b.token_id limit 5000''',
                                    (address, source)).fetchall()
                if not rows:
                    break
                tokens = [int(row[0]) for row in rows]
                if code == '0x':
                    values = [0] * len(tokens)
                elif address in CASH:
                    value = client.call('eth_call', [{'to': address, 'data': '0x70a08231' + store.wallet[2:].zfill(64)}, hex(store.snapshot)])
                    values = [abi(['uint256'], value)[0]]
                else:
                    chunks = [tokens[i:i + 500] for i in range(0, len(tokens), 500)]
                    calls = []
                    for chunk in chunks:
                        data = keccak(text='balanceOfBatch(address[],uint256[])')[:4] + encode(
                            ['address[]', 'uint256[]'], [[store.wallet] * len(chunk), chunk])
                        calls.append(('eth_call', [{'to': address, 'data': '0x' + data.hex()}, hex(store.snapshot)]))
                    answers = client.batch(calls)
                    values = []
                    for chunk, answer in zip(chunks, answers, strict=True):
                        decoded, = abi(['uint256[]'], answer)
                        if len(chunk) != len(decoded):
                            raise DataError('balanceOfBatch returned the wrong number of balances')
                        values.extend(decoded)
                if len(tokens) != len(values):
                    raise DataError('Incomplete balance verification')
                record = {'kind': 'balances', 'source': source,
                          'values': [[address, str(token), str(value)] for token, value in zip(tokens, values, strict=True)]}
                commit_record(journal, store, record)
                print(f'verification: {source}, {CASH.get(address, address)}, +{len(tokens):,}', flush=True)
        checked_header(client, store.metadata['snapshot'])
        commit_record(journal, store, {'kind': 'snapshot_checked', 'source': source,
                                      'hash': store.metadata['snapshot']['hash']})


def result_summary(store):
    store.require_history()
    conn = store.conn
    sources = store.metadata['verification_sources']
    if len(sources) != 2 or len(set(sources)) != 2:
        raise DataError('Two verification sources are required')
    for source in sources:
        if not conn.execute('select 1 from source_checks where source=%s and block_hash=%s',
                            (source, store.metadata['snapshot']['hash'])).fetchone():
            raise DataError('Final snapshot check is missing')
        if conn.execute('''select 1 from balances b where not exists (select 1 from verification v
            where v.token_address=b.token_address and v.token_id=b.token_id and v.source=%s) limit 1''', (source,)).fetchone():
            raise DataError('Balance verification is incomplete')
    counts = {name: conn.execute(sql.SQL('select count(*) from {}').format(sql.Identifier(name))).fetchone()[0]
              for name in ('transactions', 'raw_logs', 'movements', 'operations', 'balances')}
    differences = conn.execute('''select count(*) from balances b where exists (
        select 1 from verification v where v.token_address=b.token_address and v.token_id=b.token_id
        and v.onchain<>b.calculated)''').fetchone()[0]
    return {'run_id': store.metadata['run_id'], 'wallet': store.wallet, 'chain_id': 137,
            'snapshot': store.metadata['snapshot'], 'status': 'mismatch' if differences else 'complete',
            'counts': counts, 'mismatches': differences, 'verification_sources': sources,
            'operation_counts': dict(conn.execute('select operation_type,count(*) from operations group by operation_type order by operation_type').fetchall())}


def write_results(store, output):
    summary = result_summary(store)
    output.mkdir(parents=True, exist_ok=True)
    temp = output / 'result.json.tmp'
    sources = summary['verification_sources']
    with temp.open('w', encoding='utf-8') as file:
        prefix = json.dumps(summary, ensure_ascii=False, separators=(',', ':'))
        file.write(prefix[:-1] + ',"assets":[')
        for index, address in enumerate(list(CASH) + list(POSITIONS)):
            if index:
                file.write(',')
            meta = {'address': address, 'standard': 'ERC20' if address in CASH else 'ERC1155',
                    'symbol': CASH.get(address), 'decimals': 6 if address in CASH else None}
            file.write(json.dumps(meta, separators=(',', ':'))[:-1] + ',"balances":[')
            with store.conn.transaction():
                with store.conn.cursor(name='report_balances') as cur:
                    cur.execute('''select b.token_id,b.calculated,v1.onchain,v2.onchain from balances b
                        join verification v1 on (v1.token_address,v1.token_id)=(b.token_address,b.token_id) and v1.source=%s
                        join verification v2 on (v2.token_address,v2.token_id)=(b.token_address,b.token_id) and v2.source=%s
                        where b.token_address=%s order by b.token_id''', (sources[0], sources[1], address))
                    first = True
                    for token, calculated, first_value, second_value in cur:
                        token, calculated, first_value, second_value = map(int, (token, calculated, first_value, second_value))
                        if not first:
                            file.write(',')
                        first = False
                        row = {'token_id': str(token) if token >= 0 else None, 'calculated': str(calculated),
                               'onchain': [str(first_value), str(second_value)],
                               'difference': [str(calculated - first_value), str(calculated - second_value)]}
                        file.write(json.dumps(row, separators=(',', ':')))
            file.write(']}')
        file.write(']}\n')
        file.flush()
        os.fsync(file.fileno())
    os.replace(temp, output / 'result.json')
    lines = [f"wallet: {store.wallet}", f"run: {summary['run_id']}",
             f"snapshot: {store.snapshot} ({summary['snapshot']['hash']})",
             f"status: {summary['status']}"]
    lines.extend(f'{key}: {value}' for key, value in summary['counts'].items())
    lines.append(f"mismatches: {summary['mismatches']}")
    lines.append('verification: ' + ', '.join(sources))
    for address, symbol in CASH.items():
        value = store.conn.execute('select calculated from balances where token_address=%s and token_id=-1', (address,)).fetchone()[0]
        value = int(value)
        lines.append(f'{symbol}: {value // 1000000}.{value % 1000000:06d}')
    text_temp = output / 'result.txt.tmp'
    with text_temp.open('w', encoding='utf-8') as file:
        file.write('\n'.join(lines) + '\n')
        file.flush()
        os.fsync(file.fileno())
    os.replace(text_temp, output / 'result.txt')
    sync_directory(output)
    store.conn.execute('update run set status=%s', (summary['status'],))
    atomic_json(output / 'status.json', summary)
    return summary


def create_metadata(rpc, snapshot_number, urls):
    finalized = rpc.call('eth_getBlockByNumber', ['finalized', False])
    if snapshot_number is not None and snapshot_number > quantity(finalized['number']):
        raise DataError('Requested snapshot is not finalized')
    tag = hex(snapshot_number) if snapshot_number is not None else 'finalized'
    block = rpc.call('eth_getBlockByNumber', [tag, False]) if snapshot_number is not None else finalized
    snapshot = {'number': quantity(block['number']), 'hash': hex_value(block['hash'], 32),
                'timestamp': quantity(block['timestamp'])}
    if snapshot_number is not None and snapshot['number'] != snapshot_number:
        raise DataError('Unexpected snapshot block')
    verifiers = archive_nodes(urls, snapshot)
    for address in POSITIONS:
        code = hex_value(rpc.call('eth_getCode', [address, hex(snapshot['number'])]))
        if code != '0x':
            answer = rpc.call('eth_call', [{'to': address, 'data': '0x01ffc9a7d9b67a26' + '0' * 56}, hex(snapshot['number'])])
            if abi(['bool'], answer) != (True,):
                raise DataError('A registered position contract does not support ERC1155')
    run_id = time.strftime('%Y%m%d_%H%M%S', time.gmtime()) + '_' + uuid.uuid4().hex[:8]
    metadata = {'format_version': FORMAT_VERSION, 'run_id': run_id, 'chain_id': 137, 'wallet': WALLET,
                'snapshot': snapshot, 'verification_sources': [rpc_source(url) for url in verifiers],
                'cash_contracts': sorted(CASH), 'position_contracts': list(POSITIONS),
                'protocol_contracts': sorted(PROTOCOL)}
    return metadata, verifiers


def arguments(argv=None):
    parser = argparse.ArgumentParser(description='Reconstruct the Polymarket wallet using Polygon RPC.')
    parser.add_argument('--fresh', action='store_true', help='start a separate run without old data')
    parser.add_argument('--snapshot', type=int, help='fixed Polygon block; defaults to finalized')
    parser.add_argument('--work-dir', type=Path, default=BASE / '.work')
    parser.add_argument('--output-dir', type=Path, default=BASE / 'result')
    parser.add_argument('--rpc', action='append', help='Polygon RPC URL; may be repeated')
    parser.add_argument('--verify-rpc', action='append', help='archive RPC URL; may be repeated')
    parser.add_argument('--pg-bin', help='directory containing PostgreSQL binaries')
    parser.add_argument('--database-name', help='separate database for replay or verification')
    parser.add_argument('--replay-only', action='store_true', help='rebuild from the current journal without network requests')
    parser.add_argument('--chunk-size', type=int, default=100000)
    parser.add_argument('--workers', type=int, default=4)
    args = parser.parse_args(argv)
    if args.chunk_size < 1 or not 1 <= args.workers <= 16 or args.snapshot is not None and args.snapshot < 0:
        parser.error('Invalid chunk size, worker count or snapshot')
    if args.fresh and args.replay_only:
        parser.error('--fresh cannot be combined with --replay-only')
    return args


def main(argv=None):
    try:
        args = arguments(argv)
    except SystemExit as exc:
        return 1 if exc.code else 0
    work, output = args.work_dir.resolve(), args.output_dir.resolve()
    lock, pg, conn, journal, metadata = None, None, None, None, None
    try:
        lock = WorkLock(work)
        atomic_json(output / 'status.json', {'status': 'starting'})
        pg = LocalPostgres(work / 'postgres', args.pg_bin)
        pg.start()
        current = work / 'current.json'
        if current.exists() and not args.fresh:
            metadata = json.loads(current.read_text())
            if metadata.get('format_version') != FORMAT_VERSION or metadata.get('wallet') != WALLET or metadata.get('chain_id') != 137:
                raise DataError('Incompatible checkpoint; use --fresh for a separate run')
            if (metadata.get('cash_contracts') != sorted(CASH) or metadata.get('position_contracts') != list(POSITIONS)
                    or metadata.get('protocol_contracts') != sorted(PROTOCOL)):
                raise DataError('Contract registry changed; use --fresh')
            if args.snapshot is not None and args.snapshot != metadata['snapshot']['number']:
                raise DataError('Requested snapshot differs from the saved run; use --fresh')
        elif args.replay_only:
            raise DataError('There is no saved run to replay')
        urls, verifiers = [], []
        if not args.replay_only:
            urls = available_nodes(args.rpc or RPC_URLS)
            rpc = Rpc(urls)
            if metadata is None:
                metadata, verifiers = create_metadata(rpc, args.snapshot, args.verify_rpc or urls)
                atomic_json(current, metadata)
            else:
                checked_header(rpc, metadata['snapshot'])
                verifier_urls = args.verify_rpc or urls
                verifiers = [url for source in metadata['verification_sources']
                             for url in verifier_urls if rpc_source(url) == source][:2]
                if len(verifiers) != 2:
                    raise DataError('The original verification RPC hosts must be available')
        run_dir = work / 'runs' / metadata['run_id']
        run_dir.mkdir(parents=True, exist_ok=True)
        atomic_json(output / 'status.json', {'status': 'running', 'run_id': metadata['run_id'],
                                            'snapshot': metadata['snapshot']})
        db_name = args.database_name or ('pw_' + metadata['run_id'])
        conn = pg.database(db_name)
        store = Store(conn, metadata)
        path = run_dir / 'journal.jsonl'
        if not path.exists() and store.offset():
            raise DataError('The journal for this database is missing')
        journal = Journal(path)
        if path.stat().st_size == 0:
            commit_record(journal, store, {'kind': 'header', 'metadata': metadata})
        journal.replay(store)
        print(f"run: {metadata['run_id']}; snapshot: {store.snapshot}; database: {db_name}", flush=True)
        if not args.replay_only:
            scan_history(rpc, store, journal, args.chunk_size, args.workers)
            process_receipts(rpc, store, journal, args.workers)
            verify_balances(store, journal, verifiers)
        summary = write_results(store, output)
        print(f"{summary['status']}: {summary['mismatches']} mismatches; {output / 'result.json'}", flush=True)
        return 2 if summary['mismatches'] else 0
    except KeyboardInterrupt:
        if lock:
            atomic_json(output / 'status.json', {'status': 'interrupted', 'run_id': metadata['run_id'] if metadata else None})
        print('Interrupted; the next run will resume from the journal.', file=sys.stderr)
        return 130
    except Exception as exc:
        if lock:
            with contextlib.suppress(OSError):
                atomic_json(output / 'status.json', {'status': 'error', 'run_id': metadata['run_id'] if metadata else None,
                                                    'error': str(exc)})
        print(f'{type(exc).__name__}: {exc}', file=sys.stderr)
        return 1
    finally:
        if journal:
            journal.close()
        if conn:
            conn.close()
        if pg:
            pg.close()
        if lock:
            lock.close()


if __name__ == '__main__':
    sys.exit(main())
