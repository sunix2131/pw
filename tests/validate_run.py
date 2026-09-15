import argparse
import gzip
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import uuid
import zipfile
import zlib
from itertools import zip_longest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import main as app

TABLES = {
    'run': 'id', 'scan_progress': 'stream', 'blocks': 'number', 'transactions': 'tx_hash',
    'raw_logs': 'tx_hash,log_index', 'movements': 'tx_hash,log_index,item_index',
    'operations': 'tx_hash,log_index,item_index', 'balances': 'token_address,token_id',
    'verification': 'token_address,token_id,source', 'source_checks': 'source',
}


def table_state(conn, table):
    checksum, size = 0, 0
    with conn.transaction():
        with conn.cursor() as cursor:
            cursor.execute('set local enable_indexscan=off')
            cursor.execute("set local work_mem='128MB'")
            with cursor.copy(f'copy (select * from {table} order by {TABLES[table]}) to stdout (format binary)') as copy:
                for data in copy:
                    checksum = zlib.crc32(data, checksum)
                    size += len(data)
        count = conn.execute(f'select count(*) from {table}').fetchone()[0]
    return count, size, checksum


def compare_tables(first, second):
    counts = {}
    for table in TABLES:
        print(f'compare: {table}', flush=True)
        left, right = table_state(first, table), table_state(second, table)
        if left != right:
            raise app.DataError(f'Database copies differ: {table}')
        counts[table] = left[0]
    return counts


def verify_report(conn, path, metadata):
    report = json.loads(path.read_text())
    for key in ('run_id', 'wallet', 'chain_id', 'snapshot', 'verification_sources'):
        if report[key] != metadata[key]:
            raise app.DataError(f'Report metadata differs from SQL: {key}')
    addresses = list(app.CASH) + list(app.POSITIONS)
    if [asset['address'] for asset in report['assets']] != addresses:
        raise app.DataError('Report omits or repeats an asset contract')
    sources = metadata['verification_sources']
    checked, mismatches = 0, 0
    for asset in report['assets']:
        with conn.transaction():
            with conn.cursor(name='verify_report') as cursor:
                cursor.execute('''select b.token_id,b.calculated,v1.onchain,v2.onchain from balances b
                    join verification v1 using(token_address,token_id)
                    join verification v2 using(token_address,token_id)
                    where b.token_address=%s and v1.source=%s and v2.source=%s order by b.token_id''',
                    (asset['address'], *sources))
                for row, actual in zip_longest(cursor, asset['balances']):
                    if row is None or actual is None:
                        raise app.DataError('Report balance count differs from SQL')
                    token, calculated, first, second = map(int, row)
                    expected = {'token_id': str(token) if token >= 0 else None,
                        'calculated': str(calculated), 'onchain': [str(first), str(second)],
                        'difference': [str(calculated-first), str(calculated-second)]}
                    if expected != actual:
                        raise app.DataError('A JSON balance differs from SQL')
                    checked += 1
                    mismatches += calculated != first or calculated != second
    if checked != report['counts']['balances'] or mismatches != report['mismatches']:
        raise app.DataError('Report totals differ from SQL')
    return checked


def export_jsonl(conn, query, destination):
    count = 0
    with gzip.open(destination, 'wt', encoding='utf-8', compresslevel=6) as file:
        with conn.transaction():
            with conn.cursor(name='export_rows') as cursor:
                cursor.execute(query)
                for row, in cursor:
                    file.write(json.dumps(row, ensure_ascii=False, separators=(',', ':')) + '\n')
                    count += 1
    return count


def export_samples(conn, output):
    for label, direction in [('first', 'asc'), ('last', 'desc')]:
        rows = conn.execute(f'''select tx_hash,log_index,block_number,block_hash,address,topics,data
            from raw_logs where scanned order by block_number {direction},log_index {direction} limit 20''').fetchall()
        if direction == 'desc': rows.reverse()
        with (output / f'logs_{label}.jsonl').open('w') as file:
            for tx, index, block, block_hash, address, topics, data in rows:
                file.write(json.dumps({'transactionHash': tx, 'logIndex': hex(index), 'blockNumber': hex(block),
                    'blockHash': block_hash, 'address': address, 'topics': topics, 'data': data}, separators=(',', ':')) + '\n')


def release_parts(path, limit=1900 * 1024 * 1024):
    if path.stat().st_size <= limit:
        return [path]
    parts = []
    with path.open('rb') as source:
        while source.tell() < path.stat().st_size:
            part = path.with_name(f'{path.name}.{len(parts) + 1:03d}')
            with part.open('wb') as target:
                remaining = limit
                while remaining and (data := source.read(min(1024 * 1024, remaining))):
                    target.write(data)
                    remaining -= len(data)
            parts.append(part)
    with path.open('rb') as source:
        for part in parts:
            with part.open('rb') as target:
                while data := target.read(1024 * 1024):
                    if source.read(len(data)) != data:
                        raise app.DataError('Release parts differ from the original file')
        if source.read(1):
            raise app.DataError('Release parts are incomplete')
    return parts


def validate(work, output, replay_name=None):
    output.mkdir(parents=True, exist_ok=True)
    evidence_dir = app.BASE / '.validation'
    evidence_dir.mkdir(exist_ok=True)
    test = subprocess.run([sys.executable, '-m', 'unittest', 'discover', '-s', 'tests', '-v'],
                          cwd=app.BASE, capture_output=True, text=True)
    (evidence_dir / 'tests.log').write_text(test.stdout + test.stderr)
    if test.returncode:
        raise app.DataError('Regression tests failed; see .validation/tests.log')
    test_count = int(re.search(r'Ran (\d+) tests', test.stderr)[1])
    metadata = json.loads((work / 'current.json').read_text())
    run_id = metadata['run_id']
    replay_output = evidence_dir / 'replay_result'
    if replay_name is None:
        replay_name = 'replay_' + uuid.uuid4().hex[:16]
        with (evidence_dir / 'replay.log').open('w') as log:
            replay = subprocess.run([sys.executable, str(app.BASE / 'main.py'), '--work-dir', str(work),
                '--output-dir', str(replay_output), '--database-name', replay_name, '--replay-only'], stdout=log, stderr=log)
        if replay.returncode:
            raise app.DataError('Full journal replay failed; see .validation/replay.log')
    elif not re.fullmatch(r'replay_[0-9a-f]{16}', replay_name):
        raise app.DataError('Expected the name of a completed replay database')
    if (output / 'result.json').read_bytes() != (replay_output / 'result.json').read_bytes():
        raise app.DataError('JSON report changed after rebuilding from the journal')
    lock = app.WorkLock(work)
    pg = app.LocalPostgres(work / 'postgres')
    first = second = restored = None
    try:
        pg.start()
        first = pg.database('pw_' + run_id)
        store = app.Store(first, metadata)
        summary = app.result_summary(store)
        if summary['status'] != 'complete':
            raise app.DataError('The live run is not complete')
        journal = app.Journal(work / 'runs' / run_id / 'journal.jsonl')
        try:
            if journal.replay(store):
                raise app.DataError('The live database still had uncommitted journal records')
        finally: journal.close()
        second = pg.database(replay_name)
        counts = compare_tables(first, second)
        report_rows = verify_report(first, output / 'result.json', metadata)
        report_path = output / 'result.json'
        compressed_report = output / 'result.json.gz'
        with report_path.open('rb') as source, gzip.open(compressed_report, 'wb', compresslevel=6) as target:
            shutil.copyfileobj(source, target)
        with report_path.open('rb') as source, gzip.open(compressed_report, 'rb') as compressed:
            while data := source.read(1024 * 1024):
                if compressed.read(len(data)) != data:
                    raise app.DataError('Compressed JSON differs from the full report')
            if compressed.read(1):
                raise app.DataError('Compressed JSON contains extra data')
        missing_operations = first.execute('''select count(*) from transactions t
            where not exists (select 1 from operations o where o.tx_hash=t.tx_hash)''').fetchone()[0]
        if missing_operations:
            raise app.DataError('Some wallet transactions have no recorded operation')
        error = first.execute('''with ledger as (
            select token_address,token_id,sum(delta_raw) as calculated from movements group by token_address,token_id
        ) select 1 from ledger l full join balances b using(token_address,token_id)
          where coalesce(l.calculated,0)<>b.calculated or b.calculated is null limit 1''').fetchone()
        if error:
            raise app.DataError('Stored balances differ from the movement ledger')
        artifacts = work / 'artifacts' / run_id
        artifacts.mkdir(parents=True, exist_ok=True)
        dump = artifacts / 'wallet.dump'
        common = ['-h', '127.0.0.1', '-p', str(pg.port), '-U', pg.user]
        print('export: PostgreSQL dump', flush=True)
        subprocess.run([pg.binary('pg_dump'), *common, '-d', 'pw_' + run_id, '-Fc', '-Z', '6',
                        '--no-owner', '--no-privileges', '-f', str(dump)], check=True)
        restore_name = 'restore_' + uuid.uuid4().hex[:16]
        restored = pg.database(restore_name)
        print('restore: PostgreSQL dump into an empty database', flush=True)
        subprocess.run([pg.binary('pg_restore'), *common, '-d', restore_name, '--no-owner', '--no-privileges',
                        '--exit-on-error', str(dump)], check=True)
        compare_tables(first, restored)
        raw_count = export_jsonl(first, '''select jsonb_build_object('transactionHash',tx_hash,
            'logIndex',log_index,'blockNumber',block_number,'blockHash',block_hash,'address',address,
            'topics',topics,'data',data) from raw_logs order by tx_hash,log_index''', artifacts / 'raw_logs.jsonl.gz')
        operations_count = export_jsonl(first, '''select jsonb_build_object('tx_hash',tx_hash,'log_index',log_index,
            'item_index',item_index,'block_number',block_number,'timestamp',timestamp,
            'type',operation_type,'token_address',token_address,'token_id',token_id::text,
            'amount_raw',amount_raw::text,'details',details) from wallet_history
            order by block_number,transaction_index,log_index,item_index''', artifacts / 'history.jsonl.gz')
        if raw_count != counts['raw_logs'] or operations_count != counts['operations']:
            raise app.DataError('JSONL exports omit database rows')
        export_samples(first, output)
        app.atomic_json(output / 'scan_state.json', {'run_id': run_id, 'wallet': app.WALLET,
            'snapshot': metadata['snapshot'], 'streams': dict(first.execute('select stream,next_block from scan_progress'))})
        evidence = {'run_id': run_id, 'snapshot': metadata['snapshot'], 'status': 'passed',
                    'regression_tests': test_count, 'live_run': {'started_with_empty_database': True,
                    'used_previous_logs': False, 'all_streams_complete': True, 'all_receipts_complete': True},
                    'verification_sources': metadata['verification_sources'], 'mismatches': 0,
                    'journal_rebuild': 'identical', 'dump_restore': 'identical', 'sql_json': 'identical',
                    'table_counts': counts, 'raw_logs_exported': raw_count, 'operations_exported': operations_count,
                    'json_balances_compared_with_sql': report_rows,
                    'transactions_without_operations': missing_operations,
                    'history_records': {'operation': 'trade or protocol action',
                        'settlement': 'collateral conversion inside a protocol action',
                        'movement': 'token transfer; balances use the movements table only'},
                    'contract_sources': ['https://docs.polymarket.com/resources/contracts',
                        'https://developers.circle.com/stablecoins/usdc-contract-addresses'],
                    'postgresql_version': first.execute('show server_version').fetchone()[0],
                    'journal_replay_command': 'unzip wallet_data.zip -d data && python3 main.py --work-dir data --replay-only',
                    'restore_command': 'createdb pw && pg_restore --no-owner --no-privileges --exit-on-error -d pw postgresql/wallet.dump'}
        abi_evidence = evidence_dir / 'contract_abis.json'
        if abi_evidence.exists():
            audit = json.loads(abi_evidence.read_text())
            if audit['run_id'] == run_id and audit['snapshot'] == metadata['snapshot']:
                evidence['abi_checks'] = audit
        app.atomic_json(output / 'checks.json', evidence)
        archive = artifacts / 'pw.zip'
        with zipfile.ZipFile(archive, 'w', zipfile.ZIP_DEFLATED, compresslevel=6) as package:
            for relative in ['main.py', 'requirements.txt', 'postgresql/schema.sql', 'tests/test_main.py', 'tests/validate_run.py']:
                package.write(app.BASE / relative, relative)
            for name in ['result.json', 'result.txt', 'status.json', 'checks.json', 'scan_state.json', 'logs_first.jsonl', 'logs_last.jsonl']:
                package.write(output / name, 'result/' + name)
            package.write(dump, 'postgresql/wallet.dump', compress_type=zipfile.ZIP_STORED)
        with zipfile.ZipFile(archive) as package:
            if package.testzip() is not None:
                raise app.DataError('Archive integrity check failed')
        data_archive = artifacts / 'wallet_data.zip'
        print('export: complete journal archive', flush=True)
        with zipfile.ZipFile(data_archive, 'w', zipfile.ZIP_DEFLATED, compresslevel=6) as package:
            package.write(work / 'current.json', 'current.json')
            package.write(work / 'runs' / run_id / 'journal.jsonl', f'runs/{run_id}/journal.jsonl')
        with zipfile.ZipFile(data_archive) as package:
            if package.testzip() is not None:
                raise app.DataError('Journal archive integrity check failed')
        release_files = [part for path in [archive, data_archive, artifacts / 'raw_logs.jsonl.gz',
                         artifacts / 'history.jsonl.gz'] for part in release_parts(path)]
        print(json.dumps({'status': 'passed', 'artifacts': str(artifacts),
                         'archive_bytes': archive.stat().st_size, 'data_archive_bytes': data_archive.stat().st_size,
                         'release_files': [{'name': path.name, 'bytes': path.stat().st_size}
                                           for path in release_files]}), flush=True)
        return artifacts
    finally:
        for conn in (first, second, restored):
            if conn: conn.close()
        pg.close()
        lock.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--work-dir', type=Path, default=app.BASE / '.work')
    parser.add_argument('--output-dir', type=Path, default=app.BASE / 'result')
    parser.add_argument('--replay-database', help='continue checks using a completed journal replay')
    args = parser.parse_args()
    try:
        validate(args.work_dir.resolve(), args.output_dir.resolve(), args.replay_database)
    except (Exception, KeyboardInterrupt) as exc:
        metadata_path = args.work_dir / 'current.json'
        metadata = json.loads(metadata_path.read_text()) if metadata_path.exists() else {}
        interrupted = isinstance(exc, KeyboardInterrupt)
        app.atomic_json(args.output_dir / 'checks.json', {'run_id': metadata.get('run_id'),
            'status': 'interrupted' if interrupted else 'failed', 'error': str(exc)})
        print(f'{type(exc).__name__}: {exc}', file=sys.stderr)
        sys.exit(130 if interrupted else 1)
