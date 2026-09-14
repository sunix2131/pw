import atexit
import getpass
import json
import shutil
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path

import psycopg
from psycopg import sql
import requests
from eth_abi import decode, encode
from eth_utils import keccak
from psycopg.types.json import Jsonb

wallet_address = "0x46b353667fd7d846af3bbeda6584b0e5b883d3de".lower()
ctf_contract = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045"

rpc_nodes = [
    ("tenderly", "https://tenderly.rpc.polygon.community"),
    ("allnodes", "https://polygon.publicnode.com"),
    ("nodies", "https://polygon-public.nodies.app/"),
    ("1rpc", "https://1rpc.io/matic"),
    ("onfinality", "https://polygon.api.onfinality.io/public"),
    ("drpc", "https://polygon.drpc.org"),
]

exchange_v1 = {
    "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e",
    "0xc5d563a36ae78145c45a50134d48a1215220f80a",
}
exchange_v2 = {
    "0xe111180000d2663c0091e4f400237545b87b996b",
    "0xe2222d279d744050d28e00520010520000310f59",
}

rpc_url = None
log_chunk_size = 10_000
session = requests.Session()

base_dir = Path(__file__).resolve().parent
result_dir = base_dir / "result"
data_file = result_dir / "ctf_logs.jsonl"
state_file = result_dir / "scan_state.json"
result_file = result_dir / "result.txt"
result_json = result_dir / "result.json"

pg_root = base_dir / ".postgres"
pg_data = pg_root / "data"
pg_log = pg_root / "postgres.log"
pg_port = 55432
db_name = "polymarket_wallet"
pg_ctl_path = None
pg_started = False
balance_ids_per_call = 500
balance_calls_per_batch = 10


def event(signature):
    return "0x" + keccak(text=signature).hex()


transfer_single = event("TransferSingle(address,address,address,uint256,uint256)")
transfer_batch = event("TransferBatch(address,address,address,uint256[],uint256[])")
position_split = event("PositionSplit(address,address,bytes32,bytes32,uint256[],uint256)")
positions_merge = event("PositionsMerge(address,address,bytes32,bytes32,uint256[],uint256)")
payout_redemption = event("PayoutRedemption(address,address,bytes32,bytes32,uint256[],uint256)")
order_filled_v1 = event("OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)")
order_filled_v2 = event("OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,bytes32)")


schema = (base_dir / "postgresql" / "schema.sql").read_text(encoding="utf-8")


def rpc_request(url, method, params, timeout=20, retries=2):
    body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    last_error = None

    for attempt in range(retries):
        try:
            response = session.post(url, json=body, timeout=timeout)
            response.raise_for_status()
            data = response.json()
            if "error" in data:
                error = data["error"]
                message = error.get("message", str(error)) if isinstance(error, dict) else str(error)
                raise RuntimeError(message)
            return data["result"]
        except RuntimeError:
            raise
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            if attempt + 1 < retries:
                time.sleep(0.5)

    raise ConnectionError(f"{method}: {last_error}")


def rpc(method, params):
    while True:
        try:
            return rpc_request(rpc_url, method, params)
        except ConnectionError:
            print("  rpc недоступен ищу другой", flush=True)
            choose_rpc()


def rpc_batch(method, params_list, batch_size=50):
    result = []

    for offset in range(0, len(params_list), batch_size):
        part = params_list[offset:offset + batch_size]
        body = [
            {"jsonrpc": "2.0", "id": i, "method": method, "params": params}
            for i, params in enumerate(part)
        ]

        try:
            response = session.post(rpc_url, json=body, timeout=30)
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, list):
                raise RuntimeError("batch не поддерживается")
            by_id = {item["id"]: item for item in data}
            for i in range(len(part)):
                item = by_id.get(i, {})
                if "error" in item:
                    raise RuntimeError(str(item["error"]))
                result.append(item.get("result"))
        except (requests.RequestException, ValueError, RuntimeError):
            for params in part:
                result.append(rpc(method, params))

    return result


def choose_rpc():
    global rpc_url, log_chunk_size

    test_from = 4_023_686
    wallet_topic = "0x" + "0" * 24 + wallet_address[2:]
    sizes = [250_000, 50_000, 10_000, 2_000, 500, 100]

    while True:
        candidates = []
        print("rpc")

        for name, url in rpc_nodes:
            try:
                latest = int(rpc_request(url, "eth_blockNumber", [], timeout=4, retries=1), 16)
            except (RuntimeError, ConnectionError):
                print(f"  {name}: недоступен")
                continue

            accepted = 0
            latency = 999.0
            for size in sizes:
                flt = {
                    "fromBlock": hex(test_from),
                    "toBlock": hex(min(test_from + size - 1, latest)),
                    "address": ctf_contract,
                    "topics": [[transfer_single, transfer_batch], None, wallet_topic],
                }
                started = time.monotonic()
                try:
                    rpc_request(url, "eth_getLogs", [flt], timeout=5, retries=1)
                except (RuntimeError, ConnectionError):
                    continue
                accepted = size
                latency = time.monotonic() - started
                break

            if accepted:
                print(f"  {name}: {accepted:,} блоков, {latency:.2f}с")
                candidates.append((accepted, -latency, name, url))
            else:
                print(f"  {name}: не подошёл")

        if candidates:
            accepted, _, name, url = max(candidates)
            rpc_url = url
            log_chunk_size = accepted
            print(f"  выбран: {name}, chunk {accepted:,}\n")
            return

        print("  нет доступных rpc жду 15с\n", flush=True)
        time.sleep(15)


def first_contract_block(address, latest):
    left, right = 0, latest
    while left < right:
        mid = (left + right) // 2
        code = rpc("eth_getCode", [address, hex(mid)])
        if code and code != "0x":
            right = mid
        else:
            left = mid + 1
    return left


def save_state(state):
    tmp = state_file.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(state_file)


def load_state():
    if not state_file.exists():
        return None
    return json.loads(state_file.read_text(encoding="utf-8"))


def append_logs(logs, direction):
    if not logs:
        return

    with data_file.open("a", encoding="utf-8") as f:
        for log in logs:
            row = {"direction": direction, "log": log}
            f.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
        f.flush()


def scanned_batch(logs):
    batch = []
    for log in logs:
        batch.append((
            log["transactionHash"].lower(),
            int(log["logIndex"], 16),
            int(log["blockNumber"], 16),
            log["address"].lower(),
            Jsonb(log.get("topics", [])),
            log.get("data", "0x"),
            log,
        ))
    return batch


def scan_logs(conn, topics, start, end, direction, state):
    chunk = log_chunk_size
    key = f"{direction}_next"
    done_key = f"{direction}_done"
    block = max(start, int(state.get(key, start)))

    if state.get(done_key):
        print(f"{direction}: уже сохранено")
        return

    total = end - start + 1
    started = time.monotonic()
    requests_done = 0
    found_total = int(state.get(f"{direction}_count", 0))

    print(direction)

    while block <= end:
        from_block = block
        to_block = min(from_block + chunk - 1, end)
        flt = {
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block),
            "address": ctf_contract,
            "topics": topics,
        }

        try:
            logs = rpc("eth_getLogs", [flt])
        except RuntimeError:
            if chunk == 1:
                raise
            chunk = max(chunk // 2, 1)
            print(f"  rpc chunk {chunk:,}", flush=True)
            continue

        append_logs(logs, direction)
        if logs:
            save_ctf_batch(conn, scanned_batch(logs))
        found_total += len(logs)
        block = to_block + 1
        requests_done += 1

        state[key] = block
        state[f"{direction}_count"] = found_total
        save_state(state)

        percent = (to_block - start + 1) * 100 / total
        elapsed = time.monotonic() - started
        if logs or requests_done == 1 or requests_done % 20 == 0 or to_block == end:
            print(f"  {percent:5.1f}%  {from_block:,}-{to_block:,}  +{len(logs)}  всего {found_total}  {elapsed:,.0f}с", flush=True)

    state[done_key] = True
    save_state(state)
    print(f"  готово {found_total}\n")


def topic_address(value):
    return "0x" + value[-40:].lower()


def decode_log(log):
    if not log.get("topics"):
        return None

    t0 = log["topics"][0].lower()
    address = log["address"].lower()
    data = bytes.fromhex(log["data"][2:])

    if address == ctf_contract and t0 == transfer_single.lower():
        token_id, amount = decode(["uint256", "uint256"], data)
        return {
            "event": "TransferSingle",
            "from": topic_address(log["topics"][2]),
            "to": topic_address(log["topics"][3]),
            "token_id": str(token_id),
            "amount_raw": str(amount),
        }

    if address == ctf_contract and t0 == transfer_batch.lower():
        token_ids, amounts = decode(["uint256[]", "uint256[]"], data)
        return {
            "event": "TransferBatch",
            "from": topic_address(log["topics"][2]),
            "to": topic_address(log["topics"][3]),
            "token_ids": [str(x) for x in token_ids],
            "amounts_raw": [str(x) for x in amounts],
        }

    if address == ctf_contract and t0 == position_split.lower():
        return {"event": "PositionSplit", "wallet": topic_address(log["topics"][1])}
    if address == ctf_contract and t0 == positions_merge.lower():
        return {"event": "PositionsMerge", "wallet": topic_address(log["topics"][1])}
    if address == ctf_contract and t0 == payout_redemption.lower():
        return {"event": "PayoutRedemption", "wallet": topic_address(log["topics"][1])}

    if (address in exchange_v1 and t0 == order_filled_v1.lower()) or (address in exchange_v2 and t0 == order_filled_v2.lower()):
        return {
            "event": "OrderFilled",
            "maker": topic_address(log["topics"][2]),
            "taker": topic_address(log["topics"][3]),
        }

    return None


def movement_rows(log):
    item = decode_log(log)
    if not item or item["event"] not in {"TransferSingle", "TransferBatch"}:
        return []

    if item["event"] == "TransferSingle":
        pairs = [(item["token_id"], item["amount_raw"])]
    else:
        pairs = zip(item["token_ids"], item["amounts_raw"])

    rows = []
    for token_id, amount in pairs:
        delta = 0
        if item["to"] == wallet_address:
            delta += int(amount)
        if item["from"] == wallet_address:
            delta -= int(amount)
        if delta:
            rows.append((int(token_id), delta))
    return rows


def get_movements(logs):
    rows = []
    for log in logs:
        for token_id, delta in movement_rows(log):
            rows.append((int(log["logIndex"], 16), token_id, delta))
    return rows


def get_operations(logs, movements):
    known = [decode_log(log) for log in logs]
    known = [x for x in known if x]

    has_fill = any(
        x["event"] == "OrderFilled" and wallet_address in {x["maker"], x["taker"]}
        for x in known
    )
    if has_fill:
        reason = "order_filled"
    elif any(x["event"] == "PositionSplit" and x["wallet"] == wallet_address for x in known):
        reason = "position_split"
    elif any(x["event"] == "PositionsMerge" and x["wallet"] == wallet_address for x in known):
        reason = "positions_merge"
    elif any(x["event"] == "PayoutRedemption" and x["wallet"] == wallet_address for x in known):
        reason = "payout_redemption"
    else:
        reason = "transfer"

    totals = defaultdict(int)
    for _, token_id, delta in movements:
        totals[token_id] += delta

    result = []
    for token_id, delta in totals.items():
        if reason == "order_filled":
            op = "buy" if delta > 0 else "sell"
        elif reason == "position_split":
            op = "split"
        elif reason == "positions_merge":
            op = "merge"
        elif reason == "payout_redemption":
            op = "redeem"
        else:
            op = "transfer_in" if delta > 0 else "transfer_out"
        result.append((op, token_id, abs(delta), {"reason": reason, "delta_raw": str(delta)}))
    return result


def stop_database():
    if not pg_started or not pg_ctl_path:
        return
    subprocess.run(
        [pg_ctl_path, "-D", str(pg_data), "stop", "-m", "fast"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def start_database():
    global pg_ctl_path, pg_started

    print("postgresql")
    initdb = shutil.which("initdb")
    pg_ctl_path = shutil.which("pg_ctl")
    if not initdb or not pg_ctl_path:
        raise RuntimeError("postgresql не найден в PATH")

    pg_root.mkdir(exist_ok=True)
    if not (pg_data / "PG_VERSION").exists():
        subprocess.run(
            [initdb, "-D", str(pg_data), "-A", "trust", "--encoding=UTF8", "--no-locale"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )

    status = subprocess.run(
        [pg_ctl_path, "-D", str(pg_data), "status"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    if status.returncode != 0:
        subprocess.run(
            [
                pg_ctl_path, "-D", str(pg_data), "-l", str(pg_log),
                "-o", f"-p {pg_port} -h 127.0.0.1", "start",
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        pg_started = True
        atexit.register(stop_database)

    user = getpass.getuser()
    deadline = time.monotonic() + 10
    while True:
        try:
            admin = psycopg.connect(
                host="127.0.0.1", port=pg_port, dbname="postgres", user=user,
                connect_timeout=2, autocommit=True,
            )
            break
        except psycopg.Error:
            if time.monotonic() >= deadline:
                raise RuntimeError("postgresql не запустился")
            time.sleep(0.2)

    with admin:
        row = admin.execute("select 1 from pg_database where datname = %s", (db_name,)).fetchone()
        if not row:
            admin.execute(sql.SQL("create database {}").format(sql.Identifier(db_name)))

    conn = psycopg.connect(
        host="127.0.0.1", port=pg_port, dbname=db_name, user=user, connect_timeout=5
    )
    with conn.cursor() as cur:
        cur.execute(schema)
    conn.commit()
    print(f"  {db_name} :{pg_port}\n")
    return conn


def clear_database(conn):
    with conn.cursor() as cur:
        cur.execute("truncate operations, events, transactions, movements, ctf_logs restart identity")
    conn.commit()


def import_ctf_logs(conn):
    with conn.cursor() as cur:
        cur.execute("select count(*) from ctf_logs")
        existing = cur.fetchone()[0]

    if existing:
        print(f"ctf json уже в базе {existing}")
        return

    if not data_file.exists():
        return

    print("ctf json в postgresql")
    saved = 0
    batch = []

    with data_file.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue

            log = item["log"]
            tx_hash = log["transactionHash"].lower()
            log_index = int(log["logIndex"], 16)
            block = int(log["blockNumber"], 16)
            batch.append((
                tx_hash,
                log_index,
                block,
                log["address"].lower(),
                Jsonb(log.get("topics", [])),
                log.get("data", "0x"),
                log,
            ))

            if len(batch) >= 1000:
                save_ctf_batch(conn, batch)
                saved += len(batch)
                batch.clear()
                if saved % 50_000 == 0:
                    print(f"  {saved:,}", flush=True)

    if batch:
        save_ctf_batch(conn, batch)
        saved += len(batch)

    print(f"  готово {saved:,}\n")


def save_ctf_batch(conn, batch):
    if not batch:
        return

    logs_rows = []
    movement_data = []
    for tx_hash, log_index, block, address, topics, data, log in batch:
        logs_rows.append((tx_hash, log_index, block, address, topics, data))
        for token_id, delta in movement_rows(log):
            movement_data.append((tx_hash, log_index, token_id, delta))

    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into ctf_logs (tx_hash,log_index,block_number,address,topics,data)
            values (%s,%s,%s,%s,%s,%s)
            on conflict do nothing
            """,
            logs_rows,
        )
        if movement_data:
            cur.executemany(
                """
                insert into movements (tx_hash,log_index,token_id,delta_raw)
                values (%s,%s,%s,%s)
                on conflict do nothing
                """,
                movement_data,
            )
    conn.commit()


def balances_onchain(token_ids, block):
    if not token_ids:
        return {}

    selector = keccak(text="balanceOfBatch(address[],uint256[])")[:4]
    result = {}
    chunks = [
        token_ids[offset:offset + balance_ids_per_call]
        for offset in range(0, len(token_ids), balance_ids_per_call)
    ]
    params = []
    for chunk in chunks:
        args = encode(["address[]", "uint256[]"], [[wallet_address] * len(chunk), chunk])
        data = "0x" + (selector + args).hex()
        params.append([{"to": ctf_contract, "data": data}, hex(block)])

    total_calls = len(params)
    for offset in range(0, total_calls, balance_calls_per_batch):
        part = params[offset:offset + balance_calls_per_batch]
        raw_results = rpc_batch("eth_call", part, batch_size=balance_calls_per_batch)
        for chunk, raw in zip(chunks[offset:offset + len(part)], raw_results):
            values = decode(["uint256[]"], bytes.fromhex(raw[2:]))[0]
            result.update(dict(zip(chunk, values)))
        print(f"  {min(offset + len(part), total_calls)}/{total_calls}", flush=True)

    return result


def verify_balances(conn, snapshot):
    with conn.cursor() as cur:
        cur.execute(
            """
            select token_id::text, sum(delta_raw)::text
            from movements
            group by token_id
            having sum(delta_raw) <> 0
            order by token_id
            """
        )
        calculated = {int(token): int(balance) for token, balance in cur.fetchall()}

    print(f"ненулевые балансы {len(calculated)} token_id")
    print("on-chain")
    onchain = balances_onchain(sorted(calculated), snapshot)
    verification = [(token, balance, int(onchain[token]), balance - int(onchain[token])) for token, balance in calculated.items()]
    bad = [row for row in verification if row[3] != 0]

    print("итог")
    print(f"  расхождения {len(bad)}")
    if not bad:
        print("  балансы совпали")
    print()

    return verification, bad


def process_receipts(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            select distinct c.tx_hash
            from ctf_logs c
            left join transactions t on t.tx_hash = c.tx_hash
            where t.tx_hash is null
            order by c.tx_hash
            """
        )
        tx_hashes = [row[0] for row in cur.fetchall()]

    if not tx_hashes:
        print("receipts: готово\n")
        return

    print(f"receipts {len(tx_hashes):,}")
    done = 0
    block_times = {}

    for offset in range(0, len(tx_hashes), 50):
        hashes = tx_hashes[offset:offset + 50]
        receipts = rpc_batch("eth_getTransactionReceipt", [[h] for h in hashes])

        missing_blocks = sorted({
            int(receipt["blockNumber"], 16)
            for receipt in receipts
            if receipt and int(receipt["blockNumber"], 16) not in block_times
        })
        if missing_blocks:
            blocks = rpc_batch("eth_getBlockByNumber", [[hex(block), False] for block in missing_blocks])
            for block_number, block_data in zip(missing_blocks, blocks):
                block_times[block_number] = int(block_data["timestamp"], 16)

        with conn.cursor() as cur:
            for tx_hash, receipt in zip(hashes, receipts):
                if not receipt:
                    continue

                block = int(receipt["blockNumber"], 16)
                timestamp = block_times[block]
                movements = get_movements(receipt["logs"])
                operations = get_operations(receipt["logs"], movements)

                from_address = (receipt.get("from") or wallet_address).lower()
                to_address = receipt.get("to")
                if to_address:
                    to_address = to_address.lower()

                cur.execute(
                    """
                    insert into transactions values (%s,%s,to_timestamp(%s),%s,%s,%s)
                    on conflict do nothing
                    """,
                    (tx_hash, block, timestamp, from_address, to_address, int(receipt["status"], 16)),
                )

                for log in receipt["logs"]:
                    decoded = decode_log(log)
                    cur.execute(
                        """
                        insert into events values (%s,%s,%s,%s,%s,%s)
                        on conflict do nothing
                        """,
                        (
                            tx_hash,
                            int(log["logIndex"], 16),
                            block,
                            log["address"].lower(),
                            log["topics"][0].lower() if log.get("topics") else None,
                            Jsonb(decoded) if decoded else None,
                        ),
                    )

                for op, token_id, amount, details in operations:
                    cur.execute(
                        """
                        insert into operations (tx_hash,block_number,timestamp,operation_type,token_id,amount_raw,details)
                        values (%s,%s,to_timestamp(%s),%s,%s,%s,%s)
                        on conflict do nothing
                        """,
                        (tx_hash, block, timestamp, op, token_id, amount, Jsonb(details)),
                    )

        conn.commit()
        done += len(hashes)
        if done == len(hashes) or done % 1000 == 0 or done >= len(tx_hashes):
            print(f"  {done:,}/{len(tx_hashes):,}", flush=True)

    print("  готово\n")


def write_result(conn, snapshot, verification, bad):
    with conn.cursor() as cur:
        cur.execute("select count(*) from ctf_logs")
        ctf_count = cur.fetchone()[0]
        cur.execute("select count(distinct tx_hash) from ctf_logs")
        ctf_tx_count = cur.fetchone()[0]
        cur.execute("select count(*) from movements")
        movement_count = cur.fetchone()[0]

    lines = [
        f"кошелёк: {wallet_address}",
        f"snapshot блок: {snapshot}",
        f"ctf logs: {ctf_count}",
        f"ctf транзакций: {ctf_tx_count}",
        f"движений: {movement_count}",
        f"ненулевых балансов: {len(verification)}",
    ]

    lines.append("\nрасчёт и on-chain:")
    for token, calc, chain, diff in verification:
        lines.append(f"token={token} calculated={calc} onchain={chain} difference={diff}")

    lines += ["", f"расхождений: {len(bad)}"]
    if not bad:
        lines.append("все рассчитанные ctf балансы совпали с состоянием контракта")

    result_file.write_text("\n".join(lines) + "\n", encoding="utf-8")
    result_json.write_text(
        json.dumps(
            {
                "wallet": wallet_address,
                "snapshot": snapshot,
                "ctf_logs": ctf_count,
                "transactions": ctf_tx_count,
                "movements": movement_count,
                "nonzero_balances": len(verification),
                "verification": [
                    {
                        "token_id": str(token),
                        "calculated": str(calc),
                        "onchain": str(chain),
                        "difference": str(diff),
                    }
                    for token, calc, chain, diff in verification
                ],
                "mismatches": len(bad),
            },
            ensure_ascii=False,
            indent=2,
        ) + "\n",
        encoding="utf-8",
    )


def migrate_old_files():
    old_data = base_dir / "ctf_logs.txt"
    old_state = base_dir / "scan_state.txt"
    if old_data.exists() and not data_file.exists():
        old_data.replace(data_file)
    if old_state.exists() and not state_file.exists():
        old_state.replace(state_file)


def main():
    result_dir.mkdir(exist_ok=True)
    migrate_old_files()
    conn = start_database()

    wallet_topic = "0x" + "0" * 24 + wallet_address[2:]
    choose_rpc()

    import_ctf_logs(conn)
    state = load_state()
    with conn.cursor() as cur:
        cur.execute("select count(*) from ctf_logs")
        database_has_logs = cur.fetchone()[0] > 0
    if state and not database_has_logs and not data_file.exists():
        print("checkpoint без ctf_logs.jsonl и данных в базе пропущен\n")
        state = None
    if state:
        snapshot = int(state["snapshot"])
        start = int(state["start"])
        print("ctf")
        print(f"  snapshot {snapshot:,}")
        print("  продолжаю с json\n")
    else:
        clear_database(conn)
        snapshot = int(rpc("eth_blockNumber", []), 16)
        start = first_contract_block(ctf_contract, snapshot)
        state = {
            "wallet": wallet_address,
            "snapshot": snapshot,
            "start": start,
            "outgoing_next": start,
            "incoming_next": start,
            "outgoing_count": 0,
            "incoming_count": 0,
            "outgoing_done": False,
            "incoming_done": False,
        }
        data_file.write_text("", encoding="utf-8")
        save_state(state)
        print("ctf")
        print(f"  кошелёк {wallet_address}")
        print(f"  snapshot {snapshot:,}")
        print(f"  старт {start:,}\n")

    transfer_events = [transfer_single, transfer_batch]
    scan_logs(conn, [transfer_events, None, wallet_topic], start, snapshot, "outgoing", state)
    scan_logs(conn, [transfer_events, None, None, wallet_topic], start, snapshot, "incoming", state)

    print(f"json {data_file}")
    print(f"  {data_file.stat().st_size / 1024 / 1024:.1f} mb\n")

    verification, bad = verify_balances(conn, snapshot)
    write_result(conn, snapshot, verification, bad)

    if "--receipts" in sys.argv:
        process_receipts(conn)
        write_result(conn, snapshot, verification, bad)

    conn.close()
    print("result.txt")
    print("result.json")


if __name__ == "__main__":
    main()
