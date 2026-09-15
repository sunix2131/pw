create table if not exists run (
    id boolean primary key default true check (id),
    metadata jsonb not null,
    journal_offset bigint not null default 0,
    journal_crc32 bigint not null default 0,
    status text not null default 'running'
);
create table if not exists scan_progress (stream text primary key, next_block bigint not null);
create table if not exists blocks (number bigint primary key, hash text not null, timestamp bigint not null);
create table if not exists transactions (
    tx_hash text primary key, block_number bigint not null, block_hash text not null,
    transaction_index integer not null, from_address text, to_address text, status smallint,
    receipt_done boolean not null default false
);
create index if not exists pending_receipts on transactions (block_number, transaction_index) where not receipt_done;
create table if not exists raw_logs (
    tx_hash text not null references transactions, log_index integer not null,
    block_number bigint not null, block_hash text not null, address text not null,
    topics jsonb not null, data text not null, scanned boolean not null,
    primary key (tx_hash, log_index)
);
create table if not exists movements (
    tx_hash text not null, log_index integer not null, item_index integer not null,
    token_address text not null, token_id numeric(78,0) not null check (token_id >= -1),
    from_address text not null, to_address text not null,
    amount_raw numeric(78,0) not null check (amount_raw >= 0), delta_raw numeric(78,0) not null,
    primary key (tx_hash, log_index, item_index),
    foreign key (tx_hash, log_index) references raw_logs
);
create index if not exists movements_asset on movements (token_address, token_id);
create table if not exists operations (
    tx_hash text not null, log_index integer not null, item_index integer not null default 0,
    operation_type text not null, token_address text, token_id numeric(78,0),
    amount_raw numeric(78,0), details jsonb not null,
    primary key (tx_hash, log_index, item_index),
    foreign key (tx_hash, log_index) references raw_logs
);
create table if not exists balances (
    token_address text not null, token_id numeric(78,0) not null, calculated numeric(78,0) not null,
    primary key (token_address, token_id)
);
create table if not exists verification (
    token_address text not null, token_id numeric(78,0) not null,
    source text not null, onchain numeric(78,0) not null,
    primary key (token_address, token_id, source),
    foreign key (token_address, token_id) references balances
);
create or replace view balance_report as
select b.token_address, b.token_id, b.calculated, v.source, v.onchain, b.calculated - v.onchain as difference
from balances b left join verification v using (token_address, token_id);
create or replace view wallet_history as
select t.block_number, t.transaction_index, b.timestamp, o.tx_hash, o.log_index, o.item_index,
       o.operation_type, o.token_address, o.token_id, o.amount_raw, o.details
from operations o join transactions t using (tx_hash) join blocks b on b.number = t.block_number;
create table if not exists source_checks (source text primary key, block_hash text not null);
