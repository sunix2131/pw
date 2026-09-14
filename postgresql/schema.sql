create table if not exists ctf_logs (
    tx_hash text not null,
    log_index integer not null,
    block_number bigint not null,
    address text not null,
    topics jsonb not null,
    data text not null,
    primary key (tx_hash, log_index)
);

create table if not exists transactions (
    tx_hash text primary key,
    block_number bigint not null,
    timestamp timestamptz not null,
    from_address text not null,
    to_address text,
    status smallint not null
);

create table if not exists events (
    tx_hash text not null,
    log_index integer not null,
    block_number bigint not null,
    address text not null,
    topic0 text,
    decoded jsonb,
    primary key (tx_hash, log_index)
);

create table if not exists movements (
    tx_hash text not null,
    log_index integer not null,
    token_id numeric(78,0) not null,
    delta_raw numeric(78,0) not null,
    primary key (tx_hash, log_index, token_id)
);

create table if not exists operations (
    id bigserial primary key,
    tx_hash text not null,
    block_number bigint not null,
    timestamp timestamptz not null,
    operation_type text not null,
    token_id numeric(78,0) not null,
    amount_raw numeric(78,0) not null,
    details jsonb not null,
    unique (tx_hash, operation_type, token_id, amount_raw)
);
