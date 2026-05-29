-- ============================================================================
-- ETF 扩展迁移（可扩展方案）
-- 目标：在现有库内新增 ETF 相关表，保持 a_share_ 前缀并兼容既有股票模块
-- ============================================================================

-- 1) ETF 基础信息
create table if not exists public.a_share_etf_instruments (
    code            varchar(16) primary key,                  -- ETF 代码，如 515180
    name            varchar(128) not null,                    -- ETF 名称
    provider        varchar(64)  not null default '易方达',    -- 发行商
    tracking_index  varchar(128),                             -- 跟踪指数
    market          varchar(16)  not null default 'SH',       -- SH / SZ
    is_active       boolean      not null default true,
    price_sync_date date,
    history_sync_date date,
    created_at      timestamptz  not null default now(),
    updated_at      timestamptz  not null default now()
);

create index if not exists idx_a_share_etf_instruments_active
    on public.a_share_etf_instruments (is_active);

-- 2) ETF 最新价格（覆盖式）
create table if not exists public.a_share_etf_prices (
    code         varchar(16) primary key references public.a_share_etf_instruments(code) on delete cascade,
    latest_price numeric(14, 4) not null,
    price_date   date not null default current_date,
    updated_at   timestamptz not null default now()
);

-- 3) ETF 日线历史（回测预留）
create table if not exists public.a_share_etf_price_history (
    id           bigserial primary key,
    code         varchar(16) not null references public.a_share_etf_instruments(code) on delete cascade,
    trade_date   date not null,
    open_price   numeric(14, 4),
    high_price   numeric(14, 4),
    low_price    numeric(14, 4),
    close_price  numeric(14, 4) not null,
    volume       numeric(22, 2),
    amount       numeric(22, 2),
    source       varchar(32) default 'akshare',
    updated_at   timestamptz not null default now(),
    unique (code, trade_date)
);

create index if not exists idx_a_share_etf_price_history_code_date
    on public.a_share_etf_price_history (code, trade_date desc);

-- 4) ETF 事件（分红/拆分，回测净值口径预留）
create table if not exists public.a_share_etf_distributions (
    id                bigserial primary key,
    code              varchar(16) not null references public.a_share_etf_instruments(code) on delete cascade,
    event_date        date not null,
    event_type        varchar(32) not null default 'cash_dividend', -- cash_dividend / split / reverse_split
    cash_per_share    numeric(14, 6),
    split_ratio       numeric(14, 6),
    source            varchar(32) default 'manual',
    updated_at        timestamptz not null default now(),
    unique (code, event_date, event_type)
);

create index if not exists idx_a_share_etf_distributions_code_date
    on public.a_share_etf_distributions (code, event_date desc);

-- 5) ETF 同步日志
create table if not exists public.a_share_etf_sync_logs (
    id            bigserial primary key,
    job_type      varchar(32) not null,   -- price / history / distribution / all
    status        varchar(16) not null,   -- running / success / failed
    affected_rows int default 0,
    message       text,
    started_at    timestamptz not null default now(),
    finished_at   timestamptz
);

create index if not exists idx_a_share_etf_sync_logs_started
    on public.a_share_etf_sync_logs (started_at desc);

-- 6) updated_at 触发器（复用已有函数）
do $$
declare
    t text;
begin
    for t in select unnest(array[
        'a_share_etf_instruments',
        'a_share_etf_prices',
        'a_share_etf_price_history',
        'a_share_etf_distributions'
    ])
    loop
        execute format('drop trigger if exists trg_%I_updated_at on public.%I', t, t);
        execute format(
            'create trigger trg_%I_updated_at before update on public.%I
             for each row execute function public.a_share_set_updated_at()',
             t, t
        );
    end loop;
end $$;

-- 7) ETF 看板视图
create or replace view public.a_share_etf_dashboard_view as
select
    i.code,
    i.name,
    i.provider,
    i.tracking_index,
    i.market,
    p.latest_price,
    p.price_date,
    i.price_sync_date,
    i.history_sync_date,
    greatest(i.updated_at, p.updated_at) as updated_at
from public.a_share_etf_instruments i
left join public.a_share_etf_prices p on p.code = i.code
where i.is_active = true;

alter table public.a_share_etf_instruments
    add column if not exists history_sync_date date;

-- 8) 初始数据（易方达）
insert into public.a_share_etf_instruments (code, name, provider, tracking_index, market)
values
    ('515180', '易方达中证红利ETF', '易方达', '中证红利', 'SH')
on conflict (code) do update
set
    name = excluded.name,
    provider = excluded.provider,
    tracking_index = excluded.tracking_index,
    market = excluded.market,
    is_active = true;
