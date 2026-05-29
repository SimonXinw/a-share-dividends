-- ============================================================================
-- ETF 回测快照表
-- 目标：支持离线策略落库，页面按 code 直接读取最新策略结果
-- ============================================================================

create table if not exists public.a_share_etf_backtest_snapshots (
    code                 varchar(16) primary key references public.a_share_etf_instruments(code) on delete cascade,
    strategy_key         varchar(128),
    ma_window            int,
    latest_zone          varchar(32),
    latest_deviation_pct numeric(12, 4),
    item_count           int,
    payload              jsonb not null,
    generated_at         timestamptz not null default now(),
    updated_at           timestamptz not null default now()
);

create index if not exists idx_a_share_etf_backtest_snapshots_generated
    on public.a_share_etf_backtest_snapshots (generated_at desc);

drop trigger if exists trg_a_share_etf_backtest_snapshots_updated_at on public.a_share_etf_backtest_snapshots;
create trigger trg_a_share_etf_backtest_snapshots_updated_at
before update on public.a_share_etf_backtest_snapshots
for each row execute function public.a_share_set_updated_at();
