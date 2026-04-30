-- 为股票表增加“价格同步日期/基本面同步日期”，并更新仪表盘视图
-- 在 Supabase SQL Editor 执行

alter table public.a_share_stocks
    add column if not exists price_sync_date date,
    add column if not exists fundamental_sync_date date;

alter table public.a_share_prices
    add column if not exists current_market_cap numeric(22, 2),
    add column if not exists last_year_end_price numeric(14, 4),
    add column if not exists last_year_end_market_cap numeric(22, 2),
    add column if not exists last_year_end_date date;

create or replace view public.a_share_dashboard_view as
with last_year as (
    select code, max(year) as ly
    from public.a_share_dividends
    group by code
),
ly_div as (
    select d.code, d.year as last_year, d.dividend_per_share as last_year_div, d.net_profit as last_year_profit, d.payout_ratio
    from public.a_share_dividends d
    join last_year l on d.code = l.code and d.year = l.ly
)
select
    s.code,
    s.name,
    s.industry,
    coalesce(o.price, p.price) as price,
    coalesce(o.last_year_dividend, ly_div.last_year_div) as last_year_dividend,
    coalesce(o.last_year_net_profit, ly_div.last_year_profit) as last_year_net_profit,
    ly_div.last_year as last_year,
    ly_div.payout_ratio,
    o.this_year_estimated_profit as override_this_year_profit,
    o.note,
    p.price_date,
    s.price_sync_date,
    s.fundamental_sync_date,
    greatest(s.price_sync_date, s.fundamental_sync_date) as sync_date,
    p.current_market_cap,
    p.last_year_end_market_cap,
    p.last_year_end_price,
    p.last_year_end_date
from public.a_share_stocks s
left join public.a_share_prices p on p.code = s.code
left join ly_div on ly_div.code = s.code
left join public.a_share_overrides o on o.code = s.code
where s.is_active = true;
