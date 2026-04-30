-- ============================================================================
-- A 股红利股息项目 - 数据库初始化脚本（Supabase / PostgreSQL）
-- 所有表统一前缀 a_share_，避免与现有 public schema 中的表冲突
-- ============================================================================

-- 1. 股票基础信息表 ----------------------------------------------------------
create table if not exists public.a_share_stocks (
    code           varchar(10)  primary key,                 -- 股票代码 6 位，如 600519
    name           varchar(64)  not null,                    -- 股票名称
    industry       varchar(64),                              -- 所属行业
    market         varchar(16),                              -- SH / SZ / BJ
    is_active      boolean      not null default true,       -- 是否启用（在表格中展示）
    created_at     timestamptz  not null default now(),
    updated_at     timestamptz  not null default now()
);

create index if not exists idx_a_share_stocks_active on public.a_share_stocks (is_active);

alter table public.a_share_stocks
    add column if not exists price_sync_date date,
    add column if not exists fundamental_sync_date date;


-- 2. 股票当前价格表（最新一条覆盖式更新） -----------------------------------
create table if not exists public.a_share_prices (
    code           varchar(10)  primary key references public.a_share_stocks(code) on delete cascade,
    price          numeric(14, 4) not null,                  -- 当前股价
    price_date     date         not null default current_date,
    current_market_cap numeric(22, 2),                       -- 当前市值（元）
    last_year_end_price numeric(14, 4),                      -- 去年年末最后交易日收盘价
    last_year_end_market_cap numeric(22, 2),                 -- 去年年末最后交易日市值（元）
    last_year_end_date date,                                 -- 去年年末最后交易日
    updated_at     timestamptz  not null default now()
);


-- 3. 年度分红表 -------------------------------------------------------------
-- 用于存放每只股票每个会计年度的分红总额（每股派现 × 总股本，或者直接每股派现）
-- 这里我们存"每股分红总额"（元/股），方便直接除以股价得到股息率
create table if not exists public.a_share_dividends (
    id             bigserial primary key,
    code           varchar(10)  not null references public.a_share_stocks(code) on delete cascade,
    year           int          not null,                    -- 会计年度，如 2024
    dividend_per_share numeric(14, 6) not null default 0,    -- 当年累计每股分红（元/股）
    net_profit     numeric(20, 2),                           -- 当年归母净利润（元）
    payout_ratio   numeric(8, 6),                            -- 分红比例 = 分红总额 / 净利润
    source         varchar(32),                              -- 数据来源：akshare / manual
    updated_at     timestamptz  not null default now(),
    unique (code, year)
);

create index if not exists idx_a_share_dividends_code_year on public.a_share_dividends (code, year desc);


-- 4. 季度净利润表（用于推算今年预估利润） -----------------------------------
create table if not exists public.a_share_quarterly_profits (
    id             bigserial primary key,
    code           varchar(10)  not null references public.a_share_stocks(code) on delete cascade,
    year           int          not null,                    -- 报告年份
    quarter        int          not null check (quarter between 1 and 4),
    net_profit     numeric(20, 2) not null,                  -- 单季度归母净利润（元）
    is_published   boolean      not null default true,       -- 是否已披露
    source         varchar(32),                              -- akshare / manual
    updated_at     timestamptz  not null default now(),
    unique (code, year, quarter)
);

create index if not exists idx_a_share_q_profit_code on public.a_share_quarterly_profits (code, year desc, quarter);


-- 5. 用户编辑覆盖表 ----------------------------------------------------------
-- 前端表格修改后保存到这里，这样不会污染从数据源拉到的原始数据
-- 计算时，覆盖表里有值则优先取覆盖表的值
create table if not exists public.a_share_overrides (
    code           varchar(10)  primary key references public.a_share_stocks(code) on delete cascade,
    price          numeric(14, 4),                           -- 覆盖：当前股价
    last_year_dividend numeric(14, 6),                       -- 覆盖：去年每股分红
    last_year_net_profit numeric(20, 2),                     -- 覆盖：去年净利润
    this_year_estimated_profit numeric(20, 2),               -- 覆盖：今年预估净利润
    note           text,                                     -- 备注
    updated_at     timestamptz  not null default now()
);


-- 6. 同步任务日志表 ----------------------------------------------------------
create table if not exists public.a_share_sync_logs (
    id             bigserial primary key,
    job_type       varchar(32) not null,                     -- price / dividend / profit / all
    status         varchar(16) not null,                     -- success / failed / running
    affected_rows  int default 0,
    message        text,
    started_at     timestamptz not null default now(),
    finished_at    timestamptz
);

create index if not exists idx_a_share_sync_logs_started on public.a_share_sync_logs (started_at desc);


-- 7. 触发器：自动维护 updated_at -------------------------------------------
create or replace function public.a_share_set_updated_at()
returns trigger language plpgsql as $$
begin
    new.updated_at = now();
    return new;
end;
$$;

do $$
declare
    t text;
begin
    for t in select unnest(array[
        'a_share_stocks',
        'a_share_prices',
        'a_share_dividends',
        'a_share_quarterly_profits',
        'a_share_overrides'
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


-- 8. 视图：表格直接展示用 ---------------------------------------------------
-- 把所有计算所需要的字段在数据库层先聚合一次，后端取出后再做"今年预估"
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


-- 9. 示例数据：经典 A 股红利股（按板块整理） -------------------------------
-- 真实运行时通过 /api/sync 拉数据；这里预置一批"长期连续分红、稳定经营、公共事业型"的标的
-- 已包含：六大行 + 股份行 + 城商行、五大险企、三桶油 + 神华 + 陕煤 + 中煤、
--          长江电力 + 五大电力、高速公路龙头、铁路港口、运营商、白酒/家电/食品饮料、基建央企等
insert into public.a_share_stocks (code, name, industry, market) values
    -- ---------------- 银行 ----------------
    ('601398', '工商银行', '银行', 'SH'),
    ('601288', '农业银行', '银行', 'SH'),
    ('601988', '中国银行', '银行', 'SH'),
    ('601939', '建设银行', '银行', 'SH'),
    ('601328', '交通银行', '银行', 'SH'),
    ('601658', '邮储银行', '银行', 'SH'),
    ('600036', '招商银行', '银行', 'SH'),
    ('601166', '兴业银行', '银行', 'SH'),
    ('600000', '浦发银行', '银行', 'SH'),
    ('601998', '中信银行', '银行', 'SH'),
    ('600919', '江苏银行', '银行', 'SH'),
    ('601009', '南京银行', '银行', 'SH'),
    ('002142', '宁波银行', '银行', 'SZ'),
    -- ---------------- 保险 ----------------
    ('601318', '中国平安', '保险', 'SH'),
    ('601628', '中国人寿', '保险', 'SH'),
    ('601601', '中国太保', '保险', 'SH'),
    ('601319', '中国人保', '保险', 'SH'),
    ('601336', '新华保险', '保险', 'SH'),
    -- ---------------- 煤炭（高股息） ----------------
    ('601088', '中国神华', '煤炭', 'SH'),
    ('601225', '陕西煤业', '煤炭', 'SH'),
    ('600188', '兖矿能源', '煤炭', 'SH'),
    ('601898', '中煤能源', '煤炭', 'SH'),
    ('601699', '潞安环能', '煤炭', 'SH'),
    ('601666', '平煤股份', '煤炭', 'SH'),
    -- ---------------- 石油石化 ----------------
    ('601857', '中国石油', '石油石化', 'SH'),
    ('600028', '中国石化', '石油石化', 'SH'),
    ('600938', '中国海油', '石油石化', 'SH'),
    -- ---------------- 电力（公用事业） ----------------
    ('600900', '长江电力', '电力', 'SH'),
    ('600025', '华能水电', '电力', 'SH'),
    ('600886', '国投电力', '电力', 'SH'),
    ('600674', '川投能源', '电力', 'SH'),
    ('600011', '华能国际', '电力', 'SH'),
    ('600027', '华电国际', '电力', 'SH'),
    ('600642', '申能股份', '电力', 'SH'),
    ('601985', '中国核电', '电力', 'SH'),
    ('600483', '福能股份', '电力', 'SH'),
    ('600236', '桂冠电力', '电力', 'SH'),
    ('000543', '皖能电力', '电力', 'SZ'),
    -- ---------------- 高速公路 ----------------
    ('600377', '宁沪高速', '公路', 'SH'),
    ('000429', '粤高速A', '公路', 'SZ'),
    ('600350', '山东高速', '公路', 'SH'),
    ('600012', '皖通高速', '公路', 'SH'),
    ('001965', '招商公路', '公路', 'SZ'),
    ('601107', '四川成渝', '公路', 'SH'),
    ('600548', '深高速',   '公路', 'SH'),
    -- ---------------- 铁路 / 港口 / 航运 ----------------
    ('601006', '大秦铁路', '铁路', 'SH'),
    ('601816', '京沪高铁', '铁路', 'SH'),
    ('600018', '上港集团', '港口', 'SH'),
    ('601018', '宁波港',   '港口', 'SH'),
    ('601872', '招商轮船', '航运', 'SH'),
    -- ---------------- 通信运营商 ----------------
    ('600941', '中国移动', '通信', 'SH'),
    ('601728', '中国电信', '通信', 'SH'),
    ('600050', '中国联通', '通信', 'SH'),
    -- ---------------- 消费 / 食品饮料 ----------------
    ('600519', '贵州茅台', '白酒', 'SH'),
    ('000858', '五粮液',   '白酒', 'SZ'),
    ('002304', '洋河股份', '白酒', 'SZ'),
    ('600887', '伊利股份', '食品饮料', 'SH'),
    ('000895', '双汇发展', '食品饮料', 'SZ'),
    ('600600', '青岛啤酒', '食品饮料', 'SH'),
    -- ---------------- 家电 ----------------
    ('000333', '美的集团', '家电', 'SZ'),
    ('000651', '格力电器', '家电', 'SZ'),
    ('600690', '海尔智家', '家电', 'SH'),
    -- ---------------- 农林牧渔 ----------------
    ('002714', '牧原股份', '农林牧渔', 'SZ'),
    -- ---------------- 金融租赁 ----------------
    ('600901', '江苏金租', '金融租赁', 'SH'),
    -- ---------------- 基建央企 ----------------
    ('601668', '中国建筑', '建筑', 'SH'),
    ('601186', '中国铁建', '建筑', 'SH'),
    ('601390', '中国中铁', '建筑', 'SH'),
    ('601800', '中国交建', '建筑', 'SH'),
    -- ---------------- 钢铁 / 有色 ----------------
    ('600019', '宝钢股份', '钢铁', 'SH'),
    ('601899', '紫金矿业', '有色金属', 'SH'),
    -- ---------------- 商贸零售 ----------------
    ('601888', '中国中免', '商贸零售', 'SH')
on conflict (code) do nothing;
