# A 股红利股息预估表

> 一个面向自用、轻量、可编辑的 A 股股息率排行表。  
> 拉取免费数据源 → 落库 Supabase → 表格展示 → 双击单元格直接修改 → 实时重算 → 按今年预估股息率降序。

---

## 1. 功能一览

- 表格列：**股票代码 / 股名 / 行业 / 当前股价 / 数据日期 / 当前市值 / 去年年末市值 / 去年 / 去年每股分红 / 去年股息率 / 去年净利润 / 今年预估净利润 / 今年预估每股分红 / 今年预估股息率 / 备注**
- **降序排序**：默认按"今年预估股息率"降序，所有数值列也都支持点列头排序
- **任意单元格双击编辑**：编辑后立即重新计算「股息率」「预估每股分红」「预估股息率」并保存
- 编辑保存到 `a_share_overrides` 表，**不污染**原始抓取数据；点"恢复原值"可清除覆盖
- **一键同步**当前股价 / 分红明细 / 季度净利润
- 支持新增/移除关注股票

### 今年预估股息率的计算规则

> 这是产品需求里最关键的一段，对应实现见 `app/services/calculator.py`。

```text
今年预估全年净利润
    = 去年全年净利润
    - 去年「今年已发布的对应季度」净利润之和
    + 今年已发布的季度净利润之和

今年预估每股分红
    = 去年每股分红 × ( 今年预估全年净利润 / 去年全年净利润 )

今年预估股息率
    = 今年预估每股分红 / 当前股价
```

举例：贵州茅台 2024 年净利润 800 亿，每股分红 30 元；2025 年 Q1 净利润已发布为 250 亿，2024 年 Q1 净利润为 220 亿。  
则 2025 年预估净利润 = 800 - 220 + 250 = 830 亿；预估每股分红 = 30 × 830/800 ≈ 31.125；按当前股价 1500 元算，预估股息率 ≈ 2.07%。

---

## 2. 技术选型与决策

### 2.1 数据源（免费方案）

| 方案 | 是否免费 | 是否要登录 | 数据完整度 | 备注 |
| --- | --- | --- | --- | --- |
| **AKShare** ✅本项目采用 | 完全免费 | 不需要 | ⭐⭐⭐⭐⭐ | 封装东方财富/新浪/腾讯接口，社区活跃 |
| Tushare 基础版 | 免费 | 需注册拿 token | ⭐⭐⭐⭐ | 高级数据要刷积分 |
| 东方财富 HTTP 接口 | 免费 | 不需要 | ⭐⭐⭐ | 接口不稳定，需自己解析 |

**结论：使用 AKShare**。无需注册、无需登录、不限流（频率注意点别打爆即可）。

实际用到的接口：

- 当前股价/年末价/市值：`ak.stock_zh_a_daily(symbol="600519")`（按单只股票拉取并计算）
- 分红明细：`ak.stock_fhps_detail_em(symbol="600519")`
- 季度利润：`ak.stock_profit_sheet_by_report_em(symbol="SH600519")`

### 2.2 后端语言：Python vs Node

**选择 Python（FastAPI）**，原因：

1. AKShare、pandas、numpy 等金融数据生态远胜 Node
2. 数据清洗、季度利润累计 → 单季计算等用 pandas 写起来非常自然
3. FastAPI + Supabase HTTP API 对这类自用工具已足够，且避免本地直连 PostgreSQL 的网络兼容问题
4. 如果选 Node，需要自己写爬虫去调东方财富接口，工作量增加 5-10 倍

### 2.3 前端

无需复杂构建，直接 **Vue3 + Element Plus（CDN）**，单页面应用，由后端 FastAPI 一并 serve。

### 2.4 数据库

**Supabase（PostgreSQL）**，所有表加前缀 `a_share_`，与现有 `public` schema 中的表完全隔离。

---

## 3. 目录结构

> 项目采用扁平结构：所有 Python 代码、Dockerfile、依赖清单都直接放在根目录，不再有 `backend/` 中间层。

```text
a-share-dividends/
├── app/                         # FastAPI 应用包（Python 模块名就是 app）
│   ├── main.py                  # 入口（uvicorn app.main:app）
│   ├── config.py                # 配置（自动从项目根 .env 读取）
│   ├── database.py              # Supabase HTTP 访问封装（仓储层）
│   ├── schemas.py               # Pydantic 模型
│   ├── routers/
│   │   ├── stocks.py            # 列表/编辑/新增/删除
│   │   └── sync.py              # 同步任务
│   ├── services/
│   │   ├── data_source.py       # AKShare 抓数据
│   │   └── calculator.py        # 股息计算核心
│   └── static/                  # 前端单页（HTML+Vue3+ElementPlus）
│       ├── index.html
│       ├── style.css
│       └── app.js
├── sql/
│   └── init.sql                 # 一次性建表脚本（去 Supabase 跑）
├── requirements.txt             # Python 依赖
├── Dockerfile                   # 容器构建
├── docker-compose.yml
├── .dockerignore
├── .env.example                 # 环境变量模板
├── .env                         # 真实环境变量（已加入 .gitignore）
├── .gitignore
└── README.md
```

---

## 4. 数据库（Supabase）

### 4.1 一次性初始化

打开 Supabase 项目 → 左侧 **SQL Editor** → 新建 query → 把 `sql/init.sql` 全部内容粘进去 → Run。

会创建以下表（全部加 `a_share_` 前缀，**不会和已有的 public 表冲突**）：

| 表名 | 用途 |
| --- | --- |
| `a_share_stocks` | 关注的股票列表（基础信息） |
| `a_share_prices` | 每只股票最新一档价格（覆盖式更新） |
| `a_share_dividends` | 每只股票每年的分红总额 + 当年净利润 |
| `a_share_quarterly_profits` | 每只股票每年每季度的归母净利润 |
| `a_share_overrides` | 用户在表格里手改的覆盖值 |
| `a_share_sync_logs` | 同步任务日志 |
| `a_share_dashboard_view` | 视图：把上面表格 join 好供后端读取 |

会自动给 `updated_at` 字段加触发器，并塞 5 条示例股票（茅台/工行/招行/美的/长江电力），方便启动后立即看到效果。

### 4.2 配置 Supabase HTTP 访问

Supabase Dashboard → **Project Settings → API**，复制：

- `Project URL` -> `.env` 的 `SUPABASE_URL`
- `service_role`（推荐）或 `anon` -> `.env` 的 `SUPABASE_SERVICE_ROLE_KEY` / `SUPABASE_ANON_KEY`

`DATABASE_URL` 仅作为保留配置，不再是应用启动必须项。

---

## 5. 本地启动（不用 Docker）

> `.env` 放在 **项目根目录**（即 `a-share-dividends/.env`）。`app/config.py` 会按 `当前工作目录/.env → 项目根/.env` 的顺序加载，靠后的优先级更高，所以放在根目录最稳，且不论你从哪里启动都能找到。

```powershell
# 1) 配 .env（项目根目录）
copy .env.example .env
# 用编辑器打开 .env，至少填好 SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY（或 SUPABASE_ANON_KEY）

# 2) 在【项目根目录】建虚拟环境
python -m venv .venv
.venv\Scripts\activate         # Windows PowerShell
# source .venv/bin/activate    # macOS / Linux

# 3) 装依赖
pip install -r requirements.txt
# 国内网络慢可加清华源：
# pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

# 4) 启动（在项目根目录执行；模块名就是 app.main）
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

打开浏览器访问 <http://localhost:8000>，第一次进去表格是空的，点右上角 **「一键同步全部」**，等几十秒到几分钟（取决于股票数量）后刷新就能看到数据。

> **快速验证 .env 被读到了**：启动时如果 `SUPABASE_URL` 或 key 未配置，会立即报错；如果正常打印 `Supabase HTTP 客户端已就绪`，说明 .env 已正确加载。

---

## 6. Docker 部署

### 6.1 单容器（推荐自用）

> `.env` 放在项目根目录即可。`docker-compose.yml` 通过 `env_file: .env` 把全部变量一次性注入容器，**不会**把 `.env` 文件 COPY 进镜像（`.dockerignore` 已排除），不会泄露密码。

```bash
# 1) 配 .env（项目根目录）
copy .env.example .env     # Windows
# cp .env.example .env     # macOS/Linux

# 编辑 .env，填好 SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY（或 SUPABASE_ANON_KEY）

# 2) 启动
docker compose up -d --build

# 3) 查看日志
docker compose logs -f api

# 4) 停止
docker compose down
```

启动后访问 <http://localhost:8000>。

### 6.2 部署到 Render（Docker）

1. 新建 **Web Service**，选择本仓库，Environment 选 **Docker**。  
1. 端口不用手填，镜像已支持 `PORT` 环境变量（Render 会自动注入）。  
1. 在 Render 环境变量里至少配置：

- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`（或 `SUPABASE_ANON_KEY`，二选一）
- `SYNC_CONCURRENCY=5`（建议）

1. 部署后先验证：

- `GET /api/health` 返回 `{"ok":true}`
- 打开根路径 `/` 能加载表格页面

> 注意：应用在启动阶段会初始化 Supabase 客户端。如果 `SUPABASE_URL` 或 key 缺失，Render 会启动失败并反复重启，这是预期保护行为。

### 6.3 直接 docker run（不用 compose）

```bash
# 在项目根目录执行（构建上下文就是当前目录）
docker build -t a-share-dividends .

docker run -d --name a-share-dividends \
    -p 8000:8000 \
    --env-file .env \
    --restart unless-stopped \
    a-share-dividends
```

---

## 7. API 文档

启动后自动生成 Swagger：<http://localhost:8000/docs>

主要端点：

| Method | Path | 说明 |
| --- | --- | --- |
| GET | `/api/stocks` | 取表格数据（已按今年预估股息率降序） |
| POST | `/api/stocks` | 新增股票 `{"code":"600519"}` |
| DELETE | `/api/stocks/{code}` | 软删除（is_active=false） |
| PUT | `/api/stocks/{code}/override` | 保存编辑后的字段 |
| DELETE | `/api/stocks/{code}/override` | 清除编辑覆盖，恢复原值 |
| POST | `/api/sync` | 后台触发同步：`{"job_type":"price\|fundamental\|all","codes":["600519"]}` |
| POST | `/api/sync/blocking` | 阻塞同步（一次性脚本用） |
| GET | `/api/sync/logs?limit=20` | 同步日志 |
| GET | `/api/health` | 健康检查 |

---

## 8. 常见问题

**Q1：为什么我点了同步，过了一会表格还是空？**  
A：同步是后台任务，akshare 接口不算特别快，每只股票拉分红+季度利润大约 1-3 秒；首次跑 30 只股票要等 1 分钟左右。可以打开 `/api/sync/logs` 看进度。

**Q2：今年还没出 Q1 财报怎么办？**  
A：当 `a_share_quarterly_profits` 里没有今年数据时，「今年预估全年净利润」就直接等于「去年全年净利润」（即假设业绩持平）。等 Q1 财报出来后再点同步即可自动重算。

**Q3：我手改的数据会不会被同步覆盖？**  
A：**不会**。同步只写 `a_share_prices` / `a_share_dividends` / `a_share_quarterly_profits` 三张表；你的修改保存在 `a_share_overrides`，计算时 override 优先。点"恢复原值"会从 override 里删掉那一行，让原始数据重新生效。

**Q4：能否换成 Tushare/万得？**  
A：可以。把 `app/services/data_source.py` 替换为对应实现即可，其它模块不受影响。

**Q5：怎么扩展新增字段？**  
A：① 在 `sql/init.sql` 加列 → ② 在 `schemas.py` / `calculator.py` 的 `CalculationContext` 加字段 → ③ 在 `static/index.html` 加 column。

---

## 9. License

仅供个人学习/自用，数据归原数据源所有。

xw 牛逼
