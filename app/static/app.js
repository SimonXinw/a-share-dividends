const { createApp, ref, reactive, onMounted, computed, h, defineComponent } = Vue;
const { ElMessage, ElMessageBox } = ElementPlus;
const { Refresh, Edit } = ElementPlusIconsVue;

const API_BASE = "";

const formatNumber = (v, digits = 2) => {
    if (v === null || v === undefined || v === "") return "--";
    const n = Number(v);
    if (Number.isNaN(n)) return "--";
    return n.toLocaleString("zh-CN", {
        minimumFractionDigits: digits,
        maximumFractionDigits: digits,
    });
};

const formatPercent = (v) => {
    if (v === null || v === undefined || v === "") return "--";
    const n = Number(v) * 100;
    if (Number.isNaN(n)) return "--";
    return n.toFixed(2) + " %";
};

const num = (v) => {
    if (v === null || v === undefined || v === "") return -Infinity;
    const n = Number(v);
    return Number.isNaN(n) ? -Infinity : n;
};

// ============================================================================
// 可编辑单元格组件
// ============================================================================
const EditableCell = defineComponent({
    name: "EditableCell",
    props: {
        row: { type: Object, required: true },
        field: { type: String, required: true },
        digits: { type: Number, default: 2 },
        scientific: { type: Boolean, default: false },
        isText: { type: Boolean, default: false },
    },
    emits: ["save"],
    setup(props, { emit }) {
        const editing = ref(false);
        const draft = ref("");

        const display = computed(() => {
            const v = props.row[props.field];
            if (v === null || v === undefined || v === "") return "--";
            if (props.isText) return v;
            const n = Number(v);
            if (Number.isNaN(n)) return "--";
            if (props.scientific && Math.abs(n) >= 1e8) {
                return (n / 1e8).toFixed(2) + " 亿";
            }
            if (props.scientific && Math.abs(n) >= 1e4) {
                return (n / 1e4).toFixed(2) + " 万";
            }
            return n.toLocaleString("zh-CN", {
                minimumFractionDigits: props.digits,
                maximumFractionDigits: props.digits,
            });
        });

        const startEdit = () => {
            const v = props.row[props.field];
            draft.value = v === null || v === undefined ? "" : String(v);
            editing.value = true;
        };

        const cancel = () => {
            editing.value = false;
        };

        const commit = () => {
            const raw = draft.value === "" ? null : (props.isText ? draft.value : Number(draft.value));
            if (!props.isText && raw !== null && Number.isNaN(raw)) {
                ElMessage.error("请输入有效的数字");
                return;
            }
            emit("save", { code: props.row.code, field: props.field, value: raw });
            editing.value = false;
        };

        return { editing, draft, display, startEdit, cancel, commit };
    },
    template: `
        <span v-if="!editing" class="editable_cell" @dblclick="startEdit">
            <span>{{ display }}</span>
            <span class="edit_icon">✎</span>
        </span>
        <el-input
            v-else
            v-model="draft"
            size="small"
            autofocus
            ref="inputRef"
            @blur="commit"
            @keyup.enter="commit"
            @keyup.esc="cancel"
            style="width: 100%"
        />
    `,
});

// ============================================================================
// 主应用
// ============================================================================
const App = {
    components: { EditableCell },
    setup() {
        const rows = ref([]);
        const loading = reactive({ list: false, price: false, fundamental: false, all: false });
        const message = ref("");
        const messageType = ref("success");
        const newStockCode = ref("");

        const showMessage = (msg, type = "success") => {
            message.value = msg;
            messageType.value = type;
            ElMessage({ message: msg, type, duration: 3000 });
        };

        const refresh = async () => {
            loading.list = true;
            try {
                const res = await fetch(`${API_BASE}/api/stocks`);
                const data = await res.json();
                rows.value = (data.items || []).slice();
            } catch (e) {
                showMessage("加载列表失败：" + e.message, "error");
            } finally {
                loading.list = false;
            }
        };

        const recomputeRow = (row) => {
            const price = num(row.price);
            const lyDiv = num(row.last_year_dividend);
            const lyProfit = num(row.last_year_net_profit);
            const tyProfit = num(row.this_year_estimated_profit);

            row.last_year_dividend_yield =
                Number.isFinite(price) && price > 0 && Number.isFinite(lyDiv)
                    ? lyDiv / price
                    : null;

            if (
                Number.isFinite(lyDiv) &&
                Number.isFinite(lyProfit) &&
                lyProfit !== 0 &&
                Number.isFinite(tyProfit)
            ) {
                row.this_year_estimated_dividend = (lyDiv * tyProfit) / lyProfit;
                row.this_year_estimated_yield =
                    Number.isFinite(price) && price > 0
                        ? row.this_year_estimated_dividend / price
                        : null;
            } else {
                row.this_year_estimated_dividend = null;
                row.this_year_estimated_yield = null;
            }
        };

        const sortByEstimatedYieldDesc = () => {
            rows.value.sort((a, b) => {
                const av = a.this_year_estimated_yield;
                const bv = b.this_year_estimated_yield;
                const an = av === null || av === undefined ? -Infinity : Number(av);
                const bn = bv === null || bv === undefined ? -Infinity : Number(bv);
                return bn - an;
            });
        };

        const saveOverride = async ({ code, field, value }) => {
            const row = rows.value.find((r) => r.code === code);
            if (!row) return;

            const oldVal = row[field];
            row[field] = value;
            recomputeRow(row);

            const payload = {
                price: row.price,
                last_year_dividend: row.last_year_dividend,
                last_year_net_profit: row.last_year_net_profit,
                this_year_estimated_profit: row.this_year_estimated_profit,
                note: row.note ?? null,
            };

            try {
                const res = await fetch(`${API_BASE}/api/stocks/${code}/override`, {
                    method: "PUT",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify(payload),
                });
                if (!res.ok) {
                    throw new Error(await res.text());
                }
                const data = await res.json();
                if (data.row) {
                    Object.assign(row, data.row);
                }
                showMessage(`已保存 ${row.name || code} 的 ${field}`);
            } catch (e) {
                row[field] = oldVal;
                recomputeRow(row);
                showMessage("保存失败：" + e.message, "error");
            }
        };

        const resetOverride = async (row) => {
            try {
                await ElMessageBox.confirm(
                    `恢复 ${row.name || row.code} 为原始抓取的值？`,
                    "确认",
                    { type: "warning" }
                );
            } catch {
                return;
            }
            try {
                const res = await fetch(`${API_BASE}/api/stocks/${row.code}/override`, {
                    method: "DELETE",
                });
                const data = await res.json();
                if (data.row) {
                    Object.assign(row, data.row);
                    recomputeRow(row);
                }
                showMessage("已恢复原值");
            } catch (e) {
                showMessage("操作失败：" + e.message, "error");
            }
        };

        const addStock = async () => {
            const code = (newStockCode.value || "").trim();
            if (!/^\d{6}$/.test(code)) {
                showMessage("请输入 6 位股票代码", "warning");
                return;
            }
            try {
                await fetch(`${API_BASE}/api/stocks`, {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ code }),
                });
                newStockCode.value = "";
                showMessage(`已添加 ${code}，正在为它同步数据...`);
                await triggerSync({ job_type: "all", codes: [code] });
                setTimeout(refresh, 4000);
            } catch (e) {
                showMessage("添加失败：" + e.message, "error");
            }
        };

        const removeStock = async (row) => {
            try {
                await ElMessageBox.confirm(
                    `从关注列表移除 ${row.name || row.code}？`,
                    "确认",
                    { type: "warning" }
                );
            } catch {
                return;
            }
            try {
                await fetch(`${API_BASE}/api/stocks/${row.code}`, { method: "DELETE" });
                rows.value = rows.value.filter((r) => r.code !== row.code);
                showMessage("已移除");
            } catch (e) {
                showMessage("移除失败：" + e.message, "error");
            }
        };

        const triggerSync = async (body) => {
            const res = await fetch(`${API_BASE}/api/sync`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(body),
            });
            return res.json();
        };

        const syncPrices = async () => {
            loading.price = true;
            try {
                await triggerSync({ job_type: "price" });
                showMessage("股价同步任务已开始（后台执行），约 10s 后可刷新");
                setTimeout(refresh, 12000);
            } finally {
                loading.price = false;
            }
        };

        const syncFundamentals = async () => {
            loading.fundamental = true;
            try {
                await triggerSync({ job_type: "fundamental" });
                showMessage("分红/利润同步任务已开始，每只股票约耗时 1-3s，请稍后刷新");
            } finally {
                loading.fundamental = false;
            }
        };

        const syncAll = async () => {
            loading.all = true;
            try {
                await triggerSync({ job_type: "all" });
                showMessage("全量同步已开始，请耐心等待并手动刷新");
            } finally {
                loading.all = false;
            }
        };

        const yieldClass = (v) => {
            const n = Number(v);
            if (!Number.isFinite(n)) return "yield_low";
            if (n >= 0.05) return "yield_high";
            if (n >= 0.03) return "yield_mid";
            return "yield_low";
        };

        const onCellDblClick = () => {
            // 提示已迁移到组件内部，此处保留占位
        };

        onMounted(refresh);

        return {
            rows,
            loading,
            message,
            messageType,
            newStockCode,
            Refresh,
            Edit,

            refresh,
            saveOverride,
            resetOverride,
            addStock,
            removeStock,
            syncPrices,
            syncFundamentals,
            syncAll,

            formatNumber,
            formatPercent,
            num,
            yieldClass,
            onCellDblClick,
        };
    },
};

const app = createApp(App);
app.use(ElementPlus);
for (const [name, comp] of Object.entries(ElementPlusIconsVue)) {
    app.component(name, comp);
}
app.component("EditableCell", EditableCell);
app.mount("#app");
