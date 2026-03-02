"""
地方銀行財務データダッシュボード
Streamlit + Plotly
"""
from __future__ import annotations

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from scipy import stats
from etl import run_etl

# ─── Page Config ───────────────────────────────────────────
st.set_page_config(
    page_title="地方銀行 財務データダッシュボード",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Custom CSS ────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Noto+Sans+JP:wght@300;400;500;700&display=swap');
html, body, [class*="css"] {
    font-family: 'Noto Sans JP', sans-serif;
}
.metric-card {
    background: #FFFFFF;
    border: 1px solid #e1e4e8;
    border-radius: 12px;
    padding: 1.2rem;
    text-align: center;
    transition: transform 0.2s ease, box-shadow 0.2s ease;
    box-shadow: 0 2px 4px rgba(0,0,0,0.02);
}
.metric-card:hover {
    transform: translateY(-2px);
    border-color: #1a3a5c;
    box-shadow: 0 4px 8px rgba(0,0,0,0.08);
}
.metric-value {
    font-size: 1.8rem;
    font-weight: 700;
    color: #1a3a5c;
}
.metric-label {
    font-size: 0.85rem;
    color: #666666;
    margin-top: 0.3rem;
}
.metric-delta-up {
    color: #2e7d32;
    font-size: 0.9rem;
}
.metric-delta-down {
    color: #d32f2f;
    font-size: 0.9rem;
}
.section-title {
    font-size: 1.1rem;
    font-weight: 700;
    color: #333333;
    border-bottom: 2px solid #1a3a5c;
    padding-bottom: 0.3rem;
    margin-bottom: 1rem;
}
section[data-testid="stSidebar"] {
    width: 320px !important;
    min-width: 320px !important;
    max-width: 320px !important;
}
.stButton>button {
    border-radius: 8px;
    font-size: 0.9rem;
    padding: 0.4rem 0.8rem;
}
</style>
""", unsafe_allow_html=True)


# ─── Data Loading ──────────────────────────────────────────
@st.cache_data(show_spinner="データ読み込み中...")
def load_data():
    banks_df, items_df, values_df = run_etl()
    return banks_df, items_df, values_df


banks_df, items_df, values_df = load_data()

# ─── Helpers ───────────────────────────────────────────────
FISCAL_YEARS = sorted(values_df["fiscal_year"].unique(), reverse=True)

def fy_label(fy: str) -> str:
    y = fy[:4]
    m = fy[4:]
    return f"{y}年{m}月期"

def format_value(v: float | None, unit: str) -> str:
    if pd.isna(v) or v is None:
        return "-"
    try:
        if unit in ("％", "%", "倍", "円", "年", "月"):
            return f"{v:.1f}"
        else:
            return f"{v:,.0f}"
    except:
        return str(v)

BANK_NAMES = sorted(banks_df[banks_df["bank_name"] != "地銀合計"]["bank_name"].unique())
_palette = ["#4e79a7","#f28e2b","#e15759","#76b7b2","#59a14f","#edc948","#b07aa1","#ff9da7","#9c755f","#bab0ac"]
BANK_COLORS = {name: _palette[i % len(_palette)] for i, name in enumerate(BANK_NAMES)}
BANK_COLORS["地銀合計"] = "#333333"

INDIVIDUAL_BANKS = banks_df[banks_df["bank_name"] != "地銀合計"].copy()
agg_match = banks_df[banks_df["bank_name"] == "地銀合計"]["bank_code"]
AGGREGATE_CODE = agg_match.values[0] if len(agg_match) > 0 else None

def get_bank_name(code: str) -> str:
    r = banks_df[banks_df["bank_code"] == code]
    return r.iloc[0]["bank_name"] if len(r) > 0 else code

def get_item_name(item_id: str) -> str:
    if not item_id: return ""
    r = items_df[items_df["item_id"] == item_id]
    return r.iloc[0]["item_name"] if len(r) > 0 else item_id

def get_unit(item_id: str) -> str:
    if not item_id: return ""
    r = items_df[items_df["item_id"] == item_id]
    return r.iloc[0]["unit"] if len(r) > 0 else ""

def get_ranking_data(fiscal_year: str, item_id: str, ascending: bool = False) -> pd.DataFrame:
    mask = (values_df["fiscal_year"] == fiscal_year) & (values_df["item_id"] == item_id)
    if AGGREGATE_CODE:
        mask &= (values_df["bank_code"] != AGGREGATE_CODE)
    df = values_df[mask].merge(banks_df, on="bank_code")
    df = df.sort_values("value", ascending=ascending).reset_index(drop=True)
    df["rank"] = range(1, len(df) + 1)
    return df

# ─── Item list helpers ─────────────────────────────────────
item_list = items_df[["item_id", "item_name", "category_large", "category_mid", "unit", "consolidation"]].drop_duplicates()
item_list["display"] = item_list.apply(
    lambda r: f"{r['item_name']} [{r['consolidation']}] ({r['unit']})" if r['consolidation'] else f"{r['item_name']} ({r['unit']})",
    axis=1
)
ITEM_LABELS = item_list["display"].tolist()
LABEL_TO_ID = dict(zip(item_list["display"], item_list["item_id"]))

# カテゴリ一覧
CATEGORIES_LARGE = ["（すべて）"] + sorted(item_list["category_large"].dropna().unique().tolist())
def get_mid_categories(large: str) -> list[str]:
    if large == "（すべて）":
        mids = item_list["category_mid"].dropna().unique().tolist()
    else:
        mids = item_list[item_list["category_large"] == large]["category_mid"].dropna().unique().tolist()
    return ["（すべて）"] + sorted(mids)

def get_filtered_items(large: str, mid: str, search: str) -> list[str]:
    df = item_list.copy()
    if large != "（すべて）":
        df = df[df["category_large"] == large]
    if mid != "（すべて）":
        df = df[df["category_mid"] == mid]
    labels = df["display"].tolist()
    if search:
        labels = [lbl for lbl in labels if search.lower() in lbl.lower()]
    return labels

# ─── Presets ───────────────────────────────────────────────
PRESETS = {
    "💰 収益力": ["経常利益", "コア業務純益", "業務粗利益"],
    "⚡ 効率性": ["OHR(業務粗利益ベース)", "OHR(コア業務粗利益ベース)"],
    "📈 収益性": ["ROE(当期純利益ベース)", "ROA(コア業務純益ベース)"],
    "🛡️ 健全性": ["自己資本比率", "不良債権比率"],
    "📊 利鞘": ["総資金利鞘", "預貸金利鞘"],
    "🏢 規模": ["総資産", "貸出金残高", "預金残高"],
}

SCATTER_PRESETS = {
    "効率性 vs 収益性": ("OHR(コア業務粗利益ベース)", "ROE(当期純利益ベース)"),
    "規模 vs 収益力": ("総資産", "経常利益"),
    "自己資本比率 vs ROA": ("自己資本比率", "ROA(コア業務純益ベース)"),
}

def find_exact_item(name: str) -> str | None:
    exact = items_df[items_df["item_name"] == name]
    if len(exact) > 0: return exact.iloc[0]["item_id"]
    part = items_df[items_df["item_name"].str.contains(name, na=False)]
    if len(part) > 0: return part.iloc[0]["item_id"]
    return None

def item_id_to_label(item_id: str) -> str | None:
    if not item_id: return None
    row = item_list[item_list["item_id"] == item_id]
    if len(row) > 0: return row.iloc[0]["display"]
    return None

# ─── Sidebar ──────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🏦 地銀ダッシュボード")
    st.markdown("---")
    selected_fy = st.selectbox("📅 対象年度", FISCAL_YEARS, format_func=fy_label)

# ─── Main Content ─────────────────────────────────────────

tab1, tab2, tab3, tab4 = st.tabs(["📊 ランキング", "🔵 散布図分析", "📈 時系列推移", "🏛️ 個別銀行カード"])

# ============================================================
#  TAB 1: ランキング
# ============================================================
with tab1:
    st.markdown("### 📊 銀行ランキング")
    st.caption("選択した指標で全銀行をランキング表示します")

    # ── 項目選択エリア ──
    r1c1, r1c2, r1c3 = st.columns([1, 1, 1])
    with r1c1:
        sel_large = st.selectbox("大カテゴリ", CATEGORIES_LARGE, key="rank_cat_large")
    with r1c2:
        mid_options = get_mid_categories(sel_large)
        sel_mid = st.selectbox("中カテゴリ", mid_options, key="rank_cat_mid")
    with r1c3:
        rank_search = st.text_input("🔍 キーワード絞り込み", placeholder="例: ROE, 利鞘...", key="rank_search")

    filtered_labels = get_filtered_items(sel_large, sel_mid, rank_search)

    r2c1, r2c2 = st.columns([2, 3])
    with r2c1:
        # プリセットで選択された項目があればデフォルトに
        default_idx = 0
        if "preset_target_label" in st.session_state and st.session_state["preset_target_label"] in filtered_labels:
            default_idx = filtered_labels.index(st.session_state["preset_target_label"])

        sel_label = st.selectbox(
            "項目を選択",
            filtered_labels,
            index=default_idx if default_idx < len(filtered_labels) else 0,
            placeholder="項目を検索...",
            key="ranking_item"
        )
        if "preset_target_label" in st.session_state:
            del st.session_state["preset_target_label"]

        ranking_asc = st.radio(
            "並び順",
            ["降順（大きい順）", "昇順（小さい順）"],
            horizontal=True,
            key="ranking_order"
        ) == "昇順（小さい順）"

    with r2c2:
        st.markdown("**⚡ クイック選択**")
        pcols = st.columns(3)
        for i, (pname, pkeys) in enumerate(PRESETS.items()):
            btn_key = f"preset_rank_{i}"
            if pcols[i % 3].button(pname, key=btn_key, use_container_width=True):
                found = find_exact_item(pkeys[0])
                if found:
                    st.session_state["preset_target_label"] = item_id_to_label(found)
                    st.rerun()

    st.divider()

    if sel_label and sel_label in LABEL_TO_ID:
        sel_id = LABEL_TO_ID[sel_label]
        item_name = get_item_name(sel_id)
        unit = get_unit(sel_id)

        df = get_ranking_data(selected_fy, sel_id, ascending=ranking_asc)

        if len(df) == 0:
            st.info(f"データが見つかりません（{fy_label(selected_fy)}）")
        else:
            st.markdown(f"**{item_name}** ({unit}) — {fy_label(selected_fy)}　　{'昇順' if ranking_asc else '降順'}表示")

            # ── グラフ: 並び順に応じて色も正しく ──
            # ascending=Trueの場合: 小さい順にソート済み → 最下行が最大値
            # ascending=Falseの場合: 大きい順にソート済み → 最上行が最大値
            # colorscaleは値ベースなので、値が大きい=濃い は自動的に正しくなる
            # ただしグラフのy軸方向を制御する必要がある

            # グラフ用: 棒グラフは下から上に積むので、表示順を反転させる
            chart_df = df.iloc[::-1].reset_index(drop=True)

            fig = go.Figure()
            fig.add_trace(go.Bar(
                y=chart_df["bank_name"],
                x=chart_df["value"],
                orientation="h",
                marker=dict(
                    color=chart_df["value"],
                    colorscale=[[0, "#a8c4e0"], [1, "#1a3a5c"]],
                ),
                text=chart_df["value"].apply(lambda v: format_value(v, unit)),
                textposition="auto",
                textfont=dict(size=11, color="#ffffff"),
                hovertemplate="<b>%{y}</b><br>値: %{text}<extra></extra>"
            ))
            fig.update_layout(
                height=max(400, len(chart_df) * 28),
                margin=dict(l=10, r=10, t=10, b=10),
                xaxis_title=unit,
                plot_bgcolor="#FFFFFF",
                paper_bgcolor="#FFFFFF",
                font=dict(family="Noto Sans JP", color="#333333", size=13),
                xaxis=dict(gridcolor="#e1e4e8"),
            )
            st.plotly_chart(fig, use_container_width=True)

            with st.expander("📋 データテーブル", expanded=False):
                display_df = df[["rank", "bank_name", "hq_city", "value"]].copy()
                display_df.columns = ["順位", "銀行名", "本店所在地", f"{item_name} ({unit})"]
                display_df["順位"] = display_df["順位"].apply(lambda r: f"{r}位")
                display_df[f"{item_name} ({unit})"] = display_df[f"{item_name} ({unit})"].apply(
                    lambda v: format_value(v, unit)
                )
                st.dataframe(display_df, use_container_width=True, hide_index=True)


# ============================================================
#  TAB 2: 散布図
# ============================================================
with tab2:
    st.markdown("### 🔵 散布図分析")
    st.caption("2つの指標の相関関係を可視化します")

    colX, colY = st.columns(2)

    with colX:
        s2_large_x = st.selectbox("X: 大カテゴリ", CATEGORIES_LARGE, key="s2_cat_large_x")
        s2_mid_x = st.selectbox("X: 中カテゴリ", get_mid_categories(s2_large_x), key="s2_cat_mid_x")
        s2_search_x = st.text_input("X: キーワード", placeholder="例: OHR", key="s2_search_x")
        x_options = get_filtered_items(s2_large_x, s2_mid_x, s2_search_x)

        default_x = 0
        if "scatter_x_target" in st.session_state and st.session_state["scatter_x_target"] in x_options:
            default_x = x_options.index(st.session_state["scatter_x_target"])
        x_label = st.selectbox("X軸の項目", x_options, index=default_x if default_x < len(x_options) else 0, key="scatter_x")

    with colY:
        s2_large_y = st.selectbox("Y: 大カテゴリ", CATEGORIES_LARGE, key="s2_cat_large_y")
        s2_mid_y = st.selectbox("Y: 中カテゴリ", get_mid_categories(s2_large_y), key="s2_cat_mid_y")
        s2_search_y = st.text_input("Y: キーワード", placeholder="例: ROE", key="s2_search_y")
        y_options = get_filtered_items(s2_large_y, s2_mid_y, s2_search_y)

        default_y = min(1, len(y_options)-1) if len(y_options) > 0 else 0
        if "scatter_y_target" in st.session_state and st.session_state["scatter_y_target"] in y_options:
            default_y = y_options.index(st.session_state["scatter_y_target"])
        y_label = st.selectbox("Y軸の項目", y_options, index=default_y if default_y < len(y_options) else 0, key="scatter_y")

    if "scatter_x_target" in st.session_state: del st.session_state["scatter_x_target"]
    if "scatter_y_target" in st.session_state: del st.session_state["scatter_y_target"]

    st.markdown("**⚡ クイック選択（X-Y 組み合わせ）**")
    pcols = st.columns(3)
    for i, (pname, (kx, ky)) in enumerate(SCATTER_PRESETS.items()):
        if pcols[i % 3].button(pname, key=f"scatter_pre_{i}", use_container_width=True):
            ix = find_exact_item(kx)
            iy = find_exact_item(ky)
            if ix and iy:
                st.session_state["scatter_x_target"] = item_id_to_label(ix)
                st.session_state["scatter_y_target"] = item_id_to_label(iy)
                st.rerun()

    st.divider()

    if x_label and y_label and x_label in LABEL_TO_ID and y_label in LABEL_TO_ID:
        x_id = LABEL_TO_ID[x_label]
        y_id = LABEL_TO_ID[y_label]

        x_data = values_df[(values_df["fiscal_year"] == selected_fy) & (values_df["item_id"] == x_id)]
        y_data = values_df[(values_df["fiscal_year"] == selected_fy) & (values_df["item_id"] == y_id)]

        if AGGREGATE_CODE:
            x_data = x_data[x_data["bank_code"] != AGGREGATE_CODE]
            y_data = y_data[y_data["bank_code"] != AGGREGATE_CODE]

        merged = x_data[["bank_code", "value"]].merge(
            y_data[["bank_code", "value"]], on="bank_code", suffixes=("_x", "_y")
        ).dropna()
        merged = merged.merge(banks_df[["bank_code", "bank_name", "hq_city"]], on="bank_code")

        if len(merged) == 0:
            st.info("データが不足しています")
        else:
            x_name = get_item_name(x_id)
            y_name = get_item_name(y_id)

            corr, p_val = stats.pearsonr(merged["value_x"], merged["value_y"]) if len(merged) > 1 else (0, 0)

            fig = px.scatter(
                merged, x="value_x", y="value_y", hover_name="bank_name",
                hover_data={"value_x": ":.2f", "value_y": ":.2f", "hq_city": True},
                labels={"value_x": x_name, "value_y": y_name},
                color_discrete_sequence=["#1a3a5c"],
            )
            fig.update_traces(marker=dict(size=10))

            if len(merged) > 2:
                slope, intercept, _, _, _ = stats.linregress(merged["value_x"], merged["value_y"])
                x_range = np.linspace(merged["value_x"].min(), merged["value_x"].max(), 100)
                fig.add_trace(go.Scatter(
                    x=x_range, y=slope * x_range + intercept,
                    mode="lines", line=dict(color="#888888", dash="dash"),
                    showlegend=False,
                ))

            fig.update_layout(
                height=600,
                plot_bgcolor="#FFFFFF",
                paper_bgcolor="#FFFFFF",
                font=dict(family="Noto Sans JP", color="#333333", size=13),
                xaxis=dict(gridcolor="#e1e4e8"),
                yaxis=dict(gridcolor="#e1e4e8"),
            )

            fig.add_annotation(
                text=f"相関係数: {corr:.3f} (p={p_val:.4f})",
                xref="paper", yref="paper", x=0.02, y=0.98,
                showarrow=False, font=dict(size=14, color="#1a3a5c"),
                bgcolor="rgba(255,255,255,0.8)", bordercolor="#1a3a5c", borderwidth=1, borderpad=6,
            )

            st.plotly_chart(fig, use_container_width=True)


# ============================================================
#  TAB 3: 時系列
# ============================================================
with tab3:
    st.markdown("### 📈 時系列推移")
    st.caption("選択した銀行・指標の5年間の推移を比較します")

    t3c1, t3c2 = st.columns(2)
    with t3c1:
        t3_large = st.selectbox("大カテゴリ", CATEGORIES_LARGE, key="t3_cat_large")
        t3_mid = st.selectbox("中カテゴリ", get_mid_categories(t3_large), key="t3_cat_mid")
        t3_search = st.text_input("🔍 キーワード", placeholder="例: 経常利益", key="t3_search")
        t3_options = get_filtered_items(t3_large, t3_mid, t3_search)
        ts_label = st.selectbox("項目を選択", t3_options, placeholder="項目を検索...", key="timeseries_item")
        show_aggregate = st.checkbox("地銀合計を参考線として表示", value=True, key="ts_agg")

    with t3c2:
        bank_options = sorted(INDIVIDUAL_BANKS["bank_name"].tolist())
        default_banks = bank_options[:5] if len(bank_options) >= 5 else bank_options
        selected_banks = st.multiselect("比較する銀行を選択（複数可）", bank_options, default=default_banks)

    st.divider()

    if ts_label and selected_banks and ts_label in LABEL_TO_ID:
        ts_id = LABEL_TO_ID[ts_label]
        item_name = get_item_name(ts_id)
        unit = get_unit(ts_id)

        bank_codes = banks_df[banks_df["bank_name"].isin(selected_banks)]["bank_code"].tolist()
        mask = (values_df["item_id"] == ts_id) & (values_df["bank_code"].isin(bank_codes))
        ts_data = values_df[mask].merge(banks_df[["bank_code", "bank_name"]], on="bank_code")
        ts_data["year_label"] = ts_data["fiscal_year"].apply(fy_label)

        fig = go.Figure()

        for bank_name_ts in selected_banks:
            bdata = ts_data[ts_data["bank_name"] == bank_name_ts].sort_values("fiscal_year")
            fig.add_trace(go.Scatter(
                x=bdata["year_label"], y=bdata["value"],
                name=bank_name_ts, mode="lines+markers",
                line=dict(color=BANK_COLORS.get(bank_name_ts, "#888"), width=2),
                marker=dict(size=6),
                hovertemplate="<b>%{data.name}</b><br>年度: %{x}<br>値: %{y}<extra></extra>"
            ))

        if show_aggregate and AGGREGATE_CODE:
            agg_mask = (values_df["item_id"] == ts_id) & (values_df["bank_code"] == AGGREGATE_CODE)
            agg_data = values_df[agg_mask].sort_values("fiscal_year")
            agg_data["year_label"] = agg_data["fiscal_year"].apply(fy_label)
            fig.add_trace(go.Scatter(
                x=agg_data["year_label"], y=agg_data["value"],
                name="地銀合計", mode="lines+markers",
                line=dict(color="#1a3a5c", width=2, dash="dot"),
                marker=dict(size=6, symbol="diamond"),
                hovertemplate="<b>%{data.name}</b><br>年度: %{x}<br>値: %{y}<extra></extra>"
            ))

        fig.update_layout(
            title=f"{item_name} ({unit})",
            height=500,
            plot_bgcolor="#FFFFFF",
            paper_bgcolor="#FFFFFF",
            font=dict(family="Noto Sans JP", color="#333333", size=13),
            xaxis=dict(gridcolor="#e1e4e8"),
            yaxis=dict(gridcolor="#e1e4e8", title=unit),
            legend=dict(bgcolor="rgba(255,255,255,0.8)", bordercolor="#e1e4e8", borderwidth=1),
        )
        st.plotly_chart(fig, use_container_width=True)


# ============================================================
#  TAB 4: 個別銀行カード
# ============================================================
with tab4:
    st.markdown("### 🏛️ 個別銀行カード")
    st.caption("個別銀行の主要な財務指標と推移を確認します")

    col1, _ = st.columns(2)
    with col1:
        bank_name_sel = st.selectbox("銀行を選択", sorted(INDIVIDUAL_BANKS["bank_name"].tolist()), key="card_bank")

    st.divider()

    if bank_name_sel:
        bank_row = banks_df[banks_df["bank_name"] == bank_name_sel]
        bank_code = bank_row.iloc[0]["bank_code"]
        hq_city = bank_row.iloc[0]["hq_city"]

        st.markdown(f"#### {bank_name_sel}　📍 {hq_city}　—　{fy_label(selected_fy)}")

        KEY_METRICS = [
            "経常利益", "コア業務純益", "OHR", "ROE", "ROA", "自己資本比率",
            "預貸率", "総資産", "貸出金残高", "預金残高", "従業員数", "店舗数",
        ]

        cols = st.columns(4)
        for i, keyword in enumerate(KEY_METRICS):
            item_id = find_exact_item(keyword)
            if not item_id: continue

            iname = get_item_name(item_id)
            iunit = get_unit(item_id)

            cur = values_df[(values_df["fiscal_year"] == selected_fy) & (values_df["bank_code"] == bank_code) & (values_df["item_id"] == item_id)]
            cur_val = cur.iloc[0]["value"] if len(cur) > 0 else None

            fy_idx = FISCAL_YEARS.index(selected_fy) if selected_fy in FISCAL_YEARS else 0
            prev_fy = FISCAL_YEARS[fy_idx + 1] if fy_idx + 1 < len(FISCAL_YEARS) else None
            prev_val = None
            if prev_fy:
                prev = values_df[(values_df["fiscal_year"] == prev_fy) & (values_df["bank_code"] == bank_code) & (values_df["item_id"] == item_id)]
                prev_val = prev.iloc[0]["value"] if len(prev) > 0 else None

            delta_html = ""
            if cur_val is not None and prev_val is not None and prev_val != 0:
                delta_pct = (cur_val - prev_val) / abs(prev_val) * 100
                if delta_pct >= 0:
                    delta_html = f'<div class="metric-delta-up">▲ {delta_pct:+.1f}%</div>'
                else:
                    delta_html = f'<div class="metric-delta-down">▼ {delta_pct:+.1f}%</div>'

            val_display = format_value(cur_val, iunit) if cur_val is not None else "N/A"

            with cols[i % 4]:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-label">{iname}</div>
                    <div class="metric-value">{val_display}</div>
                    <div style="font-size:0.75rem; color:#888">{iunit}</div>
                    {delta_html}
                </div>
                """, unsafe_allow_html=True)
                st.write("")

        st.markdown('<div class="section-title" style="margin-top:2rem">📈 5年推移（ミニチャート）</div>', unsafe_allow_html=True)

        spark_cols = st.columns(3)
        for i, keyword in enumerate(KEY_METRICS):
            item_id = find_exact_item(keyword)
            if not item_id: continue

            iname = get_item_name(item_id)
            ts = values_df[(values_df["bank_code"] == bank_code) & (values_df["item_id"] == item_id)].sort_values("fiscal_year")
            if len(ts) == 0: continue

            with spark_cols[i % 3]:
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=ts["fiscal_year"].apply(fy_label), y=ts["value"],
                    mode="lines+markers", line=dict(color="#1a3a5c", width=2), marker=dict(size=4),
                    fill="tozeroy", fillcolor="rgba(26,58,92,0.1)",
                    hovertemplate="<b>%{x}</b><br>値: %{y}<extra></extra>"
                ))
                fig.update_layout(
                    title=dict(text=iname, font=dict(size=11, color="#333333")),
                    height=140, margin=dict(l=5, r=5, t=25, b=5),
                    plot_bgcolor="#FFFFFF", paper_bgcolor="#FFFFFF",
                    font=dict(family="Noto Sans JP", color="#666666", size=10),
                    xaxis=dict(showgrid=False, showticklabels=True, tickfont=dict(size=9)),
                    yaxis=dict(showgrid=False, showticklabels=False),
                    showlegend=False,
                )
                st.plotly_chart(fig, use_container_width=True, key=f"spark_{keyword}_{i}")
