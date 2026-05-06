# -*- coding: utf-8 -*-
from __future__ import annotations

import io
from datetime import datetime, time
from typing import List, Tuple

import altair as alt
import pandas as pd
import streamlit as st


st.set_page_config(page_title="智审平台审核历史分析", layout="wide")


REQUIRED_COLUMNS = {
    "图片": "图片",
    "标注框": "标注框",
    "来源": "来源",
    "操作时间": "操作时间",
    "操作人": "操作人",
    "摄像头编号": "摄像头编号",
    "事件编号": "事件编号",
    "任务ID": "任务ID",
    "置信度": "置信度",
    "违规类型": "违规类型",
    "审核结果": "审核结果",
    "AI免审": "AI免审",
    "是否推送": "是否推送",
    "推送接口成功": "推送接口成功",
}

DISPLAY_COLS = [
    "图片", "标注框","来源", "操作时间", "操作人", "摄像头编号", "事件编号",
    "任务ID", "置信度", "违规类型", "审核结果", "AI免审", "是否推送", "推送接口成功"
]

EVENT_TYPE_ORDER = [
    "垃圾暴露类",
    "环境脏污类",
    "应密闭未密闭类",
    "清运不及时类",
    "垃圾桶乱摆放类",
    "妨碍投放类",
    "设施残缺破损类",
]

STATUS_COLOR_DOMAIN = ["通过事件数", "人工驳回事件数", "系统驳回事件数", "未处理事件数"]
STATUS_COLOR_RANGE = ["#1f77b4", "#d62728", "#7f7f7f", "#ffbf00"]


# ------------------------------
# Utils
# ------------------------------
def normalize_text(v) -> str:
    if pd.isna(v):
        return ""
    return str(v).strip()


def parse_confidence(v) -> float:
    """将置信度统一转换为 0-1 之间的小数。兼容：31.6%、31.6、0.316。"""
    if pd.isna(v):
        return float("nan")
    text = str(v).strip().replace("％", "%")
    if text == "":
        return float("nan")

    has_percent = text.endswith("%")
    if has_percent:
        text = text[:-1].strip()

    num = pd.to_numeric(text, errors="coerce")
    if pd.isna(num):
        return float("nan")

    num = float(num)
    if has_percent or num > 1:
        num = num / 100
    return num


@st.cache_data(show_spinner=False)
def load_excel(file_bytes: bytes, file_name: str) -> pd.DataFrame:
    xls = pd.ExcelFile(io.BytesIO(file_bytes))
    return pd.read_excel(io.BytesIO(file_bytes), sheet_name=xls.sheet_names[0])


def validate_columns(df: pd.DataFrame) -> List[str]:
    return [c for c in REQUIRED_COLUMNS if c not in df.columns]


def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy().reset_index(drop=True)
    out["原始行号"] = out.index + 1

    text_cols = [
        "图片", "标注框", "来源", "操作人", "摄像头编号", "事件编号", "任务ID",
        "违规类型", "审核结果", "AI免审", "是否推送", "推送接口成功"
    ]
    for col in text_cols:
        out[col] = out[col].map(normalize_text)

    out["操作时间_dt"] = pd.to_datetime(out["操作时间"], errors="coerce")
    out["日期"] = out["操作时间_dt"].dt.date
    out["时分秒"] = out["操作时间_dt"].dt.time
    out["置信度_num"] = out["置信度"].apply(parse_confidence)

    out["来源_norm"] = out["来源"].str.upper()
    out["审核结果_norm"] = out["审核结果"]
    out["操作人_norm"] = out["操作人"]
    out["摄像头编号_norm"] = out["摄像头编号"]
    out["违规类型_norm"] = out["违规类型"]
    out["事件编号_norm"] = out["事件编号"]

    # 事件编号为空时，按单条记录参与分析，避免被错误合并
    out["去重键"] = out.apply(
        lambda r: r["事件编号_norm"] if r["事件编号_norm"] else f"__ROW_{int(r['原始行号'])}",
        axis=1,
    )

    def source_priority(v: str) -> int:
        if v == "实时":
            return 2
        if v == "AI":
            return 1
        return 0

    out["来源优先级"] = out["来源"].map(source_priority).fillna(0).astype(int)
    out["操作时间排序"] = out["操作时间_dt"].fillna(pd.Timestamp("1900-01-01"))
    return out


def in_time_ranges(t: time | None, ranges: List[Tuple[time, time]]) -> bool:
    if t is None or pd.isna(t):
        return False
    for start_t, end_t in ranges:
        if start_t <= end_t:
            if start_t <= t <= end_t:
                return True
        else:
            if t >= start_t or t <= end_t:
                return True
    return False


def apply_exclusion_ranges(df: pd.DataFrame, ranges_df: pd.DataFrame) -> pd.DataFrame:
    valid_ranges: List[Tuple[time, time]] = []
    if ranges_df is None or ranges_df.empty:
        return df

    for _, row in ranges_df.iterrows():
        enabled = bool(row.get("启用", True))
        if not enabled:
            continue
        start_v = row.get("开始时间")
        end_v = row.get("结束时间")
        if pd.isna(start_v) or pd.isna(end_v):
            continue
        if isinstance(start_v, datetime):
            start_v = start_v.time()
        if isinstance(end_v, datetime):
            end_v = end_v.time()
        if isinstance(start_v, time) and isinstance(end_v, time):
            valid_ranges.append((start_v, end_v))

    if not valid_ranges:
        return df

    mask_excluded = df["时分秒"].apply(lambda x: in_time_ranges(x, valid_ranges))
    return df.loc[~mask_excluded].copy()


def deduplicate_events(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    sorted_df = df.sort_values(
        by=["去重键", "来源优先级", "操作时间排序", "原始行号"],
        ascending=[True, False, False, False],
    ).copy()
    dedup_df = sorted_df.drop_duplicates(subset=["去重键"], keep="first").copy()
    dedup_df = dedup_df.sort_values(by=["操作时间排序", "原始行号"], ascending=[False, False])
    return dedup_df


# ------------------------------
# Metrics and data builders
# ------------------------------
def calc_overview_metrics(df: pd.DataFrame) -> dict:
    total_event_count = int(len(df))
    pass_count = int((df["审核结果_norm"] == "通过").sum())
    manual_reject_count = int(((df["审核结果_norm"] == "驳回") & (df["来源"] == "实时")).sum())
    system_reject_count = int(((df["审核结果_norm"] == "驳回") & (df["来源"] == "AI")).sum())
    untreated_count = int((df["审核结果_norm"] == "—").sum())
    push_count = int((df["是否推送"] == "是").sum())
    receive_count = int((df["推送接口成功"] == "是").sum())
    camera_count = int(df["摄像头编号_norm"].replace("", pd.NA).dropna().nunique())

    return {
        "事件总数": total_event_count,
        "上报摄像头数": camera_count,
        "通过事件数": pass_count,
        "人工驳回事件数": manual_reject_count,
        "系统驳回事件数": system_reject_count,
        "未处理事件数": untreated_count,
        "推送事件数": push_count,
        "接收事件数": receive_count,
    }


def build_daily_stats(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["日期", "事件总数", "上报摄像头数", "通过事件数", "人工驳回事件数", "系统驳回事件数", "未处理事件数", "推送事件数", "接收事件数"])

    rows = []
    for d, g in df.groupby("日期", dropna=False):
        rows.append({"日期": d, **calc_overview_metrics(g)})
    return pd.DataFrame(rows).sort_values("日期")


def build_event_type_stats(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["违规类型", "事件数", "通过事件数", "人工驳回事件数", "系统驳回事件数", "未处理事件数", "推送事件数", "接收事件数"])

    rows = []
    for event_type, g in df.groupby("违规类型_norm", dropna=False):
        metrics = calc_overview_metrics(g)
        rows.append({
            "违规类型": event_type or "（空）",
            "事件数": metrics["事件总数"],
            "通过事件数": metrics["通过事件数"],
            "人工驳回事件数": metrics["人工驳回事件数"],
            "系统驳回事件数": metrics["系统驳回事件数"],
            "未处理事件数": metrics["未处理事件数"],
            "推送事件数": metrics["推送事件数"],
            "接收事件数": metrics["接收事件数"],
        })
    return pd.DataFrame(rows).sort_values(["事件数", "违规类型"], ascending=[False, True])


def build_camera_stats(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["摄像头编号", "事件数", "通过事件数", "人工驳回事件数", "系统驳回事件数", "未处理事件数", "推送事件数", "接收事件数"])

    rows = []
    for camera, g in df.groupby("摄像头编号_norm", dropna=False):
        metrics = calc_overview_metrics(g)
        rows.append({
            "摄像头编号": camera or "（空）",
            "事件数": metrics["事件总数"],
            "通过事件数": metrics["通过事件数"],
            "人工驳回事件数": metrics["人工驳回事件数"],
            "系统驳回事件数": metrics["系统驳回事件数"],
            "未处理事件数": metrics["未处理事件数"],
            "推送事件数": metrics["推送事件数"],
            "接收事件数": metrics["接收事件数"],
        })
    return pd.DataFrame(rows).sort_values(["事件数", "摄像头编号"], ascending=[False, True])


def to_excel_bytes(named_dfs: dict[str, pd.DataFrame]) -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        for sheet_name, xdf in named_dfs.items():
            xdf.to_excel(writer, index=False, sheet_name=sheet_name[:31])
    return buffer.getvalue()


def get_auto_reject_threshold(event_type: str, d) -> float | None:
    """返回指定事件类型、指定日期下的系统自动驳回置信度阈值。返回值统一为 0-1 小数。"""
    if pd.isna(d):
        return None

    d = pd.to_datetime(d).date()
    change_date = pd.to_datetime("2026-04-23").date()

    rules_before = {
        "妨碍投放类": 0.94,
        "垃圾桶乱摆放类": 0.50,
        "应密闭未密闭类": 0.70,
        "设施残缺破损类": 0.50,
        "清运不及时类": 0.70,
    }

    rules_after = {
        "妨碍投放类": 0.94,
        "垃圾桶乱摆放类": 0.70,
        "应密闭未密闭类": 0.85,
        "设施残缺破损类": 0.30,
        "清运不及时类": 0.90,
        "垃圾暴露类": 0.60,
        "环境脏污类": 0.45,
    }

    rules = rules_after if d >= change_date else rules_before
    return rules.get(event_type)


def build_daily_event_type_stats(df: pd.DataFrame, event_type: str, all_dates: list) -> pd.DataFrame:
    sub = df[df["违规类型_norm"] == event_type].copy()

    rows = []
    for d in all_dates:
        g = sub[sub["日期"] == d]
        metrics = calc_overview_metrics(g)

        total = metrics["事件总数"]
        pass_count = metrics["通过事件数"]
        manual_reject = metrics["人工驳回事件数"]
        system_reject = metrics["系统驳回事件数"]
        untreated = metrics["未处理事件数"]
        camera_count = metrics["上报摄像头数"]

        manual_denominator = pass_count + manual_reject
        manual_pass_rate = pass_count / manual_denominator if manual_denominator > 0 else 0
        system_reject_rate = system_reject / total if total > 0 else 0

        manual_pass_confidence = g.loc[
            (g["审核结果_norm"] == "通过") & (g["来源"] == "实时"),
            "置信度_num",
        ].mean()
        manual_reject_confidence = g.loc[
            (g["审核结果_norm"] == "驳回") & (g["来源"] == "实时"),
            "置信度_num",
        ].mean()
        auto_reject_threshold = get_auto_reject_threshold(event_type, d)

        rows.append({
            "日期": d,
            "日期文本": pd.to_datetime(d).strftime("%m-%d") if pd.notna(d) else "",
            "违规类型": event_type,
            "事件总数": total,
            "通过事件数": pass_count,
            "人工驳回事件数": manual_reject,
            "系统驳回事件数": system_reject,
            "未处理事件数": untreated,
            "上报摄像头数": camera_count,
            "人工通过率": manual_pass_rate,
            "系统驳回率": system_reject_rate,
            "人工通过平均置信度": manual_pass_confidence,
            "人工驳回平均置信度": manual_reject_confidence,
            "自动驳回阈值": auto_reject_threshold,
        })

    return pd.DataFrame(rows)


def build_overall_daily_stats(df: pd.DataFrame, all_dates: list) -> pd.DataFrame:
    rows = []
    for d in all_dates:
        g = df[df["日期"] == d]
        metrics = calc_overview_metrics(g)
        rows.append({
            "日期": d,
            "日期文本": pd.to_datetime(d).strftime("%m-%d") if pd.notna(d) else "",
            **metrics,
        })
    return pd.DataFrame(rows)


def build_confidence_distribution(df: pd.DataFrame, group_name: str) -> pd.DataFrame:
    """
    置信度按 0-10、10-20...90-100 分桶，并按处理状态统计事件数。

    这里按用户当前关注的三类进行拆分：
    - 人工通过事件数：审核结果=通过 且 来源=实时
    - 人工驳回事件数：审核结果=驳回 且 来源=实时
    - 系统驳回事件数：审核结果=驳回 且 来源=AI
    """
    bins = [i / 100 for i in range(0, 110, 10)]
    labels = [f"{i}-{i + 10}" for i in range(0, 100, 10)]
    status_order = ["人工通过事件数", "人工驳回事件数", "系统驳回事件数"]

    base_index = pd.MultiIndex.from_product(
        [labels, status_order],
        names=["置信度区间", "事件状态"],
    )

    work = df.dropna(subset=["置信度_num"]).copy()
    if work.empty:
        result = base_index.to_frame(index=False)
        result["事件数"] = 0
        result["类型"] = group_name
        return result[["类型", "置信度区间", "事件状态", "事件数"]]

    work["置信度_num_clip"] = work["置信度_num"].clip(lower=0, upper=1)
    work["置信度区间"] = pd.cut(
        work["置信度_num_clip"],
        bins=bins,
        right=False,
        include_lowest=True,
        labels=labels,
    )
    # 1.0 会落到 NaN，单独归到 90-100
    work.loc[work["置信度_num_clip"] == 1, "置信度区间"] = "90-100"

    def classify_status(row) -> str | None:
        if row["审核结果_norm"] == "通过" and row["来源"] == "实时":
            return "人工通过事件数"
        if row["审核结果_norm"] == "驳回" and row["来源"] == "实时":
            return "人工驳回事件数"
        if row["审核结果_norm"] == "驳回" and row["来源"] == "AI":
            return "系统驳回事件数"
        return None

    work["事件状态"] = work.apply(classify_status, axis=1)
    work = work.dropna(subset=["事件状态", "置信度区间"]).copy()

    if work.empty:
        result = base_index.to_frame(index=False)
        result["事件数"] = 0
    else:
        result = (
            work.groupby(["置信度区间", "事件状态"], observed=False)
            .size()
            .reindex(base_index, fill_value=0)
            .reset_index(name="事件数")
        )

    result["类型"] = group_name
    result["置信度区间"] = pd.Categorical(result["置信度区间"], categories=labels, ordered=True)
    result["事件状态"] = pd.Categorical(result["事件状态"], categories=status_order, ordered=True)
    return result[["类型", "置信度区间", "事件状态", "事件数"]]

def build_auto_reject_threshold_curve(df: pd.DataFrame, group_name: str) -> pd.DataFrame:
    """
    以 1% 为步长评估自动驳回阈值。

    横轴阈值 T：置信度 < T 的人工审核事件。
    自动驳回正确率：人工驳回事件数 / (人工通过事件数 + 人工驳回事件数)。
    误驳率：人工通过事件数 / (人工通过事件数 + 人工驳回事件数)。
    """
    work_all = df.dropna(subset=["置信度_num"]).copy()
    work_all["置信度_num_clip"] = work_all["置信度_num"].clip(lower=0, upper=1)

    work = work_all[
        ((work_all["审核结果_norm"] == "通过") & (work_all["来源"] == "实时")) |
        ((work_all["审核结果_norm"] == "驳回") & (work_all["来源"] == "实时"))
    ].copy()

    rows = []
    for t in range(1, 101):
        threshold = t / 100
        sub = work[work["置信度_num_clip"] < threshold]

        all_sub = work_all[work_all["置信度_num_clip"] < threshold]

        total_below_count = int(len(all_sub))

        manual_pass = int((sub["审核结果_norm"] == "通过").sum())
        manual_reject = int((sub["审核结果_norm"] == "驳回").sum())
        sample_count = manual_pass + manual_reject

        correct_rate = manual_reject / sample_count if sample_count > 0 else None
        false_reject_rate = manual_pass / sample_count if sample_count > 0 else None

        rows.append({
            "类型": group_name,
            "阈值": threshold,
            "阈值显示": f"<{t}%",
            "人工审核样本数": sample_count,
            "误驳数量": manual_pass,
            "正确驳回数量": manual_reject,
            "自动驳回正确率": correct_rate,
            "误驳率": false_reject_rate,
            "低于阈值总事件数": total_below_count,
        })

    return pd.DataFrame(rows)


# ------------------------------
# Shared UI pieces
# ------------------------------
def render_filter_panel(source_df: pd.DataFrame, key_prefix: str) -> pd.DataFrame:
    """复用筛选栏：日期、摄像头、操作人、事件类型。"""
    st.subheader("筛选栏")

    min_date = source_df["日期"].dropna().min()
    max_date = source_df["日期"].dropna().max()

    c1, c2, c3, c4 = st.columns([1.25, 1.1, 1.1, 1.1])
    with c1:
        if pd.notna(min_date) and pd.notna(max_date):
            date_range = st.date_input(
                "日期范围",
                value=(min_date, max_date),
                min_value=min_date,
                max_value=max_date,
                key=f"{key_prefix}_date_range",
            )
        else:
            date_range = ()
            st.date_input("日期范围", disabled=True, key=f"{key_prefix}_date_range_disabled")

    camera_options = sorted([x for x in source_df["摄像头编号_norm"].dropna().astype(str).unique().tolist() if x != ""])
    operator_options = sorted([x for x in source_df["操作人_norm"].dropna().astype(str).unique().tolist() if x != ""])
    event_type_options = sorted([x for x in source_df["违规类型_norm"].dropna().astype(str).unique().tolist() if x != ""])

    with c2:
        selected_cameras = st.multiselect("摄像头（可多选）", options=camera_options, default=[], key=f"{key_prefix}_cameras")
    with c3:
        selected_operators = st.multiselect("操作人（可多选）", options=operator_options, default=[], key=f"{key_prefix}_operators")
    with c4:
        selected_event_types = st.multiselect("事件类型（可多选）", options=event_type_options, default=[], key=f"{key_prefix}_event_types")

    filtered_df = source_df.copy()
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
        filtered_df = filtered_df[filtered_df["日期"].between(start_date, end_date, inclusive="both")].copy()
    if selected_cameras:
        filtered_df = filtered_df[filtered_df["摄像头编号_norm"].isin(selected_cameras)].copy()
    if selected_operators:
        filtered_df = filtered_df[filtered_df["操作人_norm"].isin(selected_operators)].copy()
    if selected_event_types:
        filtered_df = filtered_df[filtered_df["违规类型_norm"].isin(selected_event_types)].copy()

    return filtered_df


def render_event_type_ratio_chart(filtered_df: pd.DataFrame) -> None:
    st.markdown("#### 7 类事件占比")

    pie_df = (
        filtered_df.groupby("违规类型_norm", dropna=False)
        .size()
        .reset_index(name="事件数")
    )
    pie_df["违规类型"] = pie_df["违规类型_norm"].replace("", "（空）")
    pie_df = pie_df[pie_df["事件数"] > 0].copy()

    if pie_df.empty:
        st.info("当前筛选条件下暂无事件类型数据。")
        return

    pie_df["占比"] = pie_df["事件数"] / pie_df["事件数"].sum()
    pie_df["占比显示"] = (pie_df["占比"] * 100).round(2)
    pie_df["占比标签"] = pie_df["占比显示"].astype(str) + "%"

    type_bar = (
        alt.Chart(pie_df)
        .mark_bar()
        .encode(
            y=alt.Y("违规类型:N", title="事件类型", sort="-x"),
            x=alt.X("占比:Q", title="占比", axis=alt.Axis(format="%")),
            color=alt.Color("违规类型:N", title="事件类型", legend=None),
            tooltip=[
                alt.Tooltip("违规类型:N", title="事件类型"),
                alt.Tooltip("事件数:Q", title="事件数"),
                alt.Tooltip("占比显示:Q", title="占比（%）"),
            ],
        )
        .properties(height=260)
    )

    type_text = (
        alt.Chart(pie_df)
        .mark_text(align="left", baseline="middle", dx=5, fontSize=13)
        .encode(
            y=alt.Y("违规类型:N", sort="-x"),
            x=alt.X("占比:Q"),
            text=alt.Text("占比标签:N"),
        )
    )

    st.altair_chart(type_bar + type_text, use_container_width=True)

    pie_table = pie_df[["违规类型", "事件数", "占比标签"]].copy()
    pie_table = pie_table.rename(columns={"占比标签": "占比"})
    st.dataframe(
        pie_table.sort_values("事件数", ascending=False),
        use_container_width=True,
        hide_index=True,
        height=260,
    )


# ------------------------------
# Charts
# ------------------------------
def build_status_stack_bar(df: pd.DataFrame, height: int) -> alt.Chart:
    bar_df = df.melt(
        id_vars=["日期文本"],
        value_vars=STATUS_COLOR_DOMAIN,
        var_name="指标",
        value_name="数量",
    )
    stack_order = {name: i + 1 for i, name in enumerate(STATUS_COLOR_DOMAIN)}
    bar_df["堆叠顺序"] = bar_df["指标"].map(stack_order)

    return (
        alt.Chart(bar_df)
        .mark_bar()
        .encode(
            x=alt.X("日期文本:N", title="日期", sort=None),
            y=alt.Y("数量:Q", title="事件数量"),
            color=alt.Color(
                "指标:N",
                title="指标",
                scale=alt.Scale(domain=STATUS_COLOR_DOMAIN, range=STATUS_COLOR_RANGE),
                sort=STATUS_COLOR_DOMAIN,
            ),
            order=alt.Order("堆叠顺序:Q", sort="ascending"),
            tooltip=[
                alt.Tooltip("日期文本:N", title="日期"),
                alt.Tooltip("指标:N", title="指标"),
                alt.Tooltip("数量:Q", title="数量"),
            ],
        )
        .properties(height=height)
    )


def build_camera_line(df: pd.DataFrame) -> alt.Chart:
    return (
        alt.Chart(df)
        .mark_line(point=True, strokeWidth=2, color="black")
        .encode(
            x=alt.X("日期文本:N", title="日期", sort=None),
            y=alt.Y("上报摄像头数:Q", title="上报摄像头数", axis=alt.Axis(orient="right")),
            tooltip=[
                alt.Tooltip("日期文本:N", title="日期"),
                alt.Tooltip("上报摄像头数:Q", title="上报摄像头数"),
            ],
        )
    )


def render_overall_daily_chart(df: pd.DataFrame, all_dates: list) -> None:
    stats = build_overall_daily_stats(df, all_dates)
    if stats.empty:
        st.info("当前筛选条件下暂无整体趋势数据。")
        return

    st.markdown("### 整体每日趋势")
    bar_chart = build_status_stack_bar(stats, height=420)
    camera_line_chart = build_camera_line(stats)

    overall_chart = (
        alt.layer(bar_chart, camera_line_chart)
        .resolve_scale(y="independent")
        .properties(height=420)
    )

    st.altair_chart(overall_chart, use_container_width=True)

    with st.expander("查看整体每日明细表", expanded=False):
        st.dataframe(
            stats[[
                "日期文本", "事件总数", "上报摄像头数", "通过事件数", "人工驳回事件数",
                "系统驳回事件数", "未处理事件数", "推送事件数", "接收事件数",
            ]],
            use_container_width=True,
            height=220,
        )


def render_event_type_daily_charts(df: pd.DataFrame, event_type: str, all_dates: list) -> None:
    stats = build_daily_event_type_stats(df, event_type, all_dates)
    if stats.empty:
        st.info(f"{event_type}：当前筛选条件下暂无数据。")
        return

    st.markdown(f"### {event_type}")
    left, right = st.columns([1, 1.35], gap="large")

    bar_chart = build_status_stack_bar(stats, height=520)
    camera_line_chart = build_camera_line(stats)
    left_chart = alt.layer(bar_chart, camera_line_chart).resolve_scale(y="independent").properties(height=520)

    rate_df = stats.melt(
        id_vars=["日期文本"],
        value_vars=["人工通过率", "系统驳回率"],
        var_name="指标",
        value_name="比例",
    )
    rate_df["比例显示"] = (rate_df["比例"] * 100).round(2)

    rate_chart = (
        alt.Chart(rate_df)
        .mark_line(point=True)
        .encode(
            x=alt.X("日期文本:N", title="日期", sort=None),
            y=alt.Y(
                "比例:Q",
                title="审核结果比例",
                axis=alt.Axis(format="%", values=[0, 0.25, 0.5, 0.75, 1]),
                scale=alt.Scale(domain=[0, 1]),
            ),
            color=alt.Color(
                "指标:N",
                title="指标",
                scale=alt.Scale(domain=["人工通过率", "系统驳回率"], range=["#1f77b4", "#7f7f7f"]),
                legend=alt.Legend(orient="top", direction="horizontal"),
            ),
            tooltip=[
                alt.Tooltip("日期文本:N", title="日期"),
                alt.Tooltip("指标:N", title="指标"),
                alt.Tooltip("比例显示:Q", title="比例（%）"),
            ],
        )
        .properties(height=320, title="审核结果比例")
    )

    confidence_df = stats.melt(
        id_vars=["日期文本"],
        value_vars=["人工通过平均置信度", "人工驳回平均置信度"],
        var_name="指标",
        value_name="平均置信度",
    )
    confidence_df["平均置信度显示"] = (confidence_df["平均置信度"] * 100).round(2)

    confidence_chart = (
        alt.Chart(confidence_df)
        .mark_line(point=True)
        .encode(
            x=alt.X("日期文本:N", title="日期", sort=None),
            y=alt.Y(
                "平均置信度:Q",
                title="平均置信度",
                axis=alt.Axis(format="%", values=[0, 0.25, 0.5, 0.75, 1]),
                scale=alt.Scale(domain=[0, 1]),
            ),
            color=alt.Color(
                "指标:N",
                title="指标",
                scale=alt.Scale(domain=["人工通过平均置信度", "人工驳回平均置信度"], range=["#1f77b4", "#d62728"]),
                legend=alt.Legend(orient="top", direction="horizontal"),
            ),
            tooltip=[
                alt.Tooltip("日期文本:N", title="日期"),
                alt.Tooltip("指标:N", title="指标"),
                alt.Tooltip("平均置信度显示:Q", title="平均置信度（%）"),
            ],
        )
        .properties(height=320, title="人工处理平均置信度")
    )

    threshold_df = stats[["日期文本", "自动驳回阈值"]].dropna().copy()
    threshold_df["自动驳回阈值显示"] = (threshold_df["自动驳回阈值"] * 100).round(2)

    threshold_chart = (
        alt.Chart(threshold_df)
        .mark_line(strokeDash=[6, 4], strokeWidth=2, color="green", point=True)
        .encode(
            x=alt.X("日期文本:N", title="日期", sort=None),
            y=alt.Y(
                "自动驳回阈值:Q",
                title="平均置信度",
                axis=alt.Axis(format="%", values=[0, 0.25, 0.5, 0.75, 1]),
                scale=alt.Scale(domain=[0, 1]),
            ),
            tooltip=[
                alt.Tooltip("日期文本:N", title="日期"),
                alt.Tooltip("自动驳回阈值显示:Q", title="自动驳回阈值（%）"),
            ],
        )
    )

    with left:
        st.altair_chart(left_chart, use_container_width=True)

    with right:
        st.altair_chart(rate_chart, use_container_width=True)
        confidence_combined_chart = (
            alt.layer(confidence_chart, threshold_chart)
            .resolve_scale(y="shared")
            .properties(height=320, title="人工处理平均置信度 / 自动驳回阈值")
        )
        st.altair_chart(confidence_combined_chart, use_container_width=True)

    with st.expander(f"查看 {event_type} 每日明细表", expanded=False):
        show_df = stats.copy()
        for col in ["人工通过率", "系统驳回率", "人工通过平均置信度", "人工驳回平均置信度", "自动驳回阈值"]:
            show_df[col] = show_df[col].apply(lambda x: "-" if pd.isna(x) else f"{x * 100:.2f}%")
        st.dataframe(
            show_df[[
                "日期文本", "事件总数", "上报摄像头数", "通过事件数", "人工驳回事件数",
                "系统驳回事件数", "未处理事件数", "人工通过率", "系统驳回率",
                "人工通过平均置信度", "人工驳回平均置信度", "自动驳回阈值",
            ]],
            use_container_width=True,
            height=220,
        )


def render_confidence_distribution_section(filtered_df: pd.DataFrame, title: str, height: int = 280) -> pd.DataFrame:
    dist_df = build_confidence_distribution(filtered_df, title)
    interval_order = [f"{i}-{i+10}" for i in range(0, 100, 10)]
    interval_order_desc = [f"{i}-{i+10}" for i in range(90, -10, -10)]
    status_order = ["人工通过事件数", "人工驳回事件数", "系统驳回事件数"]

    # 图 1：置信度区间分布，按人工通过 / 人工驳回 / 系统驳回堆叠显示
    chart = (
        alt.Chart(dist_df)
        .mark_bar()
        .encode(
            x=alt.X("置信度区间:N", title="置信度区间", sort=interval_order),
            y=alt.Y("事件数:Q", title="事件数"),
            color=alt.Color(
                "事件状态:N",
                title="事件状态",
                scale=alt.Scale(
                    domain=status_order,
                    range=["#1f77b4", "#d62728", "#7f7f7f"],  # 蓝、红、灰
                ),
                sort=status_order,
                legend=alt.Legend(orient="top", direction="horizontal"),
            ),
            tooltip=[
                alt.Tooltip("类型:N", title="类型"),
                alt.Tooltip("置信度区间:N", title="置信度区间"),
                alt.Tooltip("事件状态:N", title="事件状态"),
                alt.Tooltip("事件数:Q", title="事件数"),
            ],
        )
        .properties(height=height)
    )
    st.altair_chart(chart, use_container_width=True)

    # 表格数据：用于展示每个置信度区间的人工审核样本数、人工通过率等
    table_df = (
        dist_df.pivot_table(
            index="置信度区间",
            columns="事件状态",
            values="事件数",
            aggfunc="sum",
            fill_value=0,
            observed=False,
        )
        .reindex(interval_order, fill_value=0)
        .reset_index()
    )
    for col in status_order:
        if col not in table_df.columns:
            table_df[col] = 0

    table_df["合计"] = table_df[status_order].sum(axis=1)
    total_count = table_df["合计"].sum()
    table_df["比例"] = table_df["合计"].apply(
        lambda x: "-" if total_count == 0 else f"{x / total_count * 100:.2f}%"
    )
    table_df["人工审核样本数"] = table_df["人工通过事件数"] + table_df["人工驳回事件数"]
    table_df["人工通过率_num"] = table_df.apply(
        lambda row: None if row["人工审核样本数"] == 0 else row["人工通过事件数"] / row["人工审核样本数"],
        axis=1,
    )
    table_df["人工通过率"] = table_df["人工通过率_num"].apply(
        lambda x: "-" if pd.isna(x) else f"{x * 100:.2f}%"
    )

    # 图 2：置信度-人工通过率曲线
    # 口径：人工通过率 = 人工通过事件数 / (人工通过事件数 + 人工驳回事件数)
    rate_df = table_df.copy()
    rate_df["置信度区间"] = rate_df["置信度区间"].astype(str)
    rate_df = rate_df[rate_df["人工审核样本数"] > 0].copy()
    rate_df["人工通过率显示"] = (rate_df["人工通过率_num"] * 100).round(2)

    if rate_df.empty:
        st.info("当前筛选条件下没有人工审核样本，无法绘制置信度-人工通过率曲线。")
    else:
        pass_rate_line = (
            alt.Chart(rate_df)
            .mark_line(point=True, strokeWidth=3, color="#1f77b4")
            .encode(
                x=alt.X("置信度区间:N", title="置信度区间", sort=interval_order),
                y=alt.Y(
                    "人工通过率_num:Q",
                    title="人工通过率",
                    axis=alt.Axis(format="%", values=[0, 0.25, 0.5, 0.75, 1]),
                    scale=alt.Scale(domain=[0, 1]),
                ),
                tooltip=[
                    alt.Tooltip("置信度区间:N", title="置信度区间"),
                    alt.Tooltip("人工审核样本数:Q", title="人工审核样本数"),
                    alt.Tooltip("人工通过事件数:Q", title="人工通过事件数"),
                    alt.Tooltip("人工驳回事件数:Q", title="人工驳回事件数"),
                    alt.Tooltip("人工通过率显示:Q", title="人工通过率（%）"),
                ],
            )
            .properties(height=220, title="置信度-人工通过率曲线")
        )
        st.altair_chart(pass_rate_line, use_container_width=True)


    # 图 3：自动驳回阈值评估曲线
    # 口径：在置信度 < T 的人工审核事件中，人工驳回占比越高，说明把 T 作为自动驳回阈值越安全。
    threshold_curve_df = build_auto_reject_threshold_curve(filtered_df, title)
    threshold_curve_plot_df = threshold_curve_df.dropna(subset=["自动驳回正确率"]).copy()
    threshold_curve_plot_df["自动驳回正确率显示"] = (threshold_curve_plot_df["自动驳回正确率"] * 100).round(2)
    threshold_curve_plot_df["误驳率显示"] = (threshold_curve_plot_df["误驳率"] * 100).round(2)

    if threshold_curve_plot_df.empty:
        st.info("当前筛选条件下没有人工审核样本，无法绘制自动驳回阈值评估曲线。")
    else:
        threshold_curve = (
            alt.Chart(threshold_curve_plot_df)
            .mark_line(point=False, strokeWidth=3, color="#2ca02c")
            .encode(
                x=alt.X(
                    "阈值:Q",
                    title="自动驳回阈值（置信度 < T）",
                    axis=alt.Axis(format="%", values=[i / 10 for i in range(0, 11)]),
                    scale=alt.Scale(domain=[0, 1]),
                ),
                y=alt.Y(
                    "自动驳回正确率:Q",
                    title="自动驳回正确率",
                    axis=alt.Axis(format="%", values=[0, 0.25, 0.5, 0.75, 1]),
                    scale=alt.Scale(domain=[0, 1]),
                ),
                tooltip=[
                    alt.Tooltip("类型:N", title="类型"),
                    alt.Tooltip("阈值显示:N", title="阈值"),
                    alt.Tooltip("人工审核样本数:Q", title="人工审核样本数"),
                    alt.Tooltip("正确驳回数量:Q", title="正确驳回数量"),
                    alt.Tooltip("误驳数量:Q", title="误驳数量"),
                    alt.Tooltip("自动驳回正确率显示:Q", title="自动驳回正确率（%）"),
                    alt.Tooltip("误驳率显示:Q", title="误驳率（%）"),
                ],
            )
            .properties(height=380, title="自动驳回阈值评估曲线")
        )

        sample_bar = (
            alt.Chart(threshold_curve_plot_df)
            .mark_bar(opacity=0.22, color="#999999")
            .encode(
                x=alt.X("阈值:Q", title="自动驳回阈值（置信度 < T）", axis=alt.Axis(format="%"), scale=alt.Scale(domain=[0, 1])),
                y=alt.Y("低于阈值总事件数:Q", title="低于阈值总事件数", axis=alt.Axis(orient="right")),
                tooltip=[
                    alt.Tooltip("阈值显示:N", title="阈值"),
                    alt.Tooltip("低于阈值总事件数:Q", title="低于阈值总事件数"),
                ],
            )
        )

        threshold_combined = (
            alt.layer(sample_bar, threshold_curve)
            .resolve_scale(y="independent")
            .properties(height=380)
        )
        st.altair_chart(threshold_combined, use_container_width=True)

        threshold_table = threshold_curve_df.copy()
        threshold_table["自动驳回正确率"] = threshold_table["自动驳回正确率"].apply(
            lambda x: "-" if pd.isna(x) else f"{x * 100:.2f}%"
        )
        threshold_table["误驳率"] = threshold_table["误驳率"].apply(
            lambda x: "-" if pd.isna(x) else f"{x * 100:.2f}%"
        )
        with st.expander(f"查看 {title} 自动驳回阈值评估明细", expanded=False):
            st.dataframe(
                threshold_table[[
                    "阈值显示",
                    "人工审核样本数",
                    "正确驳回数量",
                    "误驳数量",
                    "自动驳回正确率",
                    "误驳率",
                    "低于阈值总事件数",
                ]],
                use_container_width=True,
                hide_index=True,
                height=320,
            )

    # 明细表：按置信度由高到低显示
    table_df["置信度区间"] = pd.Categorical(
        table_df["置信度区间"].astype(str),
        categories=interval_order_desc,
        ordered=True,
    )
    table_df = table_df.sort_values("置信度区间")
    table_df = table_df[
        [
            "置信度区间",
            *status_order,
            "合计",
            "比例",
            "人工审核样本数",
            "人工通过率",
        ]
    ]

    st.dataframe(table_df, use_container_width=True, hide_index=True, height=260)
    return dist_df
# Page rendering
# ------------------------------
st.sidebar.title("页面导航")
page = st.sidebar.radio("选择页面", ["总览页", "业务分析", "置信度分析"])

st.title("天天智审审核历史分析")
st.caption("先按免分析时间段排除，再按事件编号去重；去重后再进行筛选和统计。")

# Session defaults
if "uploaded_file_bytes" not in st.session_state:
    st.session_state["uploaded_file_bytes"] = None
if "uploaded_file_name" not in st.session_state:
    st.session_state["uploaded_file_name"] = None
if "use_sample" not in st.session_state:
    st.session_state["use_sample"] = False

# Sidebar quick status
if st.session_state.get("uploaded_file_name"):
    st.sidebar.success(f"当前文件：{st.session_state['uploaded_file_name']}")
elif st.session_state.get("use_sample"):
    st.sidebar.info("当前使用样例文件")
else:
    st.sidebar.warning("请先在总览页上传 Excel")


# Page 1: Upload and exclusion setup
if page == "总览页":
    with st.container(border=True):
        st.subheader("1. Excel 导入")
        uploaded_file = st.file_uploader(
            "上传智审平台导出的事件列表 Excel",
            type=["xlsx", "xls"],
            accept_multiple_files=False,
        )
        use_sample = st.checkbox("没有文件时，使用当前样例文件演示", value=st.session_state.get("use_sample", False))

        if uploaded_file is not None:
            st.session_state["uploaded_file_bytes"] = uploaded_file.getvalue()
            st.session_state["uploaded_file_name"] = uploaded_file.name
            st.session_state["use_sample"] = False
            st.success(f"已上传：{uploaded_file.name}")
        else:
            st.session_state["use_sample"] = use_sample

    # Load current data for preview if available
    file_bytes = st.session_state.get("uploaded_file_bytes")
    file_name = st.session_state.get("uploaded_file_name")
    if file_bytes is None and st.session_state.get("use_sample"):
        sample_path = "/mnt/data/事件标记历史（无图）_18_22_sample.xlsx"
        try:
            with open(sample_path, "rb") as f:
                file_bytes = f.read()
            file_name = "事件标记历史（无图）_18_22_sample.xlsx"
        except FileNotFoundError:
            st.warning("当前环境未找到样例文件，请上传 Excel。")

    if file_bytes is not None:
        raw_df = load_excel(file_bytes, file_name or "uploaded.xlsx")
        missing_cols = validate_columns(raw_df)
        if missing_cols:
            st.error(f"Excel 缺少必要列：{', '.join(missing_cols)}")
        else:
            df = prepare_dataframe(raw_df)
            with st.expander("查看源数据预览", expanded=True):
                st.write(f"文件名：{file_name}")
                st.write(f"原始行数：{len(df):,}")
                st.dataframe(df[DISPLAY_COLS].head(50), use_container_width=True, height=320)

    with st.container(border=True):
        st.subheader("2. 免分析时间段")
        st.caption("落在以下时段内的记录，将被排除出分析范围。该设置会用于业务分析页和置信度分析页。")
        default_ranges = pd.DataFrame([
            {"启用": True, "开始时间": time(0, 0), "结束时间": time(9, 30)},
            {"启用": True, "开始时间": time(12, 0), "结束时间": time(14, 0)},
            {"启用": True, "开始时间": time(20, 0), "结束时间": time(23, 59)},
        ])
        st.session_state["exclude_time_ranges"] = st.data_editor(
            st.session_state.get("exclude_time_ranges", default_ranges),
            use_container_width=True,
            num_rows="dynamic",
            hide_index=True,
            column_config={
                "启用": st.column_config.CheckboxColumn("启用"),
                "开始时间": st.column_config.TimeColumn("开始时间", format="HH:mm"),
                "结束时间": st.column_config.TimeColumn("结束时间", format="HH:mm"),
            },
            key="exclude_time_ranges_editor",
        )

    st.info("上传文件并确认免分析时间段后，可在左侧切换到“业务分析”或“置信度分析”。")
    st.stop()


# Load data for analysis pages
file_bytes = st.session_state.get("uploaded_file_bytes")
file_name = st.session_state.get("uploaded_file_name")
if file_bytes is None and st.session_state.get("use_sample"):
    sample_path = "/mnt/data/事件标记历史（无图）_18_22_sample.xlsx"
    try:
        with open(sample_path, "rb") as f:
            file_bytes = f.read()
        file_name = "事件标记历史（无图）_18_22_sample.xlsx"
    except FileNotFoundError:
        pass

if file_bytes is None:
    st.warning("请先在“总览页”上传 Excel 文件。")
    st.stop()

raw_df = load_excel(file_bytes, file_name or "uploaded.xlsx")
missing_cols = validate_columns(raw_df)
if missing_cols:
    st.error(f"Excel 缺少必要列：{', '.join(missing_cols)}")
    st.stop()

df = prepare_dataframe(raw_df)
exclude_ranges = st.session_state.get("exclude_time_ranges")
if exclude_ranges is None:
    exclude_ranges = pd.DataFrame([
        {"启用": True, "开始时间": time(0, 0), "结束时间": time(9, 30)},
        {"启用": True, "开始时间": time(12, 0), "结束时间": time(14, 0)},
        {"启用": True, "开始时间": time(20, 0), "结束时间": time(23, 59)},
    ])

range_filtered_df = apply_exclusion_ranges(df, exclude_ranges)
dedup_df = deduplicate_events(range_filtered_df)


if page == "业务分析":
    with st.container(border=True):
        filtered_df = render_filter_panel(dedup_df, key_prefix="business")

    with st.container(border=True):
        st.subheader("4. 数据概要")
        metrics = calc_overview_metrics(filtered_df)
        overview_left, overview_right = st.columns([1.05, 1], gap="large")

        with overview_left:
            r1c1, r1c2 = st.columns(2)
            r2c1, r2c2 = st.columns(2)
            r3c1, r3c2 = st.columns(2)
            r4c1, r4c2 = st.columns(2)

            r1c1.metric("事件总数", f"{metrics['事件总数']:,}")
            r1c2.metric("上报摄像头数", f"{metrics['上报摄像头数']:,}")
            r2c1.metric("系统驳回事件数", f"{metrics['系统驳回事件数']:,}")
            r2c2.metric("未处理事件数", f"{metrics['未处理事件数']:,}")
            r3c1.metric("通过事件数", f"{metrics['通过事件数']:,}")
            r3c2.metric("人工驳回事件数", f"{metrics['人工驳回事件数']:,}")
            r4c1.metric("推送事件数", f"{metrics['推送事件数']:,}")
            r4c2.metric("接收事件数", f"{metrics['接收事件数']:,}")

            status_sum = metrics["通过事件数"] + metrics["人工驳回事件数"] + metrics["系统驳回事件数"] + metrics["未处理事件数"]
            if status_sum == metrics["事件总数"]:
                st.success("当前口径校验通过：通过 + 人工驳回 + 系统驳回 + 未处理 = 事件总数")
            else:
                st.warning(
                    f"当前口径校验未通过：四类状态合计 {status_sum:,}，事件总数 {metrics['事件总数']:,}。请检查是否存在异常审核结果。"
                )

            st.caption(
                f"原始导入总记录数：{len(df):,} 条；"
                f"排除免分析时间段后的记录数：{len(range_filtered_df):,} 条；"
                f"去重后的事件数：{len(dedup_df):,} 条；"
                f"当前筛选后的事件数：{len(filtered_df):,} 条。"
            )

        with overview_right:
            render_event_type_ratio_chart(filtered_df)

    with st.container(border=True):
        st.subheader("5. 分事件类型每日趋势")

        if filtered_df.empty:
            st.info("当前筛选条件下暂无数据。")
        else:
            date_series = filtered_df["日期"].dropna()
            if date_series.empty:
                st.info("当前数据缺少有效日期，无法绘制趋势图。")
            else:
                start_d = date_series.min()
                end_d = date_series.max()
                all_dates = list(pd.date_range(start=start_d, end=end_d, freq="D").date)

                render_overall_daily_chart(filtered_df, all_dates)
                st.markdown("---")

                for event_type in EVENT_TYPE_ORDER:
                    render_event_type_daily_charts(filtered_df, event_type, all_dates)
                    st.markdown("---")

    st.markdown("---")
    st.subheader("统计分析")

    daily_stats = build_daily_stats(filtered_df)
    event_type_stats = build_event_type_stats(filtered_df)
    camera_stats = build_camera_stats(filtered_df)

    tab1, tab2, tab3, tab4 = st.tabs(["按日统计", "按违规类型统计", "按摄像头统计", "去重后明细"])

    with tab1:
        st.dataframe(daily_stats, use_container_width=True, height=360)
        if not daily_stats.empty:
            chart_df = daily_stats.set_index("日期")
            st.bar_chart(chart_df[["事件总数", "通过事件数", "人工驳回事件数", "系统驳回事件数", "未处理事件数"]], height=320)

    with tab2:
        st.dataframe(event_type_stats, use_container_width=True, height=420)
        if not event_type_stats.empty:
            chart_df = event_type_stats[["违规类型", "事件数"]].set_index("违规类型")
            st.bar_chart(chart_df, height=360)

    with tab3:
        st.dataframe(camera_stats, use_container_width=True, height=420)
        if not camera_stats.empty:
            top_camera_df = camera_stats.head(20)[["摄像头编号", "事件数"]].set_index("摄像头编号")
            st.bar_chart(top_camera_df, height=360)

    with tab4:
        st.dataframe(filtered_df[DISPLAY_COLS], use_container_width=True, height=520)

    st.markdown("---")
    st.subheader("导出结果")
    export_bytes = to_excel_bytes({
        "去重后筛选明细": filtered_df[DISPLAY_COLS],
        "数据概要": pd.DataFrame([metrics]),
        "按日统计": daily_stats,
        "按违规类型统计": event_type_stats,
        "按摄像头统计": camera_stats,
    })

    st.download_button(
        "下载分析结果 Excel",
        data=export_bytes,
        file_name="智审平台审核历史分析结果.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


elif page == "置信度分析":
    with st.container(border=True):
        filtered_df = render_filter_panel(dedup_df, key_prefix="confidence")

    with st.container(border=True):
        st.subheader("置信度分析")
        st.caption("置信度区间按 0-10、10-20、20-30……90-100 进行划分。统计对象为当前筛选后的去重事件。")

        if filtered_df.empty:
            st.info("当前筛选条件下暂无数据。")
            st.stop()

        st.markdown("### 整体置信度分布")
        overall_dist = render_confidence_distribution_section(filtered_df, "整体", height=300)

        st.markdown("---")
        st.markdown("### 分 7 类事件置信度分布")

        all_dist_dfs = [overall_dist]
        for event_type in EVENT_TYPE_ORDER:
            sub = filtered_df[filtered_df["违规类型_norm"] == event_type].copy()
            st.markdown(f"#### {event_type}")
            if sub.empty:
                st.info(f"当前筛选条件下暂无 {event_type} 数据。")
                dist_df = build_confidence_distribution(sub, event_type)
                all_dist_dfs.append(dist_df)
                continue
            dist_df = render_confidence_distribution_section(sub, event_type, height=240)
            all_dist_dfs.append(dist_df)

        st.markdown("---")
        st.subheader("导出置信度分析")
        confidence_export = to_excel_bytes({
            "置信度分布": pd.concat(all_dist_dfs, ignore_index=True),
            "当前筛选明细": filtered_df[DISPLAY_COLS],
        })
        st.download_button(
            "下载置信度分析 Excel",
            data=confidence_export,
            file_name="智审平台置信度分析结果.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )


with st.expander("口径说明", expanded=False):
    st.markdown(
        """
        1. **免分析时间段**：先按每日固定时间段排除记录。  
        2. **去重规则**：按事件编号去重；同一事件编号优先保留`来源=实时`，若不存在实时则保留`来源=AI`，同来源多条时保留`操作时间`最新的一条。  
        3. **事件编号为空**：按单条记录参与分析，不与其他记录合并。  
        4. **事件总数**：去重并筛选后的事件数量。  
        5. **通过事件数**：`审核结果=通过`。  
        6. **人工驳回事件数**：`审核结果=驳回`且`来源=实时`。  
        7. **系统驳回事件数**：`审核结果=驳回`且`来源=AI`。  
        8. **未处理事件数**：`审核结果=—`。  
        9. **推送事件数**：`是否推送=是`。  
        10. **接收事件数**：`推送接口成功=是`。  
        11. **校验关系**：`通过 + 人工驳回 + 系统驳回 + 未处理 = 事件总数`。若不成立，通常说明存在异常审核结果值。  
        12. **置信度分析**：置信度会统一转换为 0-1 小数；如 `31.6%`、`31.6`、`0.316` 都会被识别为 31.6%。
        """
    )
