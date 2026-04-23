# -*- coding: utf-8 -*-
from __future__ import annotations

import io
from datetime import datetime, time
from typing import List, Tuple

import pandas as pd
import streamlit as st


st.set_page_config(page_title="智审平台审核历史分析", layout="wide")


REQUIRED_COLUMNS = {
    "图片": "图片",
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
    "图片", "来源", "操作时间", "操作人", "摄像头编号", "事件编号",
    "任务ID", "置信度", "违规类型", "审核结果", "AI免审", "是否推送", "推送接口成功"
]


# ------------------------------
# Utils
# ------------------------------
def normalize_text(v) -> str:
    if pd.isna(v):
        return ""
    return str(v).strip()


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
        "图片", "来源", "操作人", "摄像头编号", "事件编号", "任务ID",
        "违规类型", "审核结果", "AI免审", "是否推送", "推送接口成功"
    ]
    for col in text_cols:
        out[col] = out[col].map(normalize_text)

    out["操作时间_dt"] = pd.to_datetime(out["操作时间"], errors="coerce")
    out["日期"] = out["操作时间_dt"].dt.date
    out["时分秒"] = out["操作时间_dt"].dt.time
    out["置信度_num"] = pd.to_numeric(out["置信度"], errors="coerce")

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
# Metrics
# ------------------------------
def calc_overview_metrics(df: pd.DataFrame) -> dict:
    total_event_count = int(len(df))
    pass_count = int((df["审核结果_norm"] == "通过").sum())
    manual_reject_count = int(((df["审核结果_norm"] == "驳回") & (df["来源"] == "实时")).sum())
    system_reject_count = int(((df["审核结果_norm"] == "驳回") & (df["来源"] == "AI")).sum())
    untreated_count = int((df["审核结果_norm"] == "—").sum())
    push_count = int((df["是否推送"] == "是").sum())
    receive_count = int((df["推送接口成功"] == "是").sum())

    return {
        "事件总数": total_event_count,
        "通过事件数": pass_count,
        "人工驳回事件数": manual_reject_count,
        "系统驳回事件数": system_reject_count,
        "未处理事件数": untreated_count,
        "推送事件数": push_count,
        "接收事件数": receive_count,
    }


def build_daily_stats(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["日期", "事件总数", "通过事件数", "人工驳回事件数", "系统驳回事件数", "未处理事件数", "推送事件数", "接收事件数"])

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


# ------------------------------
# UI
# ------------------------------
st.title("天天智审审核历史分析")
st.caption("先按免分析时间段排除，再按事件编号去重；去重后再进行筛选和统计。")

with st.container(border=True):
    st.subheader("1. Excel 导入")
    uploaded_file = st.file_uploader(
        "上传智审平台导出的事件列表 Excel",
        type=["xlsx", "xls"],
        accept_multiple_files=False,
    )
    use_sample = st.checkbox("没有文件时，使用当前样例文件演示", value=False)

if uploaded_file is None and not use_sample:
    st.info("请先上传 Excel 文件，或勾选“使用当前样例文件演示”。")
    st.stop()

if uploaded_file is not None:
    file_bytes = uploaded_file.getvalue()
    file_name = uploaded_file.name
else:
    sample_path = "/mnt/data/事件标记历史（无图）_18_22_sample.xlsx"
    with open(sample_path, "rb") as f:
        file_bytes = f.read()
    file_name = "事件标记历史（无图）_18_22_sample.xlsx"

raw_df = load_excel(file_bytes, file_name)
missing_cols = validate_columns(raw_df)
if missing_cols:
    st.error(f"Excel 缺少必要列：{', '.join(missing_cols)}")
    st.stop()

df = prepare_dataframe(raw_df)

with st.expander("查看源数据预览", expanded=False):
    st.write(f"文件名：{file_name}")
    st.write(f"原始行数：{len(df):,}")
    st.dataframe(df[DISPLAY_COLS].head(50), use_container_width=True, height=320)

with st.container(border=True):
    st.subheader("2. 免分析时间段")
    st.caption("落在以下时段内的记录，将被排除出分析范围。")
    default_ranges = pd.DataFrame([
        {"启用": True, "开始时间": time(0, 0), "结束时间": time(9, 30)},
        {"启用": True, "开始时间": time(12, 0), "结束时间": time(14, 0)},
        {"启用": True, "开始时间": time(20, 30), "结束时间": time(23, 59)},
    ])
    edited_ranges = st.data_editor(
        default_ranges,
        use_container_width=True,
        num_rows="dynamic",
        hide_index=True,
        column_config={
            "启用": st.column_config.CheckboxColumn("启用"),
            "开始时间": st.column_config.TimeColumn("开始时间", format="HH:mm"),
            "结束时间": st.column_config.TimeColumn("结束时间", format="HH:mm"),
        },
        key="exclude_time_ranges",
    )

range_filtered_df = apply_exclusion_ranges(df, edited_ranges)
dedup_df = deduplicate_events(range_filtered_df)

with st.container(border=True):
    st.subheader("3. 筛选栏")

    min_date = dedup_df["日期"].dropna().min()
    max_date = dedup_df["日期"].dropna().max()

    c1, c2, c3, c4 = st.columns([1.25, 1.1, 1.1, 1.1])
    with c1:
        if pd.notna(min_date) and pd.notna(max_date):
            date_range = st.date_input(
                "日期范围",
                value=(min_date, max_date),
                min_value=min_date,
                max_value=max_date,
            )
        else:
            date_range = ()
            st.date_input("日期范围", disabled=True)

    camera_options = sorted([x for x in dedup_df["摄像头编号_norm"].dropna().astype(str).unique().tolist() if x != ""])
    operator_options = sorted([x for x in dedup_df["操作人_norm"].dropna().astype(str).unique().tolist() if x != ""])
    event_type_options = sorted([x for x in dedup_df["违规类型_norm"].dropna().astype(str).unique().tolist() if x != ""])

    with c2:
        selected_cameras = st.multiselect("摄像头（可多选）", options=camera_options, default=[])
    with c3:
        selected_operators = st.multiselect("操作人（可多选）", options=operator_options, default=[])
    with c4:
        selected_event_types = st.multiselect("事件类型（可多选）", options=event_type_options, default=[])

filtered_df = dedup_df.copy()
if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
    filtered_df = filtered_df[filtered_df["日期"].between(start_date, end_date, inclusive="both")].copy()
if selected_cameras:
    filtered_df = filtered_df[filtered_df["摄像头编号_norm"].isin(selected_cameras)].copy()
if selected_operators:
    filtered_df = filtered_df[filtered_df["操作人_norm"].isin(selected_operators)].copy()
if selected_event_types:
    filtered_df = filtered_df[filtered_df["违规类型_norm"].isin(selected_event_types)].copy()

with st.container(border=True):
    st.subheader("4. 数据概要")
    metrics = calc_overview_metrics(filtered_df)

    m1, m2, m3, m4 = st.columns(4)
    m5, m6, m7 = st.columns(3)
    m1.metric("事件总数", f"{metrics['事件总数']:,}")
    m2.metric("通过事件数", f"{metrics['通过事件数']:,}")
    m3.metric("人工驳回事件数", f"{metrics['人工驳回事件数']:,}")
    m4.metric("系统驳回事件数", f"{metrics['系统驳回事件数']:,}")
    m5.metric("未处理事件数", f"{metrics['未处理事件数']:,}")
    m6.metric("推送事件数", f"{metrics['推送事件数']:,}")
    m7.metric("接收事件数", f"{metrics['接收事件数']:,}")

    status_sum = (
        metrics["通过事件数"]
        + metrics["人工驳回事件数"]
        + metrics["系统驳回事件数"]
        + metrics["未处理事件数"]
    )
    if status_sum == metrics["事件总数"]:
        st.success("当前口径校验通过：通过 + 人工驳回 + 系统驳回 + 未处理 = 事件总数")
    else:
        st.warning(
            f"当前口径校验未通过：四类状态合计 {status_sum:,}，事件总数 {metrics['事件总数']:,}。"
            "请检查是否存在异常审核结果。"
        )

    st.caption(
        f"原始导入总记录数：{len(df):,} 条；"
        f"排除免分析时间段后的记录数：{len(range_filtered_df):,} 条；"
        f"去重后的事件数：{len(dedup_df):,} 条；"
        f"当前筛选后的事件数：{len(filtered_df):,} 条。"
    )

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
        st.line_chart(chart_df[["事件总数", "通过事件数", "人工驳回事件数", "系统驳回事件数", "未处理事件数"]], height=320)

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
        """
    )
