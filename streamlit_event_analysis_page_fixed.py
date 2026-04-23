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
    # 默认取第一个 sheet
    df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=xls.sheet_names[0])
    return df


def validate_columns(df: pd.DataFrame) -> List[str]:
    return [c for c in REQUIRED_COLUMNS if c not in df.columns]


def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # 标准化文本列
    for col in [
        "图片", "来源", "操作人", "摄像头编号", "事件编号", "任务ID",
        "违规类型", "审核结果", "AI免审", "是否推送", "推送接口成功"
    ]:
        if col in out.columns:
            out[col] = out[col].map(normalize_text)

    # 时间列
    out["操作时间_dt"] = pd.to_datetime(out["操作时间"], errors="coerce")
    out["日期"] = out["操作时间_dt"].dt.date
    out["时分秒"] = out["操作时间_dt"].dt.time

    # 数值列
    out["置信度_num"] = pd.to_numeric(out["置信度"], errors="coerce")

    # 帮助列
    out["事件编号_key"] = out["事件编号"].replace("", pd.NA)
    out["来源_norm"] = out["来源"].str.upper()
    out["审核结果_norm"] = out["审核结果"]
    out["操作人_norm"] = out["操作人"]
    out["摄像头编号_norm"] = out["摄像头编号"]

    return out


def in_time_ranges(t: time | None, ranges: List[Tuple[time, time]]) -> bool:
    if t is None or pd.isna(t):
        return False
    for start_t, end_t in ranges:
        if start_t <= end_t:
            if start_t <= t <= end_t:
                return True
        else:
            # 跨天区间，如 22:00 - 02:00
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


# ------------------------------
# Metrics
# ------------------------------
def calc_overview_metrics(df: pd.DataFrame) -> dict:
    # 事件总数：重复的事件编号只算一次；若事件编号为空，则按行算
    total_event_count = int(df["事件编号_key"].dropna().nunique() + df["事件编号_key"].isna().sum())

    # 通过事件数：审核结果=通过
    pass_count = int((df["审核结果_norm"] == "通过").sum())

    # 人工驳回事件数：审核结果=驳回 且 来源=实时
    manual_reject_count = int(
        ((df["审核结果_norm"] == "驳回") & (df["来源"] == "实时")).sum()
    )

    # 系统驳回事件数：审核结果=驳回 且 来源=AI
    system_reject_count = int(
        ((df["审核结果_norm"] == "驳回") & (df["来源_norm"] == "AI")).sum()
    )

    # 未处理事件数：审核结果=—
    pending_count = int((df["审核结果_norm"] == "—").sum())

    # 推送事件数：是否推送=是
    push_count = int((df["是否推送"] == "是").sum())

    # 接收事件数：推送接口成功=是
    receive_count = int((df["推送接口成功"] == "是").sum())

    status_sum = pass_count + manual_reject_count + system_reject_count + pending_count
    is_balanced = (status_sum == total_event_count)

    return {
        "事件总数": total_event_count,
        "通过事件数": pass_count,
        "人工驳回事件数": manual_reject_count,
        "系统驳回事件数": system_reject_count,
        "未处理事件数": pending_count,
        "推送事件数": push_count,
        "接收事件数": receive_count,
        "状态分类合计": status_sum,
        "状态分类是否平衡": "是" if is_balanced else "否",
    }


def build_daily_stats(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["日期", "事件总数", "通过事件数", "人工驳回事件数", "系统驳回事件数", "未处理事件数", "推送事件数", "接收事件数", "状态分类合计", "状态分类是否平衡"])

    rows = []
    for d, g in df.groupby("日期", dropna=False):
        m = calc_overview_metrics(g)
        rows.append({"日期": d, **m})

    out = pd.DataFrame(rows).sort_values("日期")
    return out


def build_event_type_stats(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["违规类型", "事件数", "通过事件数", "人工驳回事件数", "系统驳回事件数", "未处理事件数", "推送事件数", "接收事件数", "状态分类合计", "状态分类是否平衡"])

    rows = []
    for event_type, g in df.groupby("违规类型", dropna=False):
        event_count = int(g["事件编号_key"].dropna().nunique() + g["事件编号_key"].isna().sum())
        rows.append({
            "违规类型": event_type or "（空）",
            "事件数": event_count,
            "通过事件数": int(((g["审核结果_norm"] == "通过") & (g["来源_norm"] != "AI")).sum()),
            "人工驳回事件数": int(((g["审核结果_norm"] == "驳回") & (g["来源"] == "实时")).sum()),
            "系统驳回事件数": int((g[(g["审核结果_norm"] == "驳回") & (g["来源_norm"] == "AI")]["事件编号_key"].dropna().nunique()) + g[(g["审核结果_norm"] == "驳回") & (g["来源_norm"] == "AI")]["事件编号_key"].isna().sum()),
            "推送事件数": int((g["是否推送"] == "是").sum()),
            "接收事件数": int((g["推送接口成功"] == "是").sum()),
        })

    return pd.DataFrame(rows).sort_values(["事件数", "违规类型"], ascending=[False, True])


def build_camera_stats(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["摄像头编号", "事件数", "通过事件数", "人工驳回事件数", "系统驳回事件数", "未处理事件数", "推送事件数", "接收事件数", "状态分类合计", "状态分类是否平衡"])

    rows = []
    for camera, g in df.groupby("摄像头编号_norm", dropna=False):
        event_count = int(g["事件编号_key"].dropna().nunique() + g["事件编号_key"].isna().sum())
        rows.append({
            "摄像头编号": camera or "（空）",
            "事件数": event_count,
            "通过事件数": int(((g["审核结果_norm"] == "通过") & (g["来源_norm"] != "AI")).sum()),
            "人工驳回事件数": int(((g["审核结果_norm"] == "驳回") & (g["来源"] == "实时")).sum()),
            "系统驳回事件数": int((g[(g["审核结果_norm"] == "驳回") & (g["来源_norm"] == "AI")]["事件编号_key"].dropna().nunique()) + g[(g["审核结果_norm"] == "驳回") & (g["来源_norm"] == "AI")]["事件编号_key"].isna().sum()),
            "推送事件数": int((g["是否推送"] == "是").sum()),
            "接收事件数": int((g["推送接口成功"] == "是").sum()),
        })

    return pd.DataFrame(rows).sort_values(["事件数", "摄像头编号"], ascending=[False, True])


def to_excel_bytes(named_dfs: dict[str, pd.DataFrame]) -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        for sheet_name, xdf in named_dfs.items():
            safe_name = sheet_name[:31]
            xdf.to_excel(writer, index=False, sheet_name=safe_name)
    return buffer.getvalue()


# ------------------------------
# UI
# ------------------------------
st.title("天天智审审核历史分析")
st.caption("支持导入事件列表 Excel，设置免分析时间段，按日期 / 摄像头 / 操作人筛选，并自动生成数据概要与统计明细。")


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
    st.dataframe(df[list(REQUIRED_COLUMNS.keys())].head(50), use_container_width=True, height=320)


with st.container(border=True):
    st.subheader("2. 免分析时间段")
    st.caption("落在以下时段内的事件，将被排除出分析范围。支持增加多段时间；若开始时间晚于结束时间，按跨天处理。")

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


# 先应用免分析时间段
range_filtered_df = apply_exclusion_ranges(df, edited_ranges)

with st.container(border=True):
    st.subheader("3. 筛选栏")

    min_date = range_filtered_df["日期"].dropna().min()
    max_date = range_filtered_df["日期"].dropna().max()

    c1, c2, c3 = st.columns([1.2, 1.2, 1])
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

    camera_options = sorted([x for x in range_filtered_df["摄像头编号_norm"].dropna().astype(str).unique().tolist() if x != ""])
    operator_options = sorted([x for x in range_filtered_df["操作人_norm"].dropna().astype(str).unique().tolist() if x != ""])

    with c2:
        selected_cameras = st.multiselect("摄像头（可多选）", options=camera_options, default=[])
    with c3:
        selected_operators = st.multiselect("操作人（可多选）", options=operator_options, default=[])


filtered_df = range_filtered_df.copy()

# 日期过滤
if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
    filtered_df = filtered_df[
        filtered_df["日期"].between(start_date, end_date, inclusive="both")
    ].copy()

# 摄像头过滤
if selected_cameras:
    filtered_df = filtered_df[filtered_df["摄像头编号_norm"].isin(selected_cameras)].copy()

# 操作人过滤
if selected_operators:
    filtered_df = filtered_df[filtered_df["操作人_norm"].isin(selected_operators)].copy()


with st.container(border=True):
    st.subheader("4. 数据概要")
    metrics = calc_overview_metrics(filtered_df)

    m1, m2, m3, m4 = st.columns(4)
    m5, m6, m7, m8 = st.columns(4)
    m1.metric("事件总数", f"{metrics['事件总数']:,}")
    m2.metric("通过事件数", f"{metrics['通过事件数']:,}")
    m3.metric("人工驳回事件数", f"{metrics['人工驳回事件数']:,}")
    m4.metric("系统驳回事件数", f"{metrics['系统驳回事件数']:,}")
    m5.metric("未处理事件数", f"{metrics['未处理事件数']:,}")
    m6.metric("推送事件数", f"{metrics['推送事件数']:,}")
    m7.metric("接收事件数", f"{metrics['接收事件数']:,}")
    m8.metric("状态分类合计", f"{metrics['状态分类合计']:,}")

    if metrics["状态分类是否平衡"] == "是":
        st.success(
            f"口径校验通过：通过 + 人工驳回 + 系统驳回 + 未处理 = {metrics['状态分类合计']:,}，与事件总数一致。"
        )
    else:
        st.error(
            f"口径校验未通过：通过 + 人工驳回 + 系统驳回 + 未处理 = {metrics['状态分类合计']:,}，"
            f"事件总数 = {metrics['事件总数']:,}。"
        )

    st.caption(
        f"当前参与分析的原始记录数：{len(filtered_df):,} 条；"
        f"排除免分析时间段后的记录数：{len(range_filtered_df):,} 条；"
        f"原始导入总记录数：{len(df):,} 条。"
    )


# 明细统计
st.markdown("---")
st.subheader("统计分析")

daily_stats = build_daily_stats(filtered_df)
event_type_stats = build_event_type_stats(filtered_df)
camera_stats = build_camera_stats(filtered_df)


tab1, tab2, tab3, tab4 = st.tabs(["按日统计", "按违规类型统计", "按摄像头统计", "明细数据"])

with tab1:
    st.dataframe(daily_stats, use_container_width=True, height=360)
    if not daily_stats.empty:
        chart_df = daily_stats.copy()
        chart_df = chart_df.set_index("日期")
        st.line_chart(chart_df[["事件总数", "通过事件数", "人工驳回事件数", "系统驳回事件数"]], height=320)

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
    display_cols = [
        "图片", "来源", "操作时间", "操作人", "摄像头编号", "事件编号",
        "任务ID", "置信度", "违规类型", "审核结果", "AI免审", "是否推送", "推送接口成功"
    ]
    st.dataframe(filtered_df[display_cols], use_container_width=True, height=500)


st.markdown("---")
st.subheader("导出结果")
export_bytes = to_excel_bytes({
    "筛选后明细": filtered_df[[
        "图片", "来源", "操作时间", "操作人", "摄像头编号", "事件编号",
        "任务ID", "置信度", "违规类型", "审核结果", "AI免审", "是否推送", "推送接口成功"
    ]],
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
        1. **事件总数**：按“事件编号”去重统计；若事件编号为空，则按行计数。  
        2. **通过事件数**：`审核结果=通过` 且 `来源!=AI`。  
        3. **人工驳回事件数**：`审核结果=驳回` 且 `来源=实时`。  
        4. **系统驳回事件数**：`审核结果=驳回` 且 `来源=AI`，并按“事件编号”去重统计。  
        5. **推送事件数**：`是否推送=是`。  
        6. **接收事件数**：`推送接口成功=是`。  
        7. **免分析时间段**：按每日固定时间段排除，不区分具体日期。  
        """
    )
