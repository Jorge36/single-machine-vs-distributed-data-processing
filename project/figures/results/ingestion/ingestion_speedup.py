import streamlit as st
import pandas as pd
import plotly.express as px  
import math

st.markdown("""
<style>
h1 {font-size:22px !important;}
h3 {font-size:16px !important;}
.big-subheader {font-size:22px !important;}
</style>
""", unsafe_allow_html=True)

color_map = {
    "Cloud environment - 1 node (AWS)": "#1f77b4",   # blue
    "Cloud environment - 2 nodes (AWS)": "#ff7f0e",  # orange
}

st.title("Speedup Analysis")

df = pd.read_csv("ingestion_execution_time_sec.csv")

df["slice_name"] = df["slice_path"].apply(lambda x: str(x).rstrip("/").split("/")[-1])

slice_order = ["1st-slice", "2nd-slice", "3rd-slice", "4th-slice"]

df["slice_label"] = df.apply(
    lambda row: f"{row['slice_name']} ({row['dataset_size_mib']:.0f} MiB)",
    axis=1
)

slice_order = ["1st-slice", "2nd-slice", "3rd-slice", "4th-slice"]
label_order = [
    df.loc[df["slice_name"] == s, "slice_label"].iloc[0]
    for s in slice_order
]

df["slice_label"] = pd.Categorical(df["slice_label"], categories=label_order, ordered=True)

# Pivot table: one row per slice, one column per configuration
pivot = df.pivot_table(
    index="slice_label",
    columns="operation-system",
    values="execution_time_sec",
).reset_index()


# Relative Speedup vs Single-machine

# Use single-machine as baseline
# For 1st slice: in-memory exists
# For larger slices: chunking exists
def get_single_machine_baseline(row):
    if pd.notna(row.get("in-memory - single-machine")):
        return row["in-memory - single-machine"]
    return row["chunking - single-machine"]

pivot["single_machine_baseline"] = pivot.apply(get_single_machine_baseline, axis=1)

rename_map = {
    "standard - 1 node": "Cloud environment - 1 node (AWS)",
    "standard - 2 nodes": "Cloud environment - 2 nodes (AWS)"
}

speedup_rows = []

for col in ["standard - 1 node", "standard - 2 nodes"]:
    if col in pivot.columns:
        temp = pivot[["slice_label", "single_machine_baseline", col]].copy()
        temp["configuration"] = rename_map.get(col, col)
        temp["relative_speedup"] = temp["single_machine_baseline"] / temp[col]
        speedup_rows.append(temp[["slice_label", "configuration", "relative_speedup"]])

df_speedup = pd.concat(speedup_rows)


fig_speedup = px.bar(
    df_speedup,
    x = "slice_label",
    y = "relative_speedup",
    color = "configuration",
    barmode = "group",
    text = "relative_speedup",
    title = "Relative Speedup Compared with Single-Machine Baseline Across Dataset Sizes",
    color_discrete_map = color_map,
    labels = {
        "slice_label": "Dataset Slice",
        "relative_speedup": "Relative Speedup",
        "configuration": "Configuration"
    }
)

fig_speedup.update_traces(
    texttemplate="%{text:.2f}x",
    textposition="outside"
)

max_speedup = df_speedup["relative_speedup"].max()
step = 0.5
y_line = math.ceil(max_speedup / step) * step

fig_speedup.add_hline(
    y = y_line,
    line_color = "rgba(255,255,255,0.1)"
)

fig_speedup.update_layout(
    yaxis_title = "Speedup Factor",
    xaxis_title = "Dataset Slice",
    legend_title = "Configuration",
    yaxis_range = [0, y_line]
)

st.plotly_chart(fig_speedup, use_container_width = True)

row = df[df["operation-system"] == "chunking - single-machine"].iloc[0]
chunk_size_str = str(int(row["chunksize"]))

st.info(
    "Speedup calculated against single‑machine baseline "
    "(in‑memory for 1st slice; chunking for larger slices, "
    f"chunk size = {chunk_size_str})\n\n"

    "Interpreting speedup:\n\n"
    "Speedup is calculated as the ratio between the execution time of the "
    "single-machine baseline and the execution time of the distributed configuration:\n\n"
    "speedup = baseline time / distributed time\n\n"
    "This means the value expresses how many times faster (or slower) the distributed "
    "system performs. For example, a speedup of 2 means the distributed setup is twice "
    "as fast, while values below 1 indicate slower performance than the baseline."
)

# Spark scale out speedup and efficiency

df_scaleout = pivot[["slice_label", "standard - 1 node", "standard - 2 nodes"]].copy()

df_scaleout["scaleout_speedup"] = (
    df_scaleout["standard - 1 node"] / df_scaleout["standard - 2 nodes"]
)

df_scaleout["scaleout_efficiency"] = df_scaleout["scaleout_speedup"] / 2

fig_eff = px.bar(
    df_scaleout,
    x = "slice_label",
    y = ["scaleout_speedup", "scaleout_efficiency"],
    barmode = "group",
    text_auto = ".2f",
    title = "Spark Scale-Out Performance (Speedup and Efficiency)",
    labels = {
        "slice_label": "Dataset Slice",
        "value": "Metric Value",
        "variable": "Metric"
    }
)

fig_eff.update_layout(
    xaxis_title = "Dataset Slice",
    yaxis_title = "Value",
    legend_title = "Metric"
)



st.plotly_chart(fig_eff, use_container_width = True)

st.info(
    "**Interpreting scale-out performance:**\n\n"
    "Scale-out speedup is calculated as:\n"
    "speedup = execution time (1 node) / execution time (2 nodes)\n\n"
    "This shows how much faster the system becomes when adding more nodes. "
    "A value greater than 1 indicates that using multiple nodes improves performance.\n\n"
    "Scale-out efficiency is calculated as:\n"
    "efficiency = speedup / number of nodes\n\n"
    "It measures how effectively the additional resources are used. "
    "An efficiency of 1 represents ideal linear scaling, while lower values "
    "indicate performance loss due to overhead such as communication, "
    "scheduling, and data movement."
)
