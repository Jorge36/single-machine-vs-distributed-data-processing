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

st.title("Scalability Analysis")

df = pd.read_csv("ml_execution_time_sec.csv")


df["configuration"] = df.apply(
    lambda row: (
        "Local single-machine (in-memory, 4 threads)"
        if row["operation - system"] == "in-memory - single-machine"

        else f"Local single-machine (chunk={int(row['chunksize'])}, 4 threads)"
        if row["operation - system"] == "chunking - single-machine"

        else "Cloud environment - 1 node (AWS)"
        if row["operation - system"] == "standard - 1 node"

        else "Cloud environment - 2 nodes (AWS)"
        if row["operation - system"] == "standard - 2 nodes"

        else row["operation - system"]
    ),
    axis=1
)

row_map = {
    "1st-slice": "1st-slice (3.6M rows)",
    "2nd-slice": "2nd-slice (50.0M rows)",
    "3rd-slice": "3rd-slice (104.5M rows)",
    "4th-slice": "4th-slice (633.4M rows)"
}

df["slice_label"] = df["slice_name"].map(row_map)

slice_order = ["1st-slice", "2nd-slice", "3rd-slice", "4th-slice"]

label_order = [
    df[df["slice_name"] == s]["slice_label"].iloc[0]
    for s in slice_order
    if not df[df["slice_name"] == s].empty
]


chunk_size = int(
    df.loc[df["operation - system"] == "chunking - single-machine", "chunksize"].iloc[0]
)

chunk_label = f"Local single-machine (chunk={chunk_size}, 4 threads)"

color_map = {
    "Local single-machine (in-memory, 4 threads)": "#2ca02c",
    chunk_label: "#d62728",
    "Cloud environment - 1 node (AWS)": "#1f77b4",
    "Cloud environment - 2 nodes (AWS)": "#ff7f0e",
}

fig_time = px.line(
    df,
    x="slice_label",
    y="execution_time_sec",
    color="configuration",
    markers=True,
    symbol="configuration",
    color_discrete_map=color_map,
    title="Execution Time Scalability Across Dataset Sizes",
    labels={
        "slice_label": "Dataset Slice",
        "execution_time_sec": "Execution Time (seconds)",
        "configuration": "Configuration"
    }
)

fig_time.update_traces(marker=dict(size=10))

fig_time.update_layout(
    xaxis_title="Dataset Slice",
    yaxis_title="Execution Time (seconds)",
    legend_title="Configuration"
)

max_val = df["execution_time_sec"].max()
upper = math.ceil(max_val / 2000) * 2000

fig_time.update_yaxes(range=[0, upper])

fig_time.add_hline(y = upper, line_color = "#444")
st.plotly_chart(fig_time, use_container_width=True)


table_df = df[[
    "configuration",
    "slice_name",
    "threads",
    "chunksize",
    "mae",
    "rmse",
    "r2"
]].copy()

table_df.columns = [
    "Configuration",
    "Dataset Slice",
    "Threads",
    "Chunk Size",
    "MAE",
    "RMSE",
    "R²"
]

table_df["Threads"] = (table_df["Threads"]
    .replace(0, "4 (default)")
    .fillna("")
)

table_df["Chunk Size"] = (
    table_df["Chunk Size"]
    .replace(0, "")
    .fillna("")
)

table_df["Chunk Size"] = table_df["Chunk Size"].apply(
    lambda x: f"{int(x):,}" if x != "" else ""
)

table_df["MAE"] = table_df["MAE"].round(3)
table_df["RMSE"] = table_df["RMSE"].round(3)
table_df["R²"] = table_df["R²"].round(3)

table_df["Configuration"] = table_df["Configuration"].replace({
    "Local single-machine (in-memory, 4 threads)": "Local (in-memory)",
    chunk_label: "Local (chunking)",
    "Cloud environment - 1 node (AWS)": "Cloud (1 node)",
    "Cloud environment - 2 nodes (AWS)": "Cloud (2 nodes)"
})

st.subheader("Machine Learning Evaluation Metrics")

st.dataframe(
    table_df,
    hide_index=True,
    use_container_width=True,
    height=420
)