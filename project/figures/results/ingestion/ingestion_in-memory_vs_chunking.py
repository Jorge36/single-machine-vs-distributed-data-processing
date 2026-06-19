import streamlit as st
import pandas as pd
import plotly.express as px
import math 

st.subheader("Local Single‑Machine: Execution Time and Memory Usage")

df = pd.read_csv("ingestion_sm_summary_in-memory_vs_chunking.csv")

df["slice_name"] = df["slice_path"].apply(lambda x: x.rstrip("/").split("/")[-1])
df["slice_label"] = df.apply(
    lambda row: f"{row['slice_name']} ({row['dataset_size_mib']:.0f} MiB)",
    axis=1
)

df_local = df[df["operation-system"].isin([
    "in-memory - single-machine",
    "chunking - single-machine"
])].copy()

chunk_size = int(df_local.loc[df_local["operation-system"] == "chunking - single-machine", "chunksize"].iloc[0])

df_local["configuration"] = df_local["operation-system"].replace({
    "in-memory - single-machine": "Local single-machine (in-memory)",
    "chunking - single-machine": f"Local single-machine (chunk={chunk_size})"
})

slice_order = ["1st-slice", "2nd-slice", "3rd-slice", "4th-slice"]
label_order = [
    df_local.loc[df_local["slice_name"] == s, "slice_label"].iloc[0]
    for s in slice_order
]

df_local["slice_label"] = pd.Categorical(df_local["slice_label"], categories=label_order, ordered=True)
df_local = df_local.sort_values("slice_label")

color_map = {
    "Local single-machine (in-memory)": "#2ca02c",  # green
    f"Local single-machine (chunk={chunk_size})": "#d62728"  # red
}


st.markdown("### Average Memory Usage (RSS)")

fig_mem = px.bar(
    df_local,
    x = "slice_label",
    y = "avg_rss_mib_average",
    color = "configuration",
    barmode = "group",
    title = "Average RSS Memory Usage",
    color_discrete_map = color_map,
    labels = {
            "slice_label": "Dataset Slice",
            "avg_rss_mib_average": "Average RSS (MiB)",
            "configuration": "Configuration"
    },
    text = "avg_rss_mib_average"
)


fig_mem.update_traces(
    texttemplate = "%{text:.0f}",   
    textposition = "outside"     
)

fig_mem.update_layout(
    xaxis_title = "Dataset Slice",
    yaxis_title = "Average RSS (MiB)",
    legend_title = "Configuration",
)

max_val = df_local["avg_rss_mib_average"].max()

y_line = math.ceil(max_val / 1000.0) * 1000

fig_mem.add_hline(
    y = y_line,
    line_color="rgba(255,255,255,0.1)"
)

st.plotly_chart(fig_mem, use_container_width = True)


st.markdown("### Average Execution Time")

fig_time = px.line(
    df_local,
    x="slice_label",
    y="execution_time_sec_average",
    color="configuration",
    markers=True,
    symbol="configuration",
    title="Average Execution Time Across Dataset Sizes",
    color_discrete_map=color_map,
    labels={
        "slice_label": "Dataset Slice",
        "execution_time_sec_average": "Average Execution Time (seconds)",
        "configuration": "Configuration"
    }
)

fig_time.update_traces(marker=dict(size=10))

fig_time.update_layout(
    xaxis_title="Dataset Slice",
    yaxis_title="Average Execution Time (seconds)",
    legend_title="Configuration"
)

max_val = df_local["execution_time_sec_average"].max()

y_line = math.ceil(max_val / 1000.0) * 1000

fig_time.add_hline(
    y = y_line,
    line_color="rgba(255,255,255,0.1)"
)

st.plotly_chart(fig_time, use_container_width=True)
