import streamlit as st
import pandas as pd
import plotly.express as px

st.subheader("Local Single‑Machine: Percentage of total system RAM used by the process")

df = pd.read_csv("cleaning_memory.csv")

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

fig_mem = px.bar(
    df_local,
    x = "slice_label",
    y = "memory_percent_of_total",
    color = "configuration",
    barmode = "group",
    title = "Memory Usage as Percentage of Total System RAM",
    color_discrete_map = color_map,
    labels = {
            "slice_label": "Dataset Slice",
            "memory_percent_of_total": "Memory Usage (% of Total RAM)",
            "configuration": "Configuration"
    },
    text = "memory_percent_of_total"
)


fig_mem.update_traces(
    texttemplate = "%{text:.2f}%",   
    textposition = "outside"     
)

fig_mem.update_layout(
    xaxis_title = "Dataset Slice",
    yaxis_title = "Memory Usage (% of Total RAM)",
    legend_title = "Configuration",
)

fig_mem.update_yaxes(range=[0, df_local["memory_percent_of_total"].max() * 1.25])

st.plotly_chart(fig_mem, use_container_width = True)