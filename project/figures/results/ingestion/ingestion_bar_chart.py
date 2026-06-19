import streamlit as st
import pandas as pd
import plotly.express as px

st.markdown("""
<style>
h1 {font-size:22px !important;}
h3 {font-size:16px !important;}
.big-subheader {font-size:22px !important;}
</style>
""", unsafe_allow_html=True)

st.title("Execution Time per Configuration Across Dataset Slices")

df = pd.read_csv("ingestion_execution_time_sec.csv")

st.markdown(
    f'<h3 class="big-subheader">Stage: {str(df["stage"].iloc[0]).capitalize()}</h3>',
    unsafe_allow_html=True
)

# Extract slice name
df["slice_name"] = df["slice_path"].apply(lambda x: str(x).rstrip("/").split("/")[-1])

df["slice_label"] = df.apply(
    lambda row: f"{row['slice_name']} ({row['dataset_size_mib']:.0f} MiB)",
    axis=1
)

df["configuration"] = df["operation-system"].astype(str)

df["configuration"] = df["configuration"].replace({
    "in-memory - single-machine": "Local single-machine (in-memory)",
    "standard - 1 node": "Cloud environment - 1 node (AWS)",
    "standard - 2 nodes": "Cloud environment - 2 nodes (AWS)"
})

df.loc[df["operation-system"] == "chunking - single-machine", "configuration"] = \
    "Local single-machine (chunk=" + df.loc[df["operation-system"] == "chunking - single-machine", "chunksize"].astype(int).astype(str) + ")"
    
    
chunk_size = int(df.loc[df["operation-system"] == "chunking - single-machine", "chunksize"].iloc[0])

chunk_label = f"Local single-machine (chunk={chunk_size})"

color_map = {
    "Cloud environment - 1 node (AWS)": "#1f77b4",   # blue
    "Cloud environment - 2 nodes (AWS)": "#ff7f0e",  # orange
    "Local single-machine (in-memory)": "#2ca02c",  # green
    chunk_label: "#d62728"  # red
}

slice_order = ["1st-slice", "2nd-slice", "3rd-slice", "4th-slice"]
label_order = [
    df.loc[df["slice_name"] == slice_name, "slice_label"].iloc[0]
    for slice_name in slice_order
]

df["slice_label"] = pd.Categorical(df["slice_label"], categories = label_order, ordered = True)
df = df.sort_values("slice_label")

st.subheader("Execution Time Values by Configuration and Dataset Size")

fig_time = px.bar(
    df,
    x = "slice_label",
    y = "execution_time_sec",
    color = "configuration",
    barmode = "group",
    text = "execution_time_sec",
    title = "Execution Time Values Across Dataset Sizes",
    color_discrete_map = color_map,
    labels = {
        "slice_name": "Dataset Slice",
        "execution_time_sec": "Execution Time (seconds)",
        "configuration": "Configuration"
    }
)

fig_time.update_traces(
    texttemplate = "%{text:.1f}",
    textposition = "outside"
)

fig_time.update_layout(
    xaxis_title="Dataset Size",
    yaxis_title="Execution Time (seconds)",
    legend_title="Configuration",
    bargap=0.25,
    uniformtext_minsize=8,
    uniformtext_mode="show"
)

st.plotly_chart(fig_time, use_container_width=True)