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


df = pd.read_csv("cleaning_execution_time_sec.csv")

st.markdown(
    f'<h3 class="big-subheader">Stage: {str(df["stage"].iloc[0]).capitalize()}</h3>',
    unsafe_allow_html=True
)

# Extract last part of path (slice name)
df["slice_name"] = df["slice_path"].apply(lambda x: x.rstrip("/").split("/")[-1])
df["slice_label"] = df.apply(
    lambda row: f"{row['slice_name']} ({row['dataset_size_mib']:.0f} MiB)",
    axis=1
)

# Create a plotting group so in-memory first slice and chunking later slices appear together
df["plot_group"] = df["operation-system"].replace({
    "in-memory - single-machine": "single-machine",
    "chunking - single-machine": "single-machine",
})

df["configuration"] = df.apply(
    lambda row: (
        "Local single-machine (in-memory)" if row["operation-system"] == "in-memory - single-machine"
        else f"Local single-machine (chunk={int(row['chunksize'])})" if row["operation-system"] == "chunking - single-machine"
        else "Cloud environment - 1 node (AWS)"
        if row["operation-system"] == "standard - 1 node"
        else "Cloud environment - 2 nodes (AWS)"
        if row["operation-system"] == "standard - 2 nodes"
        else row["operation-system"]
    ),
    axis=1
)

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


st.subheader("Execution Time by System Configuration and Dataset Size")

fig_time = px.line(
    df,
    x = "slice_label",            # dataset slices (e.g., slice1, slice2...)
    y = "execution_time_sec",
    color = "configuration",              # Pandas, Spark, DuckDB...
    markers = True,
    symbol = "configuration",
    title = "Execution Time Scalability Across Dataset Sizes",
    color_discrete_map = color_map,
    labels = {
        "slice_label": "Dataset Size (MiB)",
        "execution_time": "Execution Time (seconds)",
        "configuration": "Configuration"
    }
)

fig_time.update_traces(marker= dict(size = 10))

fig_time.update_layout(
    xaxis_title="Dataset Slice",
    yaxis_title="Execution Time (seconds)",
    legend_title="Configuration"
)

max_val = df["execution_time_sec"].max()
upper = math.ceil(max_val / 2000) * 2000

fig_time.update_yaxes(range = [0, upper])
fig_time.add_hline(y = upper, line_color = "#444")

st.plotly_chart(fig_time, use_container_width = True)