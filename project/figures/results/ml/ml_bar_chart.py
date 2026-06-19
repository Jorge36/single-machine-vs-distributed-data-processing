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

st.title("Execution Time per Configuration Across Dataset Slices")

df = pd.read_csv("ml_execution_time_sec.csv")

st.markdown(
    f'<h3 class="big-subheader">Stage: {str(df["stage"].iloc[0]).capitalize()}</h3>',
    unsafe_allow_html=True
)

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
        else row["operation"]
    ),
    axis=1
)

table_df = df[[
    "configuration",
    "slice_name",
    "total_rows_used_ml",
    "execution_time_sec"
]]

table_df.columns = [
    "Configuration",
    "Dataset Slice",
    "Rows Used",
    "Execution Time (sec)"
]

table_df["Rows Used"] = table_df["Rows Used"].apply(
    lambda x: f"{x / 1_000_000:.1f}M"
)

table_df["Execution Time (sec)"] = table_df[
    "Execution Time (sec)"
].round(1)

st.subheader("Machine Learning Execution Results")

st.dataframe(
    table_df,
    use_container_width=True,
    hide_index=True,
    height=420
)