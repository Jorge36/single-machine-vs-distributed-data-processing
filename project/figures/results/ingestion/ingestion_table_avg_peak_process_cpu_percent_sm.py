import streamlit as st
import pandas as pd

st.markdown("""
<style>
h1 {font-size:22px !important;}
h2 {font-size:18px !important;}
h3 {font-size:16px !important;}
</style>
""", unsafe_allow_html=True)

st.title("Average Process Peak CPU Percent")

df = pd.read_csv("../../../single-machine/results_4VCPUs_8GB/ingestion/csv/ingestion_sm_summary.csv")

df = df[(df["chunksize"] == 0) | (df["chunksize"] == 100000)]

df["slice"] = df["slice_path"].str.extract(r"(1st-slice|2nd-slice|3rd-slice|4th-slice)")
stage = df["stage"].iloc[0]
df = df.sort_values(["operation", "slice"], ascending = [False, True])

#st.subheader(f"Stage: {stage.capitalize()}")

# Columns you want
selected_cols = [
    "operation",
    "slice",
    "processed_rows",
    "chunksize",
    "peak_process_cpu_percent_average"
]

# Keep only those columns (only if they exist)
df = df[[col for col in selected_cols if col in df.columns]]

st.dataframe(df.reset_index(drop=True), use_container_width = True)