import streamlit as st
import pandas as pd

st.markdown("""
<style>
h1 {font-size:22px !important;}
h2 {font-size:18px !important;}
h3 {font-size:16px !important;}
</style>
""", unsafe_allow_html=True)

st.title("Cold-Start vs Clear Cache Conditions")

df = pd.read_csv("../../single-machine/results_4VCPUs_8GB/cleaning/csv/cleaning_sm.csv")

df = df[(df["test_number"] >= 102) & (df["test_number"] <= 105)]

df["slice"] = df["slice_path"].str.extract(r"(1st-slice|2nd-slice|3rd-slice|4th-slice)")
stage = df["stage"].iloc[0]
slice = df["slice"].iloc[0]

st.subheader(f"Stage: {stage.capitalize()} - Slice: {slice}")

# Columns you want
selected_cols = [
    "operation",
    "processed_rows",
    "execution_time_sec",
    "chunksize",
    "benchmark_start_timestamp"
]

# Keep only those columns (only if they exist)
df = df[[col for col in selected_cols if col in df.columns]]

st.dataframe(df.reset_index(drop=True), use_container_width=True)