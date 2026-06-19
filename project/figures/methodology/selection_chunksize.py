import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

st.markdown("""
<style>
h1 {font-size:22px !important;}
h2 {font-size:18px !important;}
h3 {font-size:16px !important;}
</style>
""", unsafe_allow_html=True)

st.title("Chunk Size Benchmark Results")

df = pd.read_csv("../../single-machine/results_4VCPUs_8GB/cleaning/csv/cleaning_sm_summary.csv")

df = df.iloc[1:4]

stage = df["stage"].iloc[0]
operation = df["operation"].iloc[0]
df["slice"] = df["slice_path"].str.extract(r"(1st-slice|2nd-slice|3rd-slice|4th-slice)")
slice = df["slice"].iloc[0]

st.subheader(f"Stage: {stage.capitalize()} - Operation: {operation.capitalize()} - Slice: {slice}")

df["chunksize"] = df["chunksize"].astype(str)

col1, col2 = st.columns(2)

with col1:

    fig, ax = plt.subplots()
    ax.bar(df["chunksize"], df["execution_time_sec_average"])
    ax.set_xlabel("Chunk Size")
    ax.set_ylabel("Average Execution Time (seconds)")
    ax.set_title("Average Execution Time by Chunk Size")
        
    st.pyplot(fig)

with col2:

    fig, ax = plt.subplots()
    ax.bar(df["chunksize"], df["avg_rss_mib_average"])
    ax.set_xlabel("Chunk Size")
    ax.set_ylabel("Average Memory Usage (MiB)")
    ax.set_title("Average Memory Usage by Chunk Size")

    st.pyplot(fig)