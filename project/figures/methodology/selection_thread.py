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

st.title("Thread Comparison: 0 vs 3")#

df = pd.read_csv("../../single-machine/results_4VCPUs_8GB/persistence/csv/persistence_sm_summary.csv")


df["slice"] = df["slice_path"].str.extract(r"(1st-slice|2nd-slice|3rd-slice|4th-slice)")

df = df[~df["slice"].str.contains("4th-slice")]

def make_label(row):
    if row["operation"] == "chunking":
        return f"{row['slice']}\n(chunking)\n (chunksize={row['chunksize']})"
    else:
        return f"{row['slice']}\n({row['operation']})"

df["label"] = df.apply(make_label, axis=1)


stage = df["stage"].iloc[0]

st.subheader(f"Stage: {stage.capitalize()}")

col1, col2 = st.columns(2)

with col1:

    pivot_time = df.pivot_table(
        index="label",
        columns="threads",
        values="execution_time_sec_average",
    )

    fig, ax = plt.subplots(figsize=(5, 3))
    pivot_time.plot(kind="bar", ax=ax)

    ax.set_xlabel("Dataset slice/Operation", fontsize=8)
    ax.set_ylabel("Average Execution time (s)", fontsize=8)
    ax.set_title("Average Execution Time by Thread Count", fontsize=9)
    ax.tick_params(axis="both", labelsize=8)
    ax.tick_params(axis='x', labelrotation=45) 
    ax.legend(title="Threads", fontsize=7, title_fontsize=8)

    st.pyplot(fig)

with col2:

    pivot_mem = df.pivot_table(
        index="label",
        columns="threads",
        values="avg_rss_mib_average",
    )

    fig, ax = plt.subplots(figsize=(5, 3))
    pivot_mem.plot(kind="bar", ax=ax)

    ax.set_xlabel("Dataset slice/Operation", fontsize=8)
    ax.set_ylabel("Average Memory Usage (MiB)", fontsize=8)
    ax.set_title("Average Memory Usage by Thread Count", fontsize=9)
    ax.tick_params(axis="both", labelsize=8)
    ax.tick_params(axis='x', labelrotation=45) 
    ax.legend(title="Threads", fontsize=7, title_fontsize=8)

    st.pyplot(fig)