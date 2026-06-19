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

df = pd.read_csv("queries_execution_time_sec_q2.csv")

df["configuration"] = df["operation"].replace({
    "DuckDB": "DuckDB (4 threads, assigned by default)",
    "Spark SQL - node 1": "Cloud Environment - 1 node (AWS, Spark SQL)",
    "Spark SQL - node 2": "Cloud Environment - 2 nodes (AWS, Spark SQL)"
})

color_map = {
    "DuckDB (4 threads, assigned by default)": "#2ca02c",
    "Cloud Environment - 1 node (AWS, Spark SQL)": "#1f77b4",
    "Cloud Environment - 2 nodes (AWS, Spark SQL)": "#ff7f0e",
}

slice_order = ["1st-slice", "2nd-slice", "3rd-slice", "4th-slice"]

df["slice_label"] = df.apply(
    lambda row: f"{row['slice_name']} ({row['total_rows_in_dataset'] / 1_000_000:.1f}M rows)",
    axis=1
)

label_order = [
    df[df["slice_name"] == s]["slice_label"].iloc[0]
    for s in slice_order
]

fig_time = px.bar(
    df,
    x="slice_label",
    y="execution_time_sec",
    color="configuration",
    barmode="group",
    text="execution_time_sec",
    color_discrete_map=color_map,
    category_orders={"slice_label": label_order},
    title="Execution Time Values Across Dataset Sizes<br>(Query 2: Most Valuable Routes Query)",
    labels={
        "slice_label": "Dataset Slice",
        "execution_time_sec": "Execution Time (seconds)",
        "configuration": "Configuration"
    }
)

fig_time.update_traces(
    texttemplate="%{text:.1f}",
    textposition="outside"
)

fig_time.update_layout(
    xaxis_title="Dataset Slice",
    yaxis_title="Execution Time (seconds)",
    legend_title="Configuration"
)

max_val = df["execution_time_sec"].max()
upper = math.ceil(max_val / 20) * 20 + 10
fig_time.update_yaxes(range=[0, upper])

st.plotly_chart(fig_time, use_container_width=True)

st.code("""
Query 2:
SELECT pickup_location_id,
       dropoff_location_id,
       COUNT(*) AS trip_count,
       AVG(trip_distance) AS avg_trip_distance,
       trip_distance_unit,
       AVG(total_amount) AS avg_total_amount,
       SUM(total_amount) AS sum_total_amount,
       currency
FROM parquet.`{parquet_path}`
GROUP BY pickup_location_id,
         dropoff_location_id,
         trip_distance_unit,
         currency
HAVING COUNT(*) > 100
ORDER BY sum_total_amount DESC,
         trip_count DESC
LIMIT 100
""", language="sql")