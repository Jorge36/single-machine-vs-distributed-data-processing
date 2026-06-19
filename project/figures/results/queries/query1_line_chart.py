import streamlit as st
import pandas as pd
import plotly.express as px
import math

st.title("Scalability Analysis")

df = pd.read_csv("queries_execution_time_sec_q1.csv")

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

slice_order = ["1st-slice", "2nd-slice", "3rd-slice", "4th-slice"]

label_order = [
    df[df["slice_name"] == s]["slice_label"].iloc[0]
    for s in slice_order
]


fig_time = px.line(
    df,
    x="slice_label",
    y="execution_time_sec",
    color="configuration",
    markers=True,
    symbol="configuration",
    color_discrete_map=color_map,
    category_orders={"slice_label": slice_order},
    title="Execution Time Scalability Across Dataset Sizes<br>(Query 1: Payment Aggregation Query by Year and Month)",
    labels={
        "slice_name": "Dataset Slice",
        "execution_time_sec": "Execution Time (seconds)",
        "configuration": "Configuration"
    }
)

fig_time.update_traces(marker=dict(size=10))

fig_time.update_layout(
    xaxis_title="Dataset Slice",
    yaxis_title="Execution Time (seconds)",
    legend_title="Configuration"
)

max_val = df["execution_time_sec"].max()
upper = math.ceil(max_val / 20) * 20

fig_time.update_yaxes(range=[0, upper])
fig_time.add_hline(y = upper, line_color = "#444")

st.plotly_chart(fig_time, use_container_width=True)

st.code("""
Query 1:
SELECT year, month, payment_type_id,
       COUNT(*) AS trip_count,
       AVG(trip_distance) AS avg_trip_distance,
       AVG(fare_amount) AS avg_fare_amount,
       AVG(tip_amount) AS avg_tip_amount,
       SUM(total_amount) AS sum_total_amount
FROM parquet.`{parquet_path}`
WHERE suspicious = FALSE
GROUP BY year, month, payment_type_id
ORDER BY year, month, payment_type_id
""", language="sql")