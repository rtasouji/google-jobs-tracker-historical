import streamlit as st
import requests
import pandas as pd
import tldextract
import psycopg2
from collections import defaultdict
import datetime
import os
import plotly.graph_objects as go

DB_URL = os.getenv("DB_URL")  # ✅ Use os.environ for GitHub Actions

if not DB_URL:
    raise ValueError("❌ ERROR: DB_URL environment variable is not set!")

# ✅ Define Database Connection Function
def get_db_connection():
    return psycopg2.connect(DB_URL, sslmode="require")

# ✅ Retrieve Historical Data with Date Range Filter and Sorting
def get_historical_data(start_date, end_date):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'share_of_voice'
        );
    """)
    
    table_exists = cursor.fetchone()[0]
    
    if not table_exists:
        st.warning("⚠️ Table 'share_of_voice' does not exist. No data available yet.")
        cursor.close()
        conn.close()
        return pd.DataFrame(), pd.DataFrame()

    # Fetch data within the selected date range
    query = """
        SELECT domain, date, sov 
        FROM share_of_voice 
        WHERE date BETWEEN %s AND %s
    """
    cursor.execute(query, (start_date, end_date))
    rows = cursor.fetchall()

    # Convert to DataFrame
    df = pd.DataFrame(rows, columns=["domain", "date", "sov"])

    cursor.close()
    conn.close()

    # ✅ Convert 'date' column to only show the date (no hours)
    df["date"] = pd.to_datetime(df["date"]).dt.date  

    # ✅ Aggregate duplicate (domain, date) pairs by averaging their SoV
    df_sov = df.groupby(["domain", "date"], as_index=False).agg({"sov": "mean"})

    # ✅ Count total occurrences of each domain per date
    df_count = df.groupby(["domain", "date"], as_index=False).size()

    # ✅ Pivot both tables: Domains as rows, Dates as columns
    pivot_sov = df_sov.pivot(index="domain", columns="date", values="sov").fillna(0)
    pivot_count = df_count.pivot(index="domain", columns="date", values="size").fillna(0).astype(int)

    # ✅ Sort both tables by the most recent date’s values (if data exists)
    if not pivot_sov.empty:
        most_recent_date = pivot_sov.columns[-1]  # Get the most recent date
        pivot_sov = pivot_sov.sort_values(by=most_recent_date, ascending=False)

    if not pivot_count.empty:
        most_recent_date = pivot_count.columns[-1]  # Get the most recent date
        pivot_count = pivot_count.sort_values(by=most_recent_date, ascending=False)

    return pivot_sov, pivot_count

# ✅ Streamlit UI
st.title("Google Jobs Share of Voice Tracker")

# ✅ Date Range Selector
st.sidebar.header("Date Range Selector")
start_date = st.sidebar.date_input("Start Date", datetime.date(2025, 2, 1))
end_date = st.sidebar.date_input("End Date", datetime.date(2025, 2, 28))

# ✅ Show Historical Trends
st.write("### Share of Voice Over Time")
df_sov, df_count = get_historical_data(start_date, end_date)

if not df_sov.empty:
    # ✅ Limit to Top 15 domains to keep the chart clean
    top_domains = df_sov.iloc[:15]

    # ✅ Create a Plotly figure manually
    fig = go.Figure()

    for domain in top_domains.index:
        fig.add_trace(go.Scatter(
            x=top_domains.columns, 
            y=top_domains.loc[domain], 
            mode="markers+lines", 
            name=domain
        ))

    # ✅ Add buttons for "Show All" and "Hide All"
    fig.update_layout(
        updatemenus=[
            {
                "buttons": [
                    {
                        "args": [{"visible": True}],  
                        "label": "Show All",  
                        "method": "update"
                    },
                    {
                        "args": [{"visible": "legendonly"}],  
                        "label": "Hide All",  
                        "method": "update"
                    }
                ],
                "direction": "right",
                "showactive": True,
                "x": 1,
                "xanchor": "right",
                "y": 1.15,
                "yanchor": "top",
            }
        ]
    )

    fig.update_layout(
        title="Share of Voice Over Time",
        xaxis=dict(
            title="Date",
            tickangle=45,
            tickformat="%Y-%m-%d"
        ),
        yaxis=dict(title="SoV (%)"),
        hovermode="x unified",  # ✅ Show only the hovered point's value
        margin=dict(l=40, r=40, t=40, b=40)
    )

    st.plotly_chart(fig)  # ✅ Display in Streamlit

    # ✅ Show the SoV Table
    st.write("#### Share of Voice (SoV) Table")
    st.dataframe(df_sov.style.format("{:.2f}"))  # ✅ Keep numbers formatted to 2 decimal places

    # ✅ Show the Appearances Table
    st.write("#### Domain Appearances Table")
    st.dataframe(df_count)  # ✅ Show raw count of domain appearances
else:
    st.write("No historical data available for the selected date range.")
