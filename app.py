import streamlit as st
import requests
import pandas as pd
import tldextract
import psycopg2
from collections import defaultdict
import datetime
import os
import matplotlib.pyplot as plt

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
        return pd.DataFrame()

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

    # ✅ Aggregate duplicate (domain, date) pairs by averaging their SoV
    df = df.groupby(["domain", "date"], as_index=False).agg({"sov": "mean"})

    # ✅ Pivot the data
    pivot_df = df.pivot(index="domain", columns="date", values="sov")

    # ✅ Sort by the most recent date’s SoV values (if data exists)
    if not pivot_df.empty:
        most_recent_date = pivot_df.columns[-1]  # Get the most recent date
        pivot_df = pivot_df.sort_values(by=most_recent_date, ascending=False)

    return pivot_df

# ✅ Streamlit UI
st.title("Google Jobs Share of Voice Tracker")

# ✅ Date Range Selector
st.sidebar.header("Date Range Selector")
start_date = st.sidebar.date_input("Start Date", datetime.date(2025, 2, 1))
end_date = st.sidebar.date_input("End Date", datetime.date(2025, 2, 28))

# ✅ Show Historical Trends
st.write("### Share of Voice Over Time")
df_sov = get_historical_data(start_date, end_date)

if not df_sov.empty:
    # ✅ Limit to Top 10 domains to keep the chart clean
    df_sov = df_sov.iloc[:10]

    # ✅ Use Matplotlib for better visualization
    fig, ax = plt.subplots(figsize=(12, 6))

    for domain in df_sov.index:
        ax.plot(df_sov.columns, df_sov.loc[domain], marker='o', linestyle='-', label=domain)

    ax.set_title("Share of Voice Over Time")
    ax.set_xlabel("Date")
    ax.set_ylabel("SoV (%)")
    ax.legend(loc="upper right", fontsize="small")  # ✅ Smaller legend to fit more items
    ax.grid(True, linestyle="--", alpha=0.5)

    plt.xticks(rotation=45)  # ✅ Rotate x-axis labels for readability
    plt.tight_layout()

    # ✅ Display the Matplotlib chart in Streamlit
    st.pyplot(fig)

    # ✅ Show the DataFrame as well
    st.write("#### Table of SoV Data")
    st.dataframe(df_sov)
else:
    st.write("No historical data available for the selected date range.")
