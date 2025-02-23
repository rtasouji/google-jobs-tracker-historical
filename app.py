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

# ✅ Ensure Table Exists Before Querying
def initialize_database():
    conn = get_db_connection()
    cursor = conn.cursor()

    # ✅ Create table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS share_of_voice (
            id SERIAL PRIMARY KEY,
            domain TEXT NOT NULL,
            sov FLOAT NOT NULL,
            appearances INT DEFAULT 0,
            avg_v_rank FLOAT DEFAULT 0,
            avg_h_rank FLOAT DEFAULT 0,
            date DATE NOT NULL
        );
    """)

    # ✅ Add missing columns if they don’t exist
    cursor.execute("ALTER TABLE share_of_voice ADD COLUMN IF NOT EXISTS appearances INT DEFAULT 0;")
    cursor.execute("ALTER TABLE share_of_voice ADD COLUMN IF NOT EXISTS avg_v_rank FLOAT DEFAULT 0;")
    cursor.execute("ALTER TABLE share_of_voice ADD COLUMN IF NOT EXISTS avg_h_rank FLOAT DEFAULT 0;")

    conn.commit()
    cursor.close()
    conn.close()

# ✅ Run this function when the app starts
initialize_database()

# ✅ Load job queries from CSV
def load_jobs():
    file_path = "jobs.csv"

    if not os.path.exists(file_path):
        st.error(f"⚠️ File '{file_path}' not found! Please ensure it exists in the project folder.")
        return []

    df = pd.read_csv(file_path)
    return df.to_dict(orient="records")

# ✅ Fetch Google Jobs Results from SerpAPI
def get_google_jobs_results(query, location):
    SERP_API_KEY = os.getenv("SERP_API_KEY")  

    if not SERP_API_KEY:
        raise ValueError("❌ ERROR: SERP_API_KEY environment variable is not set!")

    url = "https://serpapi.com/search"
    params = {
        "engine": "google_jobs",
        "q": query,
        "location": location,
        "hl": "en",
        "api_key": SERP_API_KEY
    }
    
    response = requests.get(url, params=params)

    if response.status_code != 200:
        raise RuntimeError(f"❌ ERROR: Failed to fetch data from SerpAPI. Status Code: {response.status_code}")

    return response.json().get("jobs_results", [])

# ✅ Compute Share of Voice & Additional Metrics
def compute_sov():
    domain_sov = defaultdict(float)
    domain_appearances = defaultdict(int)
    domain_v_rank = defaultdict(list)
    domain_h_rank = defaultdict(list)

    jobs_data = load_jobs()
    total_sov = 0  

    for job_query in jobs_data:
        job_title = job_query["job_title"]
        location = job_query["location"]

        jobs = get_google_jobs_results(job_title, location)

        for job_rank, job in enumerate(jobs, start=1):
            apply_options = job.get("apply_options", [])

            V = 1 / job_rank  

            for link_order, option in enumerate(apply_options, start=1):
                if "link" in option:
                    domain = extract_domain(option["link"])
                    H = 1 / link_order  

                    weight = V * H  
                    domain_sov[domain] += weight  
                    domain_appearances[domain] += 1
                    domain_v_rank[domain].append(job_rank)
                    domain_h_rank[domain].append(link_order)
                    total_sov += weight  

    if total_sov > 0:
        domain_sov = {domain: round((sov / total_sov) * 100, 4) for domain, sov in domain_sov.items()}
    
    domain_avg_v_rank = {domain: round(sum(vr) / len(vr), 2) for domain, vr in domain_v_rank.items()}
    domain_avg_h_rank = {domain: round(sum(hr) / len(hr), 2) for domain, hr in domain_h_rank.items()}

    return domain_sov, domain_appearances, domain_avg_v_rank, domain_avg_h_rank

# ✅ Extract Domain from URL
def extract_domain(url):
    extracted = tldextract.extract(url)
    domain = f"{extracted.domain}.{extracted.suffix}" if extracted.suffix else extracted.domain
    return domain.lower().replace("www.", "")

# ✅ Store Data in Database
def save_to_db(sov_data, appearances, avg_v_rank, avg_h_rank):
    conn = get_db_connection()
    cursor = conn.cursor()

    today = datetime.date.today()

    for domain in sov_data:
        cursor.execute("""
            INSERT INTO share_of_voice (domain, sov, appearances, avg_v_rank, avg_h_rank, date)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (domain, round(sov_data[domain], 2), appearances[domain], avg_v_rank[domain], avg_h_rank[domain], today))

    conn.commit()
    cursor.close()
    conn.close()

# ✅ Retrieve Historical Data with Correct Table Layout
def get_historical_data(start_date, end_date):
    conn = get_db_connection()
    cursor = conn.cursor()

    query = """
        SELECT domain, date, sov, appearances, avg_v_rank, avg_h_rank
        FROM share_of_voice 
        WHERE date BETWEEN %s AND %s
    """
    cursor.execute(query, (start_date, end_date))
    rows = cursor.fetchall()

    df = pd.DataFrame(rows, columns=["domain", "date", "sov", "appearances", "avg_v_rank", "avg_h_rank"])
    cursor.close()
    conn.close()

    # ✅ Convert 'date' column to only show the date (no hours)
    df["date"] = pd.to_datetime(df["date"]).dt.date  

    # ✅ Pivot for the first table (SoV)
    df_sov = df.pivot(index="domain", columns="date", values="sov").fillna(0)

    # ✅ Pivot for the second table (Metrics under each date)
    df_metrics = df.pivot(index="domain", columns="date", values=["appearances", "avg_v_rank", "avg_h_rank"])
    
    return df_sov, df_metrics

# ✅ Streamlit UI
st.title("Google Jobs Share of Voice Tracker")

# ✅ Date Range Selector
st.sidebar.header("Date Range Selector")
start_date = st.sidebar.date_input("Start Date", datetime.date(2025, 2, 1))
end_date = st.sidebar.date_input("End Date", datetime.date(2025, 2, 28))

# ✅ Fetch & Store Data
if st.button("Fetch & Store Data"):
    sov_data, appearances, avg_v_rank, avg_h_rank = compute_sov()
    save_to_db(sov_data, appearances, avg_v_rank, avg_h_rank)
    st.success("Data stored successfully!")

# ✅ Show Historical Trends
st.write("### Share of Voice Over Time")
df_sov, df_metrics = get_historical_data(start_date, end_date)

if not df_sov.empty:
    top_domains = df_sov.iloc[:15]
    fig = go.Figure()

    for domain in top_domains.index:
        fig.add_trace(go.Scatter(
            x=top_domains.columns, 
            y=top_domains.loc[domain], 
            mode="markers+lines", 
            name=domain
        ))

    # ✅ Add "Show All" & "Hide All" buttons
    fig.update_layout(
        updatemenus=[{
            "buttons": [
                {"args": [{"visible": True}], "label": "Show All", "method": "update"},
                {"args": [{"visible": "legendonly"}], "label": "Hide All", "method": "update"}
            ],
            "direction": "right", "x": 1, "y": 1.15
        }],
        title="Share of Voice Over Time",
        hovermode="x unified",
    )

    st.plotly_chart(fig)
    st.write("#### Table of SoV Data")
    st.dataframe(df_sov)

    st.write("### Additional Metrics")
    st.dataframe(df_metrics)
