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

# ✅ Database Connection
def get_db_connection():
    return psycopg2.connect(DB_URL, sslmode="require")

# ✅ Ensure Both Tables Exist Before Querying
def initialize_database():
    conn = get_db_connection()
    cursor = conn.cursor()

    # ✅ Keep existing `share_of_voice` table intact
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS share_of_voice (
            id SERIAL PRIMARY KEY,
            domain TEXT NOT NULL,
            sov FLOAT NOT NULL,
            date DATE NOT NULL
        );
    """)

    # ✅ New `domain_metrics` table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS domain_metrics (
            id SERIAL PRIMARY KEY,
            domain TEXT NOT NULL,
            total_appearances INT NOT NULL,
            avg_vertical_rank FLOAT NOT NULL,
            avg_horizontal_rank FLOAT NOT NULL,
            date DATE NOT NULL
        );
    """)

    conn.commit()
    cursor.close()
    conn.close()

# ✅ Initialize Tables
initialize_database()

# ✅ Extract Domain from URL
def extract_domain(url):
    extracted = tldextract.extract(url)
    domain = f"{extracted.domain}.{extracted.suffix}" if extracted.suffix else extracted.domain
    return domain.lower().replace("www.", "")  # ✅ Standardized domain format

# ✅ Fetch Google Jobs Results
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

# ✅ Compute SoV & New Metrics
def compute_sov():
    domain_sov = defaultdict(float)
    domain_appearances = defaultdict(int)
    domain_vranks = defaultdict(list)
    domain_hranks = defaultdict(list)
    jobs_data = load_jobs()
    total_sov = 0

    for job_query in jobs_data:
        job_title = job_query["job_title"]
        location = job_query["location"]
        jobs = get_google_jobs_results(job_title, location)

        for job_rank, job in enumerate(jobs, start=1):
            apply_options = job.get("apply_options", [])

            V = 1 / job_rank  # Vertical weight

            for link_order, option in enumerate(apply_options, start=1):
                if "link" in option:
                    domain = extract_domain(option["link"])
                    H = 1 / link_order  # Horizontal weight

                    weight = V * H
                    domain_sov[domain] += weight
                    domain_appearances[domain] += 1
                    domain_vranks[domain].append(job_rank)
                    domain_hranks[domain].append(link_order)
                    total_sov += weight

    # ✅ Normalize SoV to ensure the total sum is 100%
    if total_sov > 0:
        domain_sov = {domain: round((sov / total_sov) * 100, 4) for domain, sov in domain_sov.items()}

    # ✅ Compute averages
    domain_avg_vrank = {domain: sum(ranks) / len(ranks) for domain, ranks in domain_vranks.items()}
    domain_avg_hrank = {domain: sum(ranks) / len(ranks) for domain, ranks in domain_hranks.items()}

    return domain_sov, domain_appearances, domain_avg_vrank, domain_avg_hrank

# ✅ Store Data in Database
def save_to_db(domain_sov, domain_appearances, domain_avg_vrank, domain_avg_hrank):
    conn = get_db_connection()
    cursor = conn.cursor()
    today = datetime.date.today()

    for domain, sov in domain_sov.items():
        cursor.execute("INSERT INTO share_of_voice (domain, sov, date) VALUES (%s, %s, %s)",
                       (domain, round(sov, 2), today))

    for domain in domain_appearances:
        cursor.execute("""
            INSERT INTO domain_metrics (domain, total_appearances, avg_vertical_rank, avg_horizontal_rank, date)
            VALUES (%s, %s, %s, %s, %s);
        """, (domain, domain_appearances[domain], domain_avg_vrank[domain], domain_avg_hrank[domain], today))

    conn.commit()
    cursor.close()
    conn.close()

# ✅ Streamlit UI
st.title("Google Jobs Share of Voice Tracker")

st.sidebar.header("Date Range Selector")
start_date = st.sidebar.date_input("Start Date", datetime.date(2025, 2, 1))
end_date = st.sidebar.date_input("End Date", datetime.date(2025, 2, 28))

if st.button("Fetch & Store Data"):
    domain_sov, domain_appearances, domain_avg_vrank, domain_avg_hrank = compute_sov()
    save_to_db(domain_sov, domain_appearances, domain_avg_vrank, domain_avg_hrank)
    st.success("Data stored successfully!")

# ✅ Show Share of Voice Table
st.write("### Share of Voice Over Time")
df_sov = get_historical_data(start_date, end_date)
if not df_sov.empty:
    st.dataframe(df_sov.style.format("{:.2f}"))

# ✅ Show New Metrics Table
st.write("### Domain Performance Metrics")
conn = get_db_connection()
df_metrics = pd.read_sql("""
    SELECT domain, date, total_appearances, avg_vertical_rank, avg_horizontal_rank
    FROM domain_metrics
    WHERE date BETWEEN %s AND %s
""", conn, params=(start_date, end_date))
conn.close()

if not df_metrics.empty:
    st.dataframe(df_metrics.style.format("{:.2f}"))
else:
    st.write("No metrics data available for the selected date range.")

