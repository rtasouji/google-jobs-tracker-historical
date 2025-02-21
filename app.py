import streamlit as st
import requests
import pandas as pd
import tldextract
import psycopg2
from collections import defaultdict
import datetime
import os

# ✅ Securely Load Database URL from Streamlit Secrets
DB_URL = st.secrets["DB_URL"]

# ✅ Define Database Connection Function
def get_db_connection():
    return psycopg2.connect(DB_URL, sslmode="require")

# ✅ Ensure Table Exists Before Querying
def initialize_database():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS share_of_voice (
            id SERIAL PRIMARY KEY,
            domain TEXT NOT NULL,
            sov FLOAT NOT NULL,
            date DATE NOT NULL
        );
    """)

    conn.commit()
    cursor.close()
    conn.close()

# ✅ Initialize Database
initialize_database()

# ✅ Load job queries from CSV
def load_jobs():
    file_path = "jobs.csv"

    if not os.path.exists(file_path):
        st.error(f"⚠️ File '{file_path}' not found! Please ensure it exists in the project folder.")
        return []

    df = pd.read_csv(file_path)
    
    # ✅ Print column names for debugging
    st.write("Columns in jobs.csv:", df.columns.tolist())

    jobs_data = df.to_dict(orient="records")
    return jobs_data

# ✅ Fetch Google Jobs Results from SerpAPI
def get_google_jobs_results(query, location):
    url = "https://serpapi.com/search"
    params = {
        "engine": "google_jobs",
        "q": query,
        "location": location,
        "hl": "en",
        "api_key": st.secrets["SERP_API_KEY"]
    }
    response = requests.get(url, params=params)
    return response.json().get("jobs_results", [])

# ✅ Compute Share of Voice (Corrected Formula)
def compute_sov():
    domain_sov = defaultdict(float)
    jobs_data = load_jobs()  

    total_sov = 0  # ✅ Track total weight

    for job_query in jobs_data:
        job_title = job_query["job_title"]
        location = job_query["location"]

        # ✅ Fetch job listings from SerpAPI
        jobs = get_google_jobs_results(job_title, location)

        for job_rank, job in enumerate(jobs, start=1):
            apply_options = job.get("apply_options", [])

            # ✅ Vertical weight: 1/job_rank
            V = 1 / job_rank  

            for link_order, option in enumerate(apply_options, start=1):
                if "link" in option:
                    domain = extract_domain(option["link"])  # ✅ Extract normalized domain
                    H = 1 / link_order  # ✅ Horizontal weight

                    weight = V * H  
                    domain_sov[domain] += weight  # ✅ Accumulate domain's SoV
                    total_sov += weight  # ✅ Track total weight

    # ✅ Normalize SoV to ensure total = 100%
    if total_sov > 0:
        for domain in domain_sov:
            domain_sov[domain] = round((domain_sov[domain] / total_sov) * 100, 2)

    return domain_sov

# ✅ Extract Domain from URL
def extract_domain(url):
    extracted = tldextract.extract(url)
    domain = f"{extracted.domain}.{extracted.suffix}" if extracted.suffix else extracted.domain

    # ✅ Remove 'www' to merge domains correctly
    return domain.lower()  # Ensure case insensitivity


# ✅ Store Data in Database
def save_to_db(data):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS share_of_voice (
            id SERIAL PRIMARY KEY,
            domain TEXT NOT NULL,
            sov FLOAT NOT NULL,
            date DATE NOT NULL
        );
    """)

    today = datetime.date.today()
    
    for domain, sov in data.items():
        cursor.execute("INSERT INTO share_of_voice (domain, sov, date) VALUES (%s, %s, %s)",
                       (domain, round(sov, 2), today))  
    
    conn.commit()
    cursor.close()
    conn.close()

# ✅ Retrieve Historical Data
def get_historical_data():
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

    df = pd.read_sql("SELECT * FROM share_of_voice", conn)

    cursor.close()
    conn.close()

    # ✅ Aggregate duplicates by averaging SoV for the same date/domain
    df = df.groupby(["date", "domain"], as_index=False).agg({"sov": "mean"})

    return df

# ✅ Streamlit UI
st.title("Google Jobs Share of Voice Tracker")

if st.button("Fetch & Store Data"):
    domain_sov = compute_sov()  
    save_to_db(domain_sov)  
    st.success("Data stored successfully!")

# ✅ Show Historical Trends
st.write("### Share of Voice Over Time")
df_sov = get_historical_data()

if not df_sov.empty:
    df_sov["date"] = pd.to_datetime(df_sov["date"])
    pivot_df = df_sov.pivot(index="date", columns="domain", values="sov")  

    st.line_chart(pivot_df)
    st.dataframe(df_sov)
else:
    st.write("No historical data available yet.")
