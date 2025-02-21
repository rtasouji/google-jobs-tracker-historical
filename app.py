import streamlit as st
import requests
import pandas as pd
import tldextract
import psycopg2
from collections import defaultdict
import datetime
import json
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

# ✅ Load SerpAPI JSON from File
def load_serpapi_json(file_path):
    if not os.path.exists(file_path):
        st.error(f"⚠️ File '{file_path}' not found! Ensure it exists.")
        return None
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)

# ✅ Extract Domain from URL (Normalize for Consistency)
def extract_domain(url):
    extracted = tldextract.extract(url)
    domain = f"{extracted.domain}.{extracted.suffix}" if extracted.suffix else extracted.domain
    return domain.lower().replace("www.", "")  # ✅ Remove "www."

# ✅ Compute Share of Voice (SoV) Accurately
def compute_sov(json_data):
    domain_sov = defaultdict(float)
    total_weight = 0

    jobs = json_data.get("jobs_results", [])

    for job_rank, job in enumerate(jobs, start=1):  # ✅ Rank starts at 1
        apply_options = job.get("apply_options", [])  # ✅ List of application links
        V = 1 / job_rank  # ✅ Vertical weight (higher ranks contribute more)

        for link_order, option in enumerate(apply_options, start=1):
            if "link" in option:
                domain = extract_domain(option["link"])
                H = 1 / link_order  # ✅ Horizontal weight (leftmost link contributes more)
                
                weight = V * H  # ✅ Combined weight
                domain_sov[domain] += weight  # ✅ Accumulate domain's SoV
                total_weight += weight  # ✅ Track total weight for normalization

    # ✅ Normalize SoV to ensure the total is 100%
    if total_weight > 0:
        domain_sov = {domain: round((sov / total_weight) * 100, 2) for domain, sov in domain_sov.items()}
    
    return domain_sov

# ✅ Store Data in Database
def save_to_db(data):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    today = datetime.date.today()
    
    for domain, sov in data.items():
        cursor.execute("INSERT INTO share_of_voice (domain, sov, date) VALUES (%s, %s, %s)",
                       (domain, round(sov, 2), today))  # ✅ Store SoV correctly
    
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

    # ✅ Fetch data from the database
    df = pd.read_sql("SELECT * FROM share_of_voice", conn)

    cursor.close()
    conn.close()

    # ✅ Aggregate duplicates by averaging SoV for the same date/domain
    df = df.groupby(["date", "domain"], as_index=False).agg({"sov": "mean"})

    return df

# ✅ Streamlit UI
st.title("Google Jobs Share of Voice Tracker")

# ✅ Load JSON file and process SoV
file_path = "serpapi json.txt"  # Update this path as needed
json_data = load_serpapi_json(file_path)

if json_data and st.button("Compute & Store SoV"):
    domain_sov = compute_sov(json_data)  # ✅ Compute SoV from JSON
    save_to_db(domain_sov)  # ✅ Save results to database
    st.success("SoV Data Stored Successfully!")

# ✅ Show Historical Trends
st.write("### Share of Voice Over Time")
df_sov = get_historical_data()

if not df_sov.empty:
    df_sov["date"] = pd.to_datetime(df_sov["date"])  # Ensure date format
    pivot_df = df_sov.pivot(index="date", columns="domain", values="sov")  # ✅ Now pivot will work

    st.line_chart(pivot_df)
    st.dataframe(df_sov)
else:
    st.write("No historical data available yet.")
