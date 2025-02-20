import streamlit as st
import requests
import pandas as pd
import tldextract
import psycopg2
from collections import defaultdict
import datetime

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

# ✅ Now Call initialize_database AFTER Defining get_db_connection()
initialize_database()

# ✅ Load job data from CSV (without search volume)
def load_jobs():
    df = pd.read_csv("jobs.csv")  # Read CSV file
    jobs_data = df.to_dict(orient="records")  # Convert to list of dictionaries
    return jobs_data

# ✅ Fetch Google Jobs Results
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

# ✅ Compute Share of Voice (SoV) Based on Job Ranking & Link Order
def compute_sov():
    domain_sov = defaultdict(float)
    jobs_data = load_jobs()  # ✅ Load job data from CSV

    for job in jobs_data:
        job_rank = job["job_rank"]  # Vertical position of the job
        apply_options = job.get("apply_options", [])  # List of links

        V = 1 / job_rank  # Vertical weight

        for link_order, option in enumerate(apply_options, start=1):
            if "link" in option:
                domain = extract_domain(option["link"])  # Extract domain from URL
                
                H = 1 / link_order  # ✅ Horizontal weight (without scaling factor)
                
                domain_sov[domain] += V * H  # Compute SoV contribution

    # ✅ Normalize SoV to percentage (0-100%)
    total_sov = sum(domain_sov.values())
    if total_sov > 0:
        domain_sov = {domain: round((sov / total_sov) * 100, 2) for domain, sov in domain_sov.items()}

    return domain_sov

def extract_domain(url):
    """
    Extracts the domain from a given URL.
    """
    extracted = tldextract.extract(url)
    return f"{extracted.domain}.{extracted.suffix}" if extracted.suffix else extracted.domain

# ✅ Store Data in Database
def save_to_db(data):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # ✅ Ensure table exists
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
                       (domain, round(sov, 2), today))  # ✅ Store SoV correctly
    
    conn.commit()
    cursor.close()
    conn.close()

# ✅ Retrieve Historical Data
def get_historical_data():
    conn = get_db_connection()
    cursor = conn.cursor()

    # ✅ Check if table exists before querying
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

    # ✅ Fix: Aggregate duplicates by averaging SoV for the same date/domain
    df = df.groupby(["date", "domain"], as_index=False).agg({"sov": "mean"})

    return df

# ✅ Streamlit UI
st.title("Google Jobs Share of Voice Tracker")

if st.button("Fetch & Store Data"):
    domain_sov = compute_sov()  # ✅ Correct function call
    save_to_db(domain_sov)  # ✅ Pass dictionary correctly
    st.success("Data stored successfully!")

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
