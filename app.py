import streamlit as st
import requests
import pandas as pd
import tldextract
import psycopg2
from collections import defaultdict
import datetime

# Database Connection
DB_URL = "your_postgresql_url_here"

def get_db_connection():
    return psycopg2.connect(DB_URL, sslmode="require")

# ✅ Load job titles, search volumes, and locations from CSV
def load_keywords():
    df = pd.read_csv("keywords.csv")  # Read CSV file
    keywords = df.to_dict(orient="records")  # Convert to list of dictionaries
    return keywords

# CTR Model Based on Position
def estimate_ctr(position):
    ctr_table = {1: 0.30, 2: 0.20, 3: 0.15, 4: 0.10, 5: 0.08}
    return ctr_table.get(position, 0.05)  # Default 5% for positions 6+

# Fetch Google Jobs Results
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

# Compute Share of Voice (SOV)
def compute_sov():
    domain_sov = defaultdict(float)
    keywords = load_keywords()  # ✅ Load keywords from CSV

    for keyword in keywords:
        job_title = keyword["job_title"]
        search_volume = keyword["search_volume"]
        location = keyword["location"]

        jobs = get_google_jobs_results(job_title, location)

        for job in jobs:
            if "apply_options" in job:
                for idx, option in enumerate(job["apply_options"], start=1):
                    if "link" in option:
                        url = option["link"]
                        extracted = tldextract.extract(url)

                        domain = f"{extracted.subdomain}.{extracted.domain}.{extracted.suffix}" if extracted.subdomain else f"{extracted.domain}.{extracted.suffix}"
                        
                        ctr = estimate_ctr(idx)
                        domain_sov[domain] += ctr * search_volume
    
    return domain_sov

# Store Data in Database
def save_to_db(data):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS share_of_voice (
            id SERIAL PRIMARY KEY,
            domain TEXT,
            sov FLOAT,
            date DATE
        )
    """)
    
    today = datetime.date.today()
    
    for domain, sov in data.items():
        cursor.execute("INSERT INTO share_of_voice (domain, sov, date) VALUES (%s, %s, %s)",
                       (domain, round(sov, 2), today))

    conn.commit()
    cursor.close()
    conn.close()

# Retrieve Historical Data
def get_historical_data():
    conn = get_db_connection()
    df = pd.read_sql("SELECT * FROM share_of_voice", conn)
    conn.close()
    return df

# Streamlit UI
st.title("Google Jobs Share of Voice Tracker")

if st.button("Fetch & Store Data"):
    domain_sov = compute_sov()
    save_to_db(domain_sov)
    st.success("Data stored successfully!")

# Show Historical Trends
st.write("### Share of Voice Over Time")
df_sov = get_historical_data()

if not df_sov.empty:
    df_sov["date"] = pd.to_datetime(df_sov["date"])
    pivot_df = df_sov.pivot(index="date", columns="domain", values="sov")
    
    st.line_chart(pivot_df)
    st.dataframe(df_sov)
else:
    st.write("No historical data available yet.")
