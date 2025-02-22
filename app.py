import streamlit as st
import requests
import pandas as pd
import tldextract
import psycopg2
from collections import defaultdict
import datetime
import os

# ✅ Load Database URL from Environment Variables
DB_URL = os.getenv("DB_URL")  

if not DB_URL:
    raise ValueError("❌ ERROR: DB_URL environment variable is not set!")

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
            appearances INT NOT NULL,
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

    st.write("Columns in jobs.csv:", df.columns.tolist())  # Debugging

    jobs_data = df.to_dict(orient="records")
    return jobs_data

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

# ✅ Compute Share of Voice (SoV) & Count Domain Appearances
def compute_sov():
    domain_sov = defaultdict(float)
    domain_count = defaultdict(int)  # ✅ Track appearances per domain
    jobs_data = load_jobs()
    total_sov = 0  

    for job_query in jobs_data:
        job_title = job_query["job_title"]
        location = job_query["location"]

        jobs = get_google_jobs_results(job_title, location)

        for job_rank, job in enumerate(jobs, start=1):
            apply_options = job.get("apply_options", [])

            V = 1 / job_rank  # ✅ Vertical weight

            for link_order, option in enumerate(apply_options, start=1):
                if "link" in option:
                    domain = extract_domain(option["link"])
                    H = 1 / link_order  # ✅ Horizontal weight

                    weight = V * H  
                    domain_sov[domain] += weight  # ✅ Accumulate SoV
                    domain_count[domain] += 1  # ✅ Count appearances
                    total_sov += weight

    if total_sov > 0:
        domain_sov = {domain: round((sov / total_sov) * 100, 4) for domain, sov in domain_sov.items()}

    return domain_sov, domain_count  # ✅ Return both SoV & appearances

# ✅ Extract Domain from URL
def extract_domain(url):
    extracted = tldextract.extract(url)
    domain = f"{extracted.domain}.{extracted.suffix}" if extracted.suffix else extracted.domain

    # ✅ Ensure consistency by removing 'www.' from domains
    return domain.lower().replace("www.", "")

# ✅ Store Data in Database
def save_to_db(domain_sov, domain_count):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS share_of_voice (
            id SERIAL PRIMARY KEY,
            domain TEXT NOT NULL,
            sov FLOAT NOT NULL,
            appearances INT NOT NULL,
            date DATE NOT NULL
        );
    """)

    today = datetime.date.today()

    for domain, sov in domain_sov.items():
        appearances = domain_count.get(domain, 0)  # ✅ Get count for the domain
        cursor.execute("INSERT INTO share_of_voice (domain, sov, appearances, date) VALUES (%s, %s, %s, %s)",
                       (domain, round(sov, 2), appearances, today))

    conn.commit()
    cursor.close()
    conn.close()

# ✅ Retrieve Historical Data with SoV and Appearance Count
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
        st.warning("⚠️ No data available yet.")
        cursor.close()
        conn.close()
        return pd.DataFrame()

    query = """
        SELECT domain, date, sov, appearances 
        FROM share_of_voice 
        WHERE date BETWEEN %s AND %s
    """
    cursor.execute(query, (start_date, end_date))
    rows = cursor.fetchall()

    df = pd.DataFrame(rows, columns=["domain", "date", "sov", "appearances"])

    cursor.close()
    conn.close()

    pivot_sov = df.pivot(index="domain", columns="date", values="sov")
    pivot_count = df.pivot(index="domain", columns="date", values="appearances")

    if not pivot_sov.empty:
        most_recent_date = pivot_sov.columns[-1]
        pivot_sov = pivot_sov.sort_values(by=most_recent_date, ascending=False)

    return pivot_sov, pivot_count

# ✅ Streamlit UI
st.title("Google Jobs Share of Voice Tracker")

# ✅ Date Range Selector
st.sidebar.header("Date Range Selector")
start_date = st.sidebar.date_input("Start Date", datetime.date(2025, 2, 1))
end_date = st.sidebar.date_input("End Date", datetime.date(2025, 2, 28))

# ✅ Fetch & Store Data
if st.button("Fetch & Store Data"):
    domain_sov, domain_count = compute_sov()  
    save_to_db(domain_sov, domain_count)  
    st.success("✅ Data stored successfully!")

# ✅ Show Historical Trends
st.write("### Share of Voice Over Time")
df_sov, df_count = get_historical_data(start_date, end_date)

if not df_sov.empty:
    st.write("#### Share of Voice (SoV) Over Time")
    st.line_chart(df_sov.T)

    st.write("#### Appearance Count Over Time")
    st.line_chart(df_count.T)

    # ✅ Display SoV and appearances together
    st.write("#### SoV & Appearance Count Table")
    merged_df = df_sov.copy()
    merged_df["Total Appearances"] = df_count.sum(axis=1)  # ✅ Add total appearances column
    st.dataframe(merged_df)
else:
    st.write("No historical data available.")

if __name__ == "__main__":
    domain_sov, domain_count = compute_sov()  # ✅ Fetch data & calculate SoV
    save_to_db(domain_sov, domain_count)      # ✅ Store in DB
    print("✅ Data fetched and stored successfully!")
