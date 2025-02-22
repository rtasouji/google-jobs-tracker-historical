import streamlit as st
import requests
import pandas as pd
import tldextract
import psycopg2
from collections import defaultdict
import datetime
import os


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
    SERP_API_KEY = os.getenv("SERP_API_KEY")  # ✅ Load API key from environment variable

    if not SERP_API_KEY:
        raise ValueError("❌ ERROR: SERP_API_KEY environment variable is not set!")

    url = "https://serpapi.com/search"
    params = {
        "engine": "google_jobs",
        "q": query,
        "location": location,
        "hl": "en",
        "api_key": SERP_API_KEY  # ✅ Use the API key from GitHub Actions
    }
    
    response = requests.get(url, params=params)
    
    # ✅ Handle potential API errors
    if response.status_code != 200:
        raise RuntimeError(f"❌ ERROR: Failed to fetch data from SerpAPI. Status Code: {response.status_code}")
    
    return response.json().get("jobs_results", [])

# ✅ Compute Share of Voice (Corrected Formula)
def compute_sov():
    domain_sov = defaultdict(float)
    jobs_data = load_jobs()

    total_sov = 0  # ✅ Track total weight correctly

    for job_query in jobs_data:
        job_title = job_query["job_title"]
        location = job_query["location"]

        # ✅ Fetch job listings from SerpAPI
        jobs = get_google_jobs_results(job_title, location)

        for job_rank, job in enumerate(jobs, start=1):
            apply_options = job.get("apply_options", [])

            # ✅ Vertical weight: Higher-ranked jobs contribute more
            V = 1 / job_rank  

            for link_order, option in enumerate(apply_options, start=1):
                if "link" in option:
                    domain = extract_domain(option["link"])  # ✅ Extract cleaned domain
                    H = 1 / link_order  # ✅ Horizontal weight

                    weight = V * H  
                    domain_sov[domain] += weight  # ✅ Accumulate domain SoV
                    total_sov += weight  # ✅ Track total weight

    # ✅ Normalize SoV to ensure total sum is 100%
    if total_sov > 0:
        domain_sov = {domain: round((sov / total_sov) * 100, 4) for domain, sov in domain_sov.items()}

    return domain_sov

# ✅ Extract Domain from URL
def extract_domain(url):
    extracted = tldextract.extract(url)
    domain = f"{extracted.domain}.{extracted.suffix}" if extracted.suffix else extracted.domain

    # ✅ Ensure consistency by removing 'www.' from domains
    return domain.lower().replace("www.", "")

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

    # ✅ Sort by the most recent date's SoV values (if data exists)
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

# ✅ Fetch & Store Data
if st.button("Fetch & Store Data"):
    domain_sov = compute_sov()  
    save_to_db(domain_sov)  
    st.success("Data stored successfully!")

# ✅ Show Historical Trends
st.write("### Share of Voice Over Time")
df_sov = get_historical_data(start_date, end_date)

if not df_sov.empty:
    st.line_chart(df_sov.T)  # Transpose for better visualization
    st.dataframe(df_sov)
else:
    st.write("No historical data available for the selected date range.")
if __name__ == "__main__":
    domain_sov = compute_sov()  # ✅ Fetch data & calculate SoV
    save_to_db(domain_sov)      # ✅ Store in DB
    print("✅ Data fetched and stored successfully!")
