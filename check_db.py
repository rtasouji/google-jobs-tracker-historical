import psycopg2
import os
import datetime

try:
    conn = psycopg2.connect(os.getenv("DB_URL"), sslmode="require")
    cursor = conn.cursor()
    today = datetime.date.today()
    
    cursor.execute("SELECT COUNT(*) FROM share_of_voice WHERE date = %s", (today,))
    count = cursor.fetchone()[0]
    
    print(f"Records found for today: {count}")
    
    if count == 0:
        print("⚠️ WARNING: No records were stored for today!")
    
    cursor.close()
    conn.close()
except Exception as e:
    print(f"Error checking database: {str(e)}")
