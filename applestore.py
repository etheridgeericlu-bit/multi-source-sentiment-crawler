# Prerequisites:
# pip install app-store-scraper pandas

import sqlite3
import pandas as pd
from app_store_scraper import AppStore

# ==========================================
# Module 1: Data Fetching & Cleaning
# ==========================================
def fetch_apple_reviews(app_name, app_id, country='us', count=200):
    print(f"[*] Fetching reviews for {app_name} from the Apple App Store...")
    app = AppStore(country=country, app_name=app_name, app_id=app_id)
    app.review(how_many=count)
    
    df = pd.DataFrame(app.reviews)
    
    if not df.empty:
        df = df[['userName', 'rating', 'date', 'review', 'title']]
        df.rename(columns={'date': 'timestamp', 'review': 'review_text'}, inplace=True)
        df.dropna(subset=['review_text'], inplace=True)
        df['timestamp'] = df['timestamp'].astype(str) 
        print(f"[+] Successfully fetched and cleaned {len(df)} valid reviews.\n")
    else:
        print("[-] No reviews fetched.\n")
        
    return df

# ==========================================
# Module 2: Database Setup
# ==========================================
def setup_database(db_path="sentiment_pipeline.db"):
    print("[*] Initializing SQLite database and schema...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Apps (
        app_id TEXT PRIMARY KEY,
        app_name TEXT NOT NULL,
        platform TEXT NOT NULL
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Users (
        user_id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Reviews (
        review_id INTEGER PRIMARY KEY AUTOINCREMENT,
        app_id TEXT NOT NULL,
        user_id INTEGER NOT NULL,
        rating INTEGER CHECK(rating >= 1 AND rating <= 5),
        title TEXT,
        review_text TEXT NOT NULL,
        review_timestamp DATETIME NOT NULL,
        FOREIGN KEY (app_id) REFERENCES Apps (app_id),
        FOREIGN KEY (user_id) REFERENCES Users (user_id),
        UNIQUE(app_id, user_id, review_timestamp)
    )
    ''')
    
    conn.commit()
    conn.close()
    print("[+] Database initialization complete.\n")

# ==========================================
# Module 3: Data Loading
# ==========================================
def load_data_to_sqlite(df, app_name, app_id, platform="Apple App Store", db_path="sentiment_pipeline.db"):
    if df.empty:
        print("[-] No data to load.\n")
        return

    print("[*] Dismantling and loading data into the relational database...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Insert App
    cursor.execute('''
        INSERT OR IGNORE INTO Apps (app_id, app_name, platform)
        VALUES (?, ?, ?)
    ''', (str(app_id), app_name, platform))

    new_reviews_count = 0
    # Insert Users and Reviews
    for _, row in df.iterrows():
        username = row['userName']
        
        cursor.execute('''
            INSERT OR IGNORE INTO Users (username)
            VALUES (?)
        ''', (username,))
        
        cursor.execute('SELECT user_id FROM Users WHERE username = ?', (username,))
        user_result = cursor.fetchone()
        if user_result:
            user_id = user_result[0]
        else:
            continue 

        try:
            cursor.execute('''
                INSERT OR IGNORE INTO Reviews (app_id, user_id, rating, title, review_text, review_timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                str(app_id), 
                user_id, 
                row['rating'], 
                row['title'], 
                row['review_text'], 
                row['timestamp']
            ))
            if cursor.rowcount > 0:
                new_reviews_count += 1
        except sqlite3.IntegrityError as e:
            print(f"[-] Integrity Error: {e}")

    conn.commit()
    conn.close()
    print(f"[+] Data loading complete. {new_reviews_count} new reviews saved!\n")

# ==========================================
# Module 4: Verification
# ==========================================
def verify_database(db_path="sentiment_pipeline.db"):
    print("[*] Connecting to the database for validation...")
    conn = sqlite3.connect(db_path)
    
    count_query = "SELECT COUNT(*) as total_reviews FROM Reviews;"
    total_reviews_df = pd.read_sql_query(count_query, conn)
    print("\n--- Data Volume Check ---")
    print(f"Total reviews currently stored: {total_reviews_df['total_reviews'].iloc[0]}")
    
    join_query = """
    SELECT 
        a.app_name, 
        u.username, 
        r.rating, 
        r.title, 
        r.review_timestamp
    FROM Reviews r
    JOIN Apps a ON r.app_id = a.app_id
    JOIN Users u ON r.user_id = u.user_id
    ORDER BY r.review_timestamp DESC
    LIMIT 5;
    """
    
    print("\n--- Latest 5 Reviews (Joined across Apps, Users, and Reviews) ---")
    joined_df = pd.read_sql_query(join_query, conn)
    print(joined_df.to_string(index=False))
    
    conn.close()
    print("\n[+] Verification complete.")

# ==========================================
# Main Execution Flow
# ==========================================
if __name__ == "__main__":
    # Configuration
    target_app_name = 'spotify-music-and-podcasts'
    target_app_id = 324684580 
    database_file = "sentiment_pipeline.db"

    # Step 1: Fetch and Clean
    reviews_df = fetch_apple_reviews(app_name=target_app_name, app_id=target_app_id, count=100)
    
    # Step 2: Setup Database
    setup_database(db_path=database_file)
    
    # Step 3: Load Data
    load_data_to_sqlite(
        df=reviews_df, 
        app_name=target_app_name, 
        app_id=target_app_id, 
        db_path=database_file
    )
    
    # Step 4: Verify
    verify_database(db_path=database_file)