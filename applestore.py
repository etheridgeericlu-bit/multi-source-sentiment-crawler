import sqlite3
import pandas as pd
from app_store_scraper import AppStore

DEFAULT_DB_PATH = "sentiment_pipeline.db"


def fetch_apple_reviews(app_name, app_id, country='us', count=200):
    """Fetch reviews from the Apple App Store and return a cleaned DataFrame."""
    app_name = str(app_name).strip()
    app_id = str(app_id).strip()

    print(f"[*] Fetching reviews for {app_name} (id={app_id}) from the Apple App Store...")
    app = AppStore(country=country, app_name=app_name, app_id=app_id)
    app.review(how_many=count)

    df = pd.DataFrame(app.reviews)
    if df.empty:
        print("[-] No reviews fetched.\n")
        return pd.DataFrame(columns=['userName', 'rating', 'timestamp', 'review_text', 'title'])

    expected_cols = ['userName', 'rating', 'date', 'review', 'title']
    missing = [col for col in expected_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Apple response is missing expected columns: {missing}")

    df = df[expected_cols].copy()
    df.rename(columns={'date': 'timestamp', 'review': 'review_text'}, inplace=True)
    df.dropna(subset=['review_text', 'userName'], inplace=True)
    df['userName'] = df['userName'].astype(str).str.strip()
    df = df[df['userName'] != ""]
    df['rating'] = pd.to_numeric(df['rating'], errors='coerce').astype('Int64')
    df = df[df['rating'].between(1, 5, inclusive='both')]
    df['title'] = df['title'].fillna('').astype(str)
    df['review_text'] = df['review_text'].astype(str).str.strip()
    df = df[df['review_text'] != ""]
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True).astype(str)
    df.dropna(subset=['timestamp'], inplace=True)

    print(f"[+] Successfully fetched and cleaned {len(df)} valid reviews.\n")
    return df.reset_index(drop=True)



def setup_database(db_path=DEFAULT_DB_PATH):
    print(f"[*] Initializing SQLite database and schema at {db_path}...")
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
        source TEXT NOT NULL DEFAULT 'apple',
        FOREIGN KEY (app_id) REFERENCES Apps (app_id),
        FOREIGN KEY (user_id) REFERENCES Users (user_id),
        UNIQUE(app_id, user_id, review_timestamp)
    )
    ''')

    conn.commit()
    conn.close()
    print("[+] Database initialization complete.\n")



def load_data_to_sqlite(df, app_name, app_id, platform="Apple App Store", db_path=DEFAULT_DB_PATH):
    if df.empty:
        print("[-] No Apple data to load.\n")
        return 0

    print("[*] Loading Apple review data into the relational database...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute('''
        INSERT OR IGNORE INTO Apps (app_id, app_name, platform)
        VALUES (?, ?, ?)
    ''', (str(app_id), str(app_name), platform))

    new_reviews_count = 0
    for _, row in df.iterrows():
        username = row['userName']
        cursor.execute('INSERT OR IGNORE INTO Users (username) VALUES (?)', (username,))
        cursor.execute('SELECT user_id FROM Users WHERE username = ?', (username,))
        user_result = cursor.fetchone()
        if not user_result:
            continue

        user_id = user_result[0]
        cursor.execute('''
            INSERT OR IGNORE INTO Reviews (app_id, user_id, rating, title, review_text, review_timestamp, source)
            VALUES (?, ?, ?, ?, ?, ?, 'apple')
        ''', (
            str(app_id),
            user_id,
            int(row['rating']),
            row['title'],
            row['review_text'],
            row['timestamp'],
        ))
        if cursor.rowcount > 0:
            new_reviews_count += 1

    conn.commit()
    conn.close()
    print(f"[+] Apple data loading complete. {new_reviews_count} new reviews saved!\n")
    return new_reviews_count



def verify_database(db_path=DEFAULT_DB_PATH):
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


if __name__ == "__main__":
    target_app_name = 'spotify-music-and-podcasts'
    target_app_id = '324684580'
    database_file = DEFAULT_DB_PATH

    reviews_df = fetch_apple_reviews(app_name=target_app_name, app_id=target_app_id, count=100)
    setup_database(db_path=database_file)
    load_data_to_sqlite(df=reviews_df, app_name=target_app_name, app_id=target_app_id, db_path=database_file)
    verify_database(db_path=database_file)
