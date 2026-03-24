import praw
import pandas as pd
import sqlite3
import re
import demoji
from datetime import datetime

# ==========================================
# Configuration & API Credentials
# ==========================================
# Get these from https://www.reddit.com/prefs/apps
CLIENT_ID = 'YOUR_CLIENT_ID'
CLIENT_SECRET = 'YOUR_CLIENT_SECRET'
USER_AGENT = 'SentimentAnalysisBot/0.1 by /u/YOUR_REDDIT_USERNAME'

# ==========================================
# Module 1: Data Fetching (Automated Subreddit)
# ==========================================
def fetch_subreddit_posts(subreddit_name, post_limit=50):
    """
    Automatically crawls the 'Hot' section of a specific subreddit.
    """
    print(f"[*] Accessing r/{subreddit_name}...")
    reddit = praw.Reddit(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        user_agent=USER_AGENT
    )
    
    subreddit = reddit.subreddit(subreddit_name)
    raw_data = []

    for submission in subreddit.hot(limit=post_limit):
        # We collect the post content. 
        # Note: In a real-world NLP pipeline, you might also want to fetch comments.
        raw_data.append({
            'userName': str(submission.author),
            'score': int(submission.score),
            'timestamp': datetime.fromtimestamp(submission.created_utc).isoformat(),
            'text_content': submission.selftext if submission.selftext else "[Title Only Post]",
            'title': submission.title
        })
    
    df = pd.DataFrame(raw_data)
    print(f"[+] Retrieved {len(df)} posts from r/{subreddit_name}.")
    return df

# ==========================================
# Module 2: Data Cleaning (Pre-processing for NLP)
# ==========================================
def clean_text(text):
    """
    Cleans text by removing URLs, emojis, and special characters 
    to prepare it for sentiment analysis models.
    """
    if not text:
        return ""
    
    # 1. Remove URLs
    text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
    
    # 2. Remove Emojis (using demoji library)
    text = demoji.replace(text, "")
    
    # 3. Remove Special Characters & Line breaks (keep basic punctuation)
    text = re.sub(r'\s+', ' ', text) # Replace multiple spaces/newlines with single space
    text = re.sub(r'[^\x00-\x7f]',r'', text) # Remove non-ASCII characters
    
    return text.strip()

# ==========================================
# Module 3: Database Setup
# ==========================================
def setup_database(db_path="reddit_data.db"):
    print("[*] Initializing SQLite database...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Subreddits (
        sub_id TEXT PRIMARY KEY,
        sub_name TEXT NOT NULL
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Users (
        user_id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Posts (
        post_id INTEGER PRIMARY KEY AUTOINCREMENT,
        sub_id TEXT NOT NULL,
        user_id INTEGER NOT NULL,
        score INTEGER,
        title TEXT,
        clean_content TEXT NOT NULL,
        created_at DATETIME NOT NULL,
        FOREIGN KEY (sub_id) REFERENCES Subreddits (sub_id),
        FOREIGN KEY (user_id) REFERENCES Users (user_id)
    )
    ''')
    
    conn.commit()
    conn.close()

# ==========================================
# Module 4: Data Loading
# ==========================================
def load_to_db(df, sub_name, sub_id, db_path="reddit_data.db"):
    print("[*] Cleaning and saving data to database...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Insert Subreddit
    cursor.execute('INSERT OR IGNORE INTO Subreddits (sub_id, sub_name) VALUES (?, ?)', (sub_id, sub_name))

    for _, row in df.iterrows():
        # Clean the text before saving
        cleaned_text = clean_text(row['text_content'])
        if not cleaned_text: continue

        # Handle User
        cursor.execute('INSERT OR IGNORE INTO Users (username) VALUES (?)', (row['userName'],))
        cursor.execute('SELECT user_id FROM Users WHERE username = ?', (row['userName'],))
        user_id = cursor.fetchone()[0]

        # Insert Post
        cursor.execute('''
            INSERT INTO Posts (sub_id, user_id, score, title, clean_content, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (sub_id, user_id, row['score'], row['title'], cleaned_text, row['timestamp']))

    conn.commit()
    conn.close()
    print("[+] Database update complete.")

# ==========================================
# Main Execution
# ==========================================
if __name__ == "__main__":
    # Settings
    TARGET_SUB = 'technology'
    SUB_UID = 'sub_001'
    DB_NAME = "reddit_sentiment_analysis.db"

    # Step 1: Fetch
    raw_df = fetch_subreddit_posts(TARGET_SUB, post_limit=20)
    
    # Step 2: Setup DB
    setup_database(DB_NAME)
    
    # Step 3: Clean & Load
    load_to_db(raw_df, TARGET_SUB, SUB_UID, DB_NAME)
    
    print("\n--- Process Finished Successfully ---")