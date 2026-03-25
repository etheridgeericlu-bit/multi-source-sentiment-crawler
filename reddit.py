import os
import re
import sqlite3
from datetime import datetime
from urllib.parse import urlparse

import demoji
import pandas as pd
import praw

DEFAULT_DB_PATH = "sentiment_pipeline.db"
CLIENT_ID = os.getenv('REDDIT_CLIENT_ID', 'YOUR_CLIENT_ID')
CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET', 'YOUR_CLIENT_SECRET')
USER_AGENT = os.getenv('REDDIT_USER_AGENT', 'SentimentAnalysisBot/0.1 by /u/YOUR_REDDIT_USERNAME')


def _build_reddit_client():
    if CLIENT_ID == 'YOUR_CLIENT_ID' or CLIENT_SECRET == 'YOUR_CLIENT_SECRET':
        raise ValueError(
            "Reddit API credentials are not configured. Set REDDIT_CLIENT_ID, "
            "REDDIT_CLIENT_SECRET, and optionally REDDIT_USER_AGENT as environment variables."
        )
    return praw.Reddit(client_id=CLIENT_ID, client_secret=CLIENT_SECRET, user_agent=USER_AGENT)



def _submission_to_record(submission, fallback_subreddit=None):
    return {
        'post_key': submission.id,
        'subreddit_id': submission.subreddit.display_name.lower() if submission.subreddit else (fallback_subreddit or 'unknown').lower(),
        'subreddit_name': submission.subreddit.display_name if submission.subreddit else (fallback_subreddit or 'unknown'),
        'userName': str(submission.author) if submission.author else '[deleted]',
        'score': int(getattr(submission, 'score', 0) or 0),
        'timestamp': datetime.fromtimestamp(submission.created_utc).isoformat(),
        'text_content': submission.selftext if submission.selftext else '[Title Only Post]',
        'title': submission.title or '',
        'source_url': f"https://www.reddit.com{submission.permalink}" if getattr(submission, 'permalink', None) else '',
    }



def fetch_subreddit_posts(subreddit_name, post_limit=50):
    """Fetch posts from the hot tab of a subreddit."""
    subreddit_name = subreddit_name.replace('r/', '').strip()
    print(f"[*] Accessing r/{subreddit_name}...")
    reddit = _build_reddit_client()
    subreddit = reddit.subreddit(subreddit_name)
    raw_data = [_submission_to_record(submission, fallback_subreddit=subreddit_name) for submission in subreddit.hot(limit=post_limit)]
    df = pd.DataFrame(raw_data)
    print(f"[+] Retrieved {len(df)} posts from r/{subreddit_name}.")
    return df



def fetch_by_url(url):
    """Fetch a single Reddit submission by URL."""
    if 'reddit.com' not in url:
        raise ValueError('The provided URL is not a valid Reddit link.')

    reddit = _build_reddit_client()
    submission = reddit.submission(url=url)
    submission._fetch()
    record = _submission_to_record(submission)
    df = pd.DataFrame([record])
    print(f"[+] Retrieved Reddit post: {record['title'][:80]}")
    return df



def clean_text(text):
    """Clean text for downstream sentiment analysis while preserving readable Unicode text."""
    if not text:
        return ""

    text = re.sub(r'http\S+|www\S+|https\S+', '', str(text), flags=re.MULTILINE)
    text = demoji.replace(text, "")
    text = text.replace('\r', ' ').replace('\n', ' ')
    text = re.sub(r'\s+', ' ', text)
    return text.strip()



def setup_database(db_path=DEFAULT_DB_PATH):
    print(f"[*] Initializing SQLite database at {db_path}...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Subreddits (
        sub_id TEXT PRIMARY KEY,
        sub_name TEXT NOT NULL UNIQUE
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
        post_key TEXT NOT NULL,
        score INTEGER,
        title TEXT,
        clean_content TEXT NOT NULL,
        created_at DATETIME NOT NULL,
        source_url TEXT,
        FOREIGN KEY (sub_id) REFERENCES Subreddits (sub_id),
        FOREIGN KEY (user_id) REFERENCES Users (user_id),
        UNIQUE(post_key)
    )
    ''')

    conn.commit()
    conn.close()



def load_to_db(df, sub_name=None, sub_id=None, db_path=DEFAULT_DB_PATH):
    if df.empty:
        print("[-] No Reddit data to load.")
        return 0

    print("[*] Cleaning and saving Reddit data to database...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    inserted_count = 0

    for _, row in df.iterrows():
        subreddit_name = (row.get('subreddit_name') or sub_name or 'unknown').strip()
        subreddit_id = (row.get('subreddit_id') or sub_id or subreddit_name.lower()).strip()

        cursor.execute(
            'INSERT OR IGNORE INTO Subreddits (sub_id, sub_name) VALUES (?, ?)',
            (subreddit_id, subreddit_name)
        )

        cleaned_text = clean_text(row.get('text_content', ''))
        if not cleaned_text:
            continue

        username = str(row.get('userName', '[deleted]') or '[deleted]')
        cursor.execute('INSERT OR IGNORE INTO Users (username) VALUES (?)', (username,))
        cursor.execute('SELECT user_id FROM Users WHERE username = ?', (username,))
        user_result = cursor.fetchone()
        if not user_result:
            continue
        user_id = user_result[0]

        cursor.execute('''
            INSERT OR IGNORE INTO Posts (sub_id, user_id, post_key, score, title, clean_content, created_at, source_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            subreddit_id,
            user_id,
            str(row.get('post_key', '')),
            int(row.get('score', 0) or 0),
            str(row.get('title', '')),
            cleaned_text,
            str(row.get('timestamp', '')),
            str(row.get('source_url', '')),
        ))
        if cursor.rowcount > 0:
            inserted_count += 1

    conn.commit()
    conn.close()
    print(f"[+] Reddit database update complete. {inserted_count} new posts saved.")
    return inserted_count


if __name__ == "__main__":
    TARGET_SUB = 'technology'
    DB_NAME = DEFAULT_DB_PATH

    raw_df = fetch_subreddit_posts(TARGET_SUB, post_limit=20)
    setup_database(DB_NAME)
    load_to_db(raw_df, TARGET_SUB, TARGET_SUB.lower(), DB_NAME)
    print("\n--- Process Finished Successfully ---")
