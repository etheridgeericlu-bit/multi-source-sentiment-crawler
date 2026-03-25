import logging
import os
import sqlite3
from datetime import datetime, timezone
from typing import Dict, List, Optional

import pandas as pd
import praw

from utils import clean_text

DEFAULT_DB_PATH = "sentiment_pipeline.db"
logger = logging.getLogger(__name__)
_reddit_client: Optional[praw.Reddit] = None


def _create_and_validate_client() -> praw.Reddit:
    """Create and validate a read-only PRAW client."""
    client_id = os.getenv("REDDIT_CLIENT_ID")
    client_secret = os.getenv("REDDIT_CLIENT_SECRET")
    user_agent = os.getenv("REDDIT_USER_AGENT", "SentimentPipeline/1.0")

    missing = [
        name
        for name, value in {
            "REDDIT_CLIENT_ID": client_id,
            "REDDIT_CLIENT_SECRET": client_secret,
        }.items()
        if not value
    ]
    if missing:
        raise EnvironmentError(f"Missing required Reddit credentials: {', '.join(missing)}")

    reddit = praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent,
        check_for_updates=False,
    )

    # Surface credential/config issues early with one lightweight validation request.
    _ = reddit.subreddit("python").display_name
    return reddit


def get_reddit_client() -> praw.Reddit:
    """Return a cached, validated Reddit client."""
    global _reddit_client
    if _reddit_client is None:
        _reddit_client = _create_and_validate_client()
    return _reddit_client


def initialize_praw() -> praw.Reddit:
    """Backward-compatible alias for obtaining the cached Reddit client."""
    return get_reddit_client()


def _normalize_author(author) -> str:
    return str(author) if author else "[deleted]"


def _format_timestamp(created_utc: float) -> str:
    dt_object = datetime.fromtimestamp(created_utc, timezone.utc)
    return dt_object.strftime("%Y-%m-%d %H:%M:%S")


def _submission_to_record(submission) -> Dict[str, object]:
    subreddit_name = submission.subreddit.display_name.lower()
    subreddit_id = getattr(submission, "subreddit_id", None)
    if not subreddit_id:
        subreddit_attr = getattr(submission, "subreddit", None)
        subreddit_id = getattr(subreddit_attr, "fullname", None)
    if not subreddit_id:
        subreddit_attr = getattr(submission, "subreddit", None)
        subreddit_raw_id = getattr(subreddit_attr, "id", None)
        subreddit_id = f"t5_{subreddit_raw_id}" if subreddit_raw_id else ""

    return {
        "post_id": submission.id,
        "subreddit_name": subreddit_name,
        "subreddit_id": str(subreddit_id),
        "author": _normalize_author(submission.author),
        "title": clean_text(submission.title),
        "post_text": clean_text(submission.selftext),
        "score": int(submission.score),
        "num_comments": int(submission.num_comments),
        "timestamp": _format_timestamp(submission.created_utc),
        "flair": submission.link_flair_text or "",
        "post_url": submission.url,
    }


def fetch_subreddit_hot_posts(subreddit_name: str, limit: int = 100) -> pd.DataFrame:
    """Fetch hot posts from a subreddit."""
    if limit <= 0:
        raise ValueError("limit must be positive")

    reddit = get_reddit_client()
    logger.info("Fetching hot posts from r/%s (limit=%d)", subreddit_name, limit)

    records: List[Dict[str, object]] = []
    subreddit = reddit.subreddit(subreddit_name)
    for post in subreddit.hot(limit=limit):
        records.append(_submission_to_record(post))

    df = pd.DataFrame(records)
    if df.empty:
        return df
    return df[(df["title"] != "") | (df["post_text"] != "")].reset_index(drop=True)


def fetch_reddit_submission(post_id: str) -> pd.DataFrame:
    """Fetch a single Reddit submission by post ID."""
    reddit = get_reddit_client()
    logger.info("Fetching Reddit submission %s", post_id)
    submission = reddit.submission(id=post_id)
    record = _submission_to_record(submission)
    df = pd.DataFrame([record])
    return df[(df["title"] != "") | (df["post_text"] != "")].reset_index(drop=True)


def load_posts_to_sqlite(df: pd.DataFrame, db_path: str = DEFAULT_DB_PATH) -> int:
    """Insert/update Reddit posts into SQLite without calling the Reddit API."""
    if df.empty:
        return 0

    required_cols = {
        "post_id",
        "subreddit_id",
        "subreddit_name",
        "author",
        "title",
        "post_text",
        "score",
        "num_comments",
        "timestamp",
        "flair",
        "post_url",
    }
    missing_cols = required_cols - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required Reddit columns: {sorted(missing_cols)}")

    platform = "reddit"
    working_df = df.copy()

    if working_df["subreddit_name"].nunique(dropna=False) != 1:
        raise ValueError("DataFrame contains posts from multiple subreddits")
    if working_df["subreddit_id"].nunique(dropna=False) != 1:
        raise ValueError("DataFrame contains posts from multiple subreddit IDs")

    sub_name = str(working_df["subreddit_name"].iloc[0]).lower()
    sub_id = str(working_df["subreddit_id"].iloc[0]).strip()
    if not sub_id:
        raise ValueError("subreddit_id cannot be empty")

    unique_users = list({(u, platform) for u in working_df["author"].unique() if u != "[deleted]"})

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("PRAGMA foreign_keys = ON;")

        cursor.execute(
            "INSERT OR IGNORE INTO Subreddits (subreddit_id, name) VALUES (?, ?)",
            (sub_id, sub_name),
        )

        if unique_users:
            cursor.executemany(
                "INSERT OR IGNORE INTO Users (username, platform) VALUES (?, ?)",
                unique_users,
            )

        post_records = []
        for row in working_df.itertuples(index=False):
            username = row.author if row.author != "[deleted]" else None
            post_records.append(
                (
                    row.post_id,
                    sub_id,
                    username,
                    platform,
                    row.title,
                    row.post_text,
                    int(row.score),
                    int(row.num_comments),
                    row.timestamp,
                    row.flair,
                    row.post_url,
                    platform,
                )
            )

        total_before = conn.total_changes
        cursor.executemany(
            """
            INSERT INTO Posts
                (post_id, subreddit_id, user_id, title, post_text, score, num_comments, created_timestamp, flair, post_url, source)
            VALUES
                (?, ?,
                 (SELECT user_id FROM Users WHERE username = ? AND platform = ?),
                 ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(post_id) DO UPDATE SET
                subreddit_id = excluded.subreddit_id,
                user_id = excluded.user_id,
                title = excluded.title,
                post_text = excluded.post_text,
                score = excluded.score,
                num_comments = excluded.num_comments,
                created_timestamp = excluded.created_timestamp,
                flair = excluded.flair,
                post_url = excluded.post_url,
                source = excluded.source;
            """,
            post_records,
        )
        conn.commit()
        return conn.total_changes - total_before