import hashlib
import logging
import sqlite3
from typing import Tuple

import pandas as pd
from app_store_scraper import AppStore

from utils import clean_text

DEFAULT_DB_PATH = "sentiment_pipeline.db"
logger = logging.getLogger(__name__)


def generate_review_key(app_id: str, username: str, timestamp: str, review_text: str) -> str:
    normalized_text = (review_text or "").strip()
    raw_str = f"{app_id}|{username}|{timestamp}|{normalized_text[:100]}"
    return hashlib.sha256(raw_str.encode("utf-8")).hexdigest()


def fetch_apple_reviews(app_id: str, country: str = "us", count: int = 200) -> Tuple[pd.DataFrame, str]:
    """Fetch and clean reviews from the Apple App Store."""
    if count <= 0:
        raise ValueError("count must be positive")

    logger.info("Fetching %d Apple reviews for app_id=%s (country=%s)", count, app_id, country)

    app = AppStore(country=country, app_name="", app_id=str(app_id))
    try:
        app.review(how_many=count)
    except Exception as exc:
        partial_count = len(getattr(app, "reviews", []) or [])
        if partial_count > 0:
            logger.warning(
                "Scraping interrupted for app_id=%s after %d review(s): %s. Proceeding with partial data.",
                app_id,
                partial_count,
                exc,
            )
        else:
            logger.error("Error connecting to the App Store for app_id=%s: %s", app_id, exc)
            return pd.DataFrame(), f"AppId_{app_id}"

    df = pd.DataFrame(getattr(app, "reviews", []) or [])
    resolved_app_name = getattr(app, "app_name", None) or getattr(app, "name", None) or f"AppId_{app_id}"

    if df.empty:
        logger.warning("No reviews found for app_id=%s", app_id)
        return pd.DataFrame(), resolved_app_name

    expected_cols = {"date", "review", "userName", "rating", "title"}
    missing_cols = expected_cols - set(df.columns)
    if missing_cols:
        logger.error("Unexpected schema returned by app_store_scraper. Missing columns: %s", sorted(missing_cols))
        return pd.DataFrame(), resolved_app_name

    df = df.rename(columns={"date": "timestamp", "review": "review_text"}).copy()
    df["userName"] = df["userName"].fillna("Anonymous").astype(str).str.strip()
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    df["title"] = df["title"].fillna("").astype(str).map(clean_text)
    df["review_text"] = df["review_text"].fillna("").astype(str).map(clean_text)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True).dt.strftime("%Y-%m-%d %H:%M:%S")

    df = df.dropna(subset=["rating", "timestamp"])
    df["rating"] = df["rating"].astype(int)
    df = df[df["rating"].between(1, 5)]
    df = df[df["review_text"] != ""].reset_index(drop=True)
    return df, resolved_app_name


def load_reviews_to_sqlite(
    df: pd.DataFrame,
    app_name: str,
    app_id: str,
    db_path: str = DEFAULT_DB_PATH,
) -> int:
    """Insert/update Apple reviews into SQLite."""
    if df.empty:
        return 0

    platform = "apple"
    df_copy = df.copy()
    df_copy["review_key"] = df_copy.apply(
        lambda row: generate_review_key(
            str(app_id),
            row["userName"],
            row["timestamp"],
            row["review_text"],
        ),
        axis=1,
    )

    unique_users = list({(u, platform) for u in df_copy["userName"].unique()})

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("PRAGMA foreign_keys = ON;")

        cursor.execute(
            """
            INSERT INTO Apps (app_id, app_name, platform)
            VALUES (?, ?, ?)
            ON CONFLICT(app_id) DO UPDATE SET
                app_name = excluded.app_name,
                platform = excluded.platform;
            """,
            (str(app_id), app_name, platform),
        )

        if unique_users:
            cursor.executemany(
                "INSERT OR IGNORE INTO Users (username, platform) VALUES (?, ?)",
                unique_users,
            )

        review_records = [
            (
                row.review_key,
                str(app_id),
                row.userName,
                platform,
                int(row.rating),
                row.title,
                row.review_text,
                row.timestamp,
                platform,
            )
            for row in df_copy.itertuples(index=False)
        ]

        total_before = conn.total_changes
        cursor.executemany(
            """
            INSERT INTO Reviews
                (review_key, app_id, user_id, rating, title, review_text, review_timestamp, source)
            VALUES
                (?, ?,
                 (SELECT user_id FROM Users WHERE username = ? AND platform = ?),
                 ?, ?, ?, ?, ?)
            ON CONFLICT(review_key) DO UPDATE SET
                user_id = excluded.user_id,
                rating = excluded.rating,
                title = excluded.title,
                review_text = excluded.review_text,
                review_timestamp = excluded.review_timestamp,
                source = excluded.source;
            """,
            review_records,
        )
        conn.commit()
        return conn.total_changes - total_before