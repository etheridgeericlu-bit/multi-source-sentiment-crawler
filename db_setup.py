import logging
import os
import sqlite3
from typing import Iterable, Tuple

DEFAULT_DB_PATH = "sentiment_pipeline.db"
logger = logging.getLogger(__name__)

# Columns added in the text-quality upgrade. Declared here so we can migrate
# pre-existing databases in place using ALTER TABLE ADD COLUMN (SQLite allows
# adding nullable columns to a populated table cheaply).
_QUALITY_COLUMNS: Tuple[Tuple[str, str], ...] = (
    ("cleaned_length", "INTEGER"),
    ("word_count", "INTEGER"),
    ("language", "TEXT"),
    ("quality_score", "REAL"),
    ("is_low_signal", "INTEGER DEFAULT 0"),
    ("low_signal_reasons", "TEXT"),
)


def _ensure_columns(cursor: sqlite3.Cursor, table: str, columns: Iterable[Tuple[str, str]]) -> None:
    """Add any of ``columns`` that are missing from ``table``.

    Idempotent: safe to call on every startup. This lets the schema evolve
    without forcing users to drop their existing database.
    """
    cursor.execute(f"PRAGMA table_info({table})")
    existing = {row[1] for row in cursor.fetchall()}
    for column_name, definition in columns:
        if column_name in existing:
            continue
        logger.info("Adding column %s.%s (%s)", table, column_name, definition)
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column_name} {definition}")


def initialize_database(db_path: str = DEFAULT_DB_PATH) -> None:
    """Initialize the shared SQLite schema for Apple + Reddit scrapers."""
    if os.path.exists(db_path):
        logger.info("Database '%s' already exists. Synchronizing schema.", db_path)
    else:
        logger.info("Creating new database at '%s'.", db_path)

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("PRAGMA journal_mode = WAL;")
        cursor.execute("PRAGMA foreign_keys = ON;")

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Users (
                user_id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                platform TEXT NOT NULL,
                UNIQUE(username, platform)
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Apps (
                app_id TEXT PRIMARY KEY,
                app_name TEXT NOT NULL,
                platform TEXT NOT NULL DEFAULT 'apple'
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Reviews (
                review_id INTEGER PRIMARY KEY AUTOINCREMENT,
                review_key TEXT NOT NULL UNIQUE,
                app_id TEXT NOT NULL,
                user_id INTEGER,
                rating INTEGER CHECK(rating >= 1 AND rating <= 5),
                title TEXT,
                review_text TEXT NOT NULL,
                review_timestamp DATETIME,
                source TEXT NOT NULL DEFAULT 'apple',
                cleaned_length INTEGER,
                word_count INTEGER,
                language TEXT,
                quality_score REAL,
                is_low_signal INTEGER DEFAULT 0,
                low_signal_reasons TEXT,
                FOREIGN KEY (app_id) REFERENCES Apps(app_id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES Users(user_id) ON DELETE SET NULL
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Subreddits (
                subreddit_id TEXT PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Posts (
                post_id TEXT PRIMARY KEY,
                subreddit_id TEXT NOT NULL,
                user_id INTEGER,
                title TEXT,
                post_text TEXT,
                score INTEGER,
                num_comments INTEGER,
                created_timestamp DATETIME,
                flair TEXT,
                post_url TEXT,
                source TEXT NOT NULL DEFAULT 'reddit',
                cleaned_length INTEGER,
                word_count INTEGER,
                language TEXT,
                quality_score REAL,
                is_low_signal INTEGER DEFAULT 0,
                low_signal_reasons TEXT,
                FOREIGN KEY (subreddit_id) REFERENCES Subreddits(subreddit_id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES Users(user_id) ON DELETE SET NULL
            )
            """
        )

        # Migrate any pre-existing DB that predates the quality columns.
        _ensure_columns(cursor, "Reviews", _QUALITY_COLUMNS)
        _ensure_columns(cursor, "Posts", _QUALITY_COLUMNS)

        cursor.execute("CREATE INDEX IF NOT EXISTS idx_reviews_app_id ON Reviews(app_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_reviews_user_id ON Reviews(user_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_reviews_low_signal ON Reviews(is_low_signal)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_reviews_language ON Reviews(language)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_posts_subreddit_id ON Posts(subreddit_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_posts_user_id ON Posts(user_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_posts_low_signal ON Posts(is_low_signal)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_posts_language ON Posts(language)")

        conn.commit()

    logger.info("Database initialization complete.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    initialize_database()
