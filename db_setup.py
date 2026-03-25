import logging
import os
import sqlite3

DEFAULT_DB_PATH = "sentiment_pipeline.db"
logger = logging.getLogger(__name__)


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
                FOREIGN KEY (subreddit_id) REFERENCES Subreddits(subreddit_id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES Users(user_id) ON DELETE SET NULL
            )
            """
        )

        cursor.execute("CREATE INDEX IF NOT EXISTS idx_reviews_app_id ON Reviews(app_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_reviews_user_id ON Reviews(user_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_posts_subreddit_id ON Posts(subreddit_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_posts_user_id ON Posts(user_id)")

        conn.commit()

    logger.info("Database initialization complete.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    initialize_database()