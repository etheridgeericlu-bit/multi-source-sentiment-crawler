import hashlib
import logging
import sqlite3
from collections import Counter
from typing import Iterable, Optional, Tuple

import pandas as pd
from app_store_scraper import AppStore

from utils import clean_text, preprocess_text

DEFAULT_DB_PATH = "sentiment_pipeline.db"
logger = logging.getLogger(__name__)


def generate_review_key(app_id: str, username: str, timestamp: str, review_text: str) -> str:
    normalized_text = (review_text or "").strip()
    raw_str = f"{app_id}|{username}|{timestamp}|{normalized_text[:100]}"
    return hashlib.sha256(raw_str.encode("utf-8")).hexdigest()


def _log_ingestion_stats(stage: str, df: pd.DataFrame) -> None:
    """Log a compact health snapshot after a filtering stage."""
    if df.empty:
        logger.info("[%s] 0 records remaining", stage)
        return

    lang_dist = Counter(df.get("language", pd.Series(dtype=str)).fillna("unknown"))
    low_signal_count = int(df.get("is_low_signal", pd.Series(0, index=df.index)).sum())
    avg_quality = float(df.get("quality_score", pd.Series(dtype=float)).mean() or 0.0)

    logger.info(
        "[%s] rows=%d | low_signal=%d | avg_quality=%.3f | langs=%s",
        stage,
        len(df),
        low_signal_count,
        avg_quality,
        dict(lang_dist.most_common(5)),
    )


def _apply_quality_columns(df: pd.DataFrame, text_column: str) -> pd.DataFrame:
    """Run ``preprocess_text`` over a column and attach metadata columns."""
    preprocessed = df[text_column].fillna("").astype(str).map(preprocess_text)

    df = df.copy()
    df[text_column] = preprocessed.map(lambda q: q.cleaned_text)
    df["cleaned_length"] = preprocessed.map(lambda q: q.char_length)
    df["word_count"] = preprocessed.map(lambda q: q.word_count)
    df["language"] = preprocessed.map(lambda q: q.language)
    df["quality_score"] = preprocessed.map(lambda q: q.quality_score)
    df["is_low_signal"] = preprocessed.map(lambda q: int(q.is_low_signal))
    df["low_signal_reasons"] = preprocessed.map(lambda q: q.reasons)
    return df


def fetch_apple_reviews(
    app_id: str,
    country: str = "us",
    count: int = 200,
    *,
    drop_low_signal: bool = False,
    min_quality: Optional[float] = None,
    keep_languages: Optional[Iterable[str]] = None,
) -> Tuple[pd.DataFrame, str]:
    """Fetch, clean, and quality-assess reviews from the Apple App Store.

    Parameters
    ----------
    drop_low_signal
        When True, reviews flagged as low-signal are removed before return.
    min_quality
        When set, reviews whose ``quality_score`` is below this value are
        removed.
    keep_languages
        When provided, only reviews whose detected language is in this set
        are kept. Rows with language=None are always kept because
        ``langdetect`` is unreliable for short text.
    """
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
    resolved_app_name = (
        getattr(app, "app_name", None) or getattr(app, "name", None) or f"AppId_{app_id}"
    )

    if df.empty:
        logger.warning("No reviews found for app_id=%s", app_id)
        return pd.DataFrame(), resolved_app_name

    expected_cols = {"date", "review", "userName", "rating", "title"}
    missing_cols = expected_cols - set(df.columns)
    if missing_cols:
        logger.error(
            "Unexpected schema returned by app_store_scraper. Missing columns: %s",
            sorted(missing_cols),
        )
        return pd.DataFrame(), resolved_app_name

    df = df.rename(columns={"date": "timestamp", "review": "review_text"}).copy()
    df["userName"] = df["userName"].fillna("Anonymous").astype(str).str.strip()
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    df["title"] = df["title"].fillna("").astype(str).map(clean_text)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True).dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    df = df.dropna(subset=["rating", "timestamp"])
    df["rating"] = df["rating"].astype(int)
    df = df[df["rating"].between(1, 5)]

    # Deep preprocessing + metadata for the review body.
    df = _apply_quality_columns(df, "review_text")

    # Always drop rows whose body cleaned to an empty string - they carry no
    # signal regardless of the user's filter preferences.
    df = df[df["review_text"] != ""].reset_index(drop=True)
    raw_total = len(df)
    _log_ingestion_stats("after_clean", df)

    # In-batch deduplication: same user posting the identical cleaned review
    # multiple times. Keeps the first occurrence (which is the most recent
    # from the API's perspective).
    before_dedup = len(df)
    df = df.drop_duplicates(subset=["userName", "review_text"], keep="first").reset_index(drop=True)
    if len(df) < before_dedup:
        logger.info(
            "Deduplicated %d identical review(s) from the same user(s)",
            before_dedup - len(df),
        )

    # Optional filtering layer - driven by CLI flags.
    if keep_languages:
        keep_set = {lang.strip().lower() for lang in keep_languages if lang and lang.strip()}
        if keep_set:
            mask = df["language"].isna() | df["language"].str.lower().isin(keep_set)
            dropped = (~mask).sum()
            df = df[mask].reset_index(drop=True)
            logger.info("Language filter kept %s; dropped %d row(s)", sorted(keep_set), int(dropped))

    if min_quality is not None:
        mask = df["quality_score"] >= min_quality
        dropped = (~mask).sum()
        df = df[mask].reset_index(drop=True)
        logger.info("min_quality=%.2f dropped %d row(s)", float(min_quality), int(dropped))

    if drop_low_signal:
        mask = df["is_low_signal"] == 0
        dropped = (~mask).sum()
        df = df[mask].reset_index(drop=True)
        logger.info("Low-signal filter dropped %d row(s)", int(dropped))

    logger.info("Final review count: %d (from %d after basic cleaning)", len(df), raw_total)
    _log_ingestion_stats("final", df)
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

    # Be defensive: if quality columns are missing (e.g. caller bypassed
    # fetch_apple_reviews and built their own df), back-fill with nulls.
    for col, default in (
        ("cleaned_length", None),
        ("word_count", None),
        ("language", None),
        ("quality_score", None),
        ("is_low_signal", 0),
        ("low_signal_reasons", None),
    ):
        if col not in df_copy.columns:
            df_copy[col] = default

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
                None if pd.isna(row.cleaned_length) else int(row.cleaned_length),
                None if pd.isna(row.word_count) else int(row.word_count),
                None if (row.language is None or pd.isna(row.language)) else str(row.language),
                None if pd.isna(row.quality_score) else float(row.quality_score),
                int(row.is_low_signal) if not pd.isna(row.is_low_signal) else 0,
                None if (row.low_signal_reasons is None or pd.isna(row.low_signal_reasons))
                else str(row.low_signal_reasons),
            )
            for row in df_copy.itertuples(index=False)
        ]

        total_before = conn.total_changes
        cursor.executemany(
            """
            INSERT INTO Reviews
                (review_key, app_id, user_id, rating, title, review_text,
                 review_timestamp, source,
                 cleaned_length, word_count, language, quality_score,
                 is_low_signal, low_signal_reasons)
            VALUES
                (?, ?,
                 (SELECT user_id FROM Users WHERE username = ? AND platform = ?),
                 ?, ?, ?, ?, ?,
                 ?, ?, ?, ?, ?, ?)
            ON CONFLICT(review_key) DO UPDATE SET
                user_id = excluded.user_id,
                rating = excluded.rating,
                title = excluded.title,
                review_text = excluded.review_text,
                review_timestamp = excluded.review_timestamp,
                source = excluded.source,
                cleaned_length = excluded.cleaned_length,
                word_count = excluded.word_count,
                language = excluded.language,
                quality_score = excluded.quality_score,
                is_low_signal = excluded.is_low_signal,
                low_signal_reasons = excluded.low_signal_reasons;
            """,
            review_records,
        )
        conn.commit()
        return conn.total_changes - total_before
