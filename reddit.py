import logging
import os
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional

import pandas as pd
import praw

from utils import clean_text, preprocess_text

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

    # Preprocess the post body (richer than the title) and a lightweight clean
    # for the title so both fields are safe to persist.
    body_quality = preprocess_text(submission.selftext or "")

    return {
        "post_id": submission.id,
        "subreddit_name": subreddit_name,
        "subreddit_id": str(subreddit_id),
        "author": _normalize_author(submission.author),
        "title": clean_text(submission.title),
        "post_text": body_quality.cleaned_text,
        "score": int(submission.score),
        "num_comments": int(submission.num_comments),
        "timestamp": _format_timestamp(submission.created_utc),
        "flair": submission.link_flair_text or "",
        "post_url": submission.url,
        "cleaned_length": body_quality.char_length,
        "word_count": body_quality.word_count,
        "language": body_quality.language,
        "quality_score": body_quality.quality_score,
        "is_low_signal": int(body_quality.is_low_signal),
        "low_signal_reasons": body_quality.reasons,
    }


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


def _apply_quality_filters(
    df: pd.DataFrame,
    *,
    drop_low_signal: bool,
    min_quality: Optional[float],
    keep_languages: Optional[Iterable[str]],
) -> pd.DataFrame:
    if df.empty:
        return df

    # Posts with empty body AND empty title have no text to analyze -> drop.
    df = df[(df["title"] != "") | (df["post_text"] != "")].reset_index(drop=True)

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
        # Keep a post if EITHER the title or body carries real signal: a
        # strongly-worded headline can still be useful even with an empty body.
        mask = (df["is_low_signal"] == 0) | (df["title"].str.len() >= 10)
        dropped = (~mask).sum()
        df = df[mask].reset_index(drop=True)
        logger.info("Low-signal filter dropped %d row(s)", int(dropped))

    return df


def fetch_subreddit_hot_posts(
    subreddit_name: str,
    limit: int = 100,
    *,
    drop_low_signal: bool = False,
    min_quality: Optional[float] = None,
    keep_languages: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    """Fetch hot posts from a subreddit, cleaned and quality-assessed."""
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

    _log_ingestion_stats("after_clean", df)

    before_dedup = len(df)
    df = df.drop_duplicates(subset=["author", "title", "post_text"], keep="first").reset_index(
        drop=True
    )
    if len(df) < before_dedup:
        logger.info(
            "Deduplicated %d identical post(s) from the same author(s)",
            before_dedup - len(df),
        )

    df = _apply_quality_filters(
        df,
        drop_low_signal=drop_low_signal,
        min_quality=min_quality,
        keep_languages=keep_languages,
    )
    _log_ingestion_stats("final", df)
    return df


def fetch_reddit_submission(
    post_id: str,
    *,
    drop_low_signal: bool = False,
    min_quality: Optional[float] = None,
    keep_languages: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    """Fetch a single Reddit submission by post ID, cleaned and quality-assessed."""
    reddit = get_reddit_client()
    logger.info("Fetching Reddit submission %s", post_id)
    submission = reddit.submission(id=post_id)
    record = _submission_to_record(submission)
    df = pd.DataFrame([record])

    df = _apply_quality_filters(
        df,
        drop_low_signal=drop_low_signal,
        min_quality=min_quality,
        keep_languages=keep_languages,
    )
    return df


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

    # Back-fill quality columns for defensive callers.
    for col, default in (
        ("cleaned_length", None),
        ("word_count", None),
        ("language", None),
        ("quality_score", None),
        ("is_low_signal", 0),
        ("low_signal_reasons", None),
    ):
        if col not in working_df.columns:
            working_df[col] = default

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
                    None if pd.isna(row.cleaned_length) else int(row.cleaned_length),
                    None if pd.isna(row.word_count) else int(row.word_count),
                    None if (row.language is None or pd.isna(row.language)) else str(row.language),
                    None if pd.isna(row.quality_score) else float(row.quality_score),
                    int(row.is_low_signal) if not pd.isna(row.is_low_signal) else 0,
                    None if (row.low_signal_reasons is None or pd.isna(row.low_signal_reasons))
                    else str(row.low_signal_reasons),
                )
            )

        total_before = conn.total_changes
        cursor.executemany(
            """
            INSERT INTO Posts
                (post_id, subreddit_id, user_id, title, post_text, score,
                 num_comments, created_timestamp, flair, post_url, source,
                 cleaned_length, word_count, language, quality_score,
                 is_low_signal, low_signal_reasons)
            VALUES
                (?, ?,
                 (SELECT user_id FROM Users WHERE username = ? AND platform = ?),
                 ?, ?, ?, ?, ?, ?, ?, ?,
                 ?, ?, ?, ?, ?, ?)
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
                source = excluded.source,
                cleaned_length = excluded.cleaned_length,
                word_count = excluded.word_count,
                language = excluded.language,
                quality_score = excluded.quality_score,
                is_low_signal = excluded.is_low_signal,
                low_signal_reasons = excluded.low_signal_reasons;
            """,
            post_records,
        )
        conn.commit()
        return conn.total_changes - total_before
