import argparse
import logging
import re
from typing import Optional, Tuple

from applestore import fetch_apple_reviews, load_reviews_to_sqlite
from db_setup import initialize_database
from reddit import fetch_reddit_submission, fetch_subreddit_hot_posts, load_posts_to_sqlite

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("Pipeline")

APPLE_ID_RE = re.compile(r"(?:id)?(?P<app_id>\d{6,12})$")
APPLE_URL_RE = re.compile(r"apps\.apple\.com/.*/app/.*/id(?P<app_id>\d{6,12})")
REDDIT_POST_URL_RE = re.compile(
    r"reddit\.com/r/(?P<subreddit>[A-Za-z0-9_]{3,21})/comments/(?P<post_id>[A-Za-z0-9]+)"
)
REDDIT_SUB_URL_RE = re.compile(r"reddit\.com/r/(?P<subreddit>[A-Za-z0-9_]{3,21})(?:/|$)")
SUBREDDIT_NAME_RE = re.compile(r"^(?:r/)?(?P<subreddit>[A-Za-z0-9_]{3,21})$")


def identify_input_source(user_input: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns one of:
      ("APPLE", app_id)
      ("REDDIT_POST", post_id)
      ("REDDIT_SUBREDDIT", subreddit_name)
      (None, None)
    """
    normalized = user_input.strip()
    lowered = normalized.lower()

    apple_url_match = APPLE_URL_RE.search(lowered)
    if apple_url_match:
        return "APPLE", apple_url_match.group("app_id")

    apple_id_match = APPLE_ID_RE.fullmatch(lowered)
    if apple_id_match:
        return "APPLE", apple_id_match.group("app_id")

    reddit_post_match = REDDIT_POST_URL_RE.search(lowered)
    if reddit_post_match:
        return "REDDIT_POST", reddit_post_match.group("post_id")

    reddit_sub_url_match = REDDIT_SUB_URL_RE.search(lowered)
    if reddit_sub_url_match:
        return "REDDIT_SUBREDDIT", reddit_sub_url_match.group("subreddit")

    subreddit_name_match = SUBREDDIT_NAME_RE.fullmatch(lowered)
    if subreddit_name_match:
        return "REDDIT_SUBREDDIT", subreddit_name_match.group("subreddit")

    return None, None


def run_apple_pipeline(app_id: str, count: int, db_path: str, country: str) -> int:
    logger.info("Starting Apple App Store pipeline for app_id=%s", app_id)
    reviews_df, resolved_app_name = fetch_apple_reviews(
        app_id=app_id,
        country=country,
        count=count,
    )
    if reviews_df.empty:
        return 0
    return load_reviews_to_sqlite(
        reviews_df,
        app_name=resolved_app_name,
        app_id=app_id,
        db_path=db_path,
    )


def run_reddit_subreddit_pipeline(subreddit_name: str, limit: int, db_path: str) -> int:
    logger.info("Starting Reddit subreddit pipeline for r/%s", subreddit_name)
    posts_df = fetch_subreddit_hot_posts(subreddit_name=subreddit_name, limit=limit)
    return load_posts_to_sqlite(posts_df, db_path=db_path)


def run_reddit_post_pipeline(post_id: str, db_path: str) -> int:
    logger.info("Starting Reddit single-submission pipeline for post_id=%s", post_id)
    posts_df = fetch_reddit_submission(post_id=post_id)
    return load_posts_to_sqlite(posts_df, db_path=db_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Multi-Source Sentiment Data Crawler")
    parser.add_argument("input", help="Apple App ID/URL OR subreddit name/URL OR Reddit post URL")
    parser.add_argument("--count", type=int, default=200, help="Number of items to fetch")
    parser.add_argument("--db-path", default="sentiment_pipeline.db", help="SQLite database path")
    parser.add_argument("--country", default="us", help="Apple App Store country code, e.g. us, ca")
    args = parser.parse_args()

    if args.count <= 0:
        raise ValueError("--count must be a positive integer")

    initialize_database(db_path=args.db_path)
    source_type, resolved_value = identify_input_source(args.input)

    if source_type is None or resolved_value is None:
        logger.error(
            "Could not recognize input '%s'. Use an App Store ID/URL, a subreddit name/URL, or a Reddit post URL.",
            args.input,
        )
        return

    try:
        if source_type == "APPLE":
            new_records = run_apple_pipeline(
                app_id=resolved_value,
                count=args.count,
                db_path=args.db_path,
                country=args.country,
            )
        elif source_type == "REDDIT_SUBREDDIT":
            new_records = run_reddit_subreddit_pipeline(
                subreddit_name=resolved_value,
                limit=args.count,
                db_path=args.db_path,
            )
        else:
            new_records = run_reddit_post_pipeline(post_id=resolved_value, db_path=args.db_path)

        logger.info("Pipeline finished. Inserted/updated %d record(s).", new_records)
    except Exception:
        logger.exception("Pipeline failed due to an uncaught exception")
        raise


if __name__ == "__main__":
    main()