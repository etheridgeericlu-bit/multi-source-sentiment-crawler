import re
from urllib.parse import urlparse

import applestore
import reddit

DEFAULT_DB_PATH = "sentiment_pipeline.db"
APPLE_URL_ID_RE = re.compile(r'/id(\d+)')


def identify_source(input_string):
    """Detect whether the input is Apple App Store or Reddit."""
    input_string = input_string.strip()
    if "apps.apple.com" in input_string or input_string.isdigit():
        return "APPLE"
    if "reddit.com" in input_string or input_string.startswith("r/"):
        return "REDDIT"
    return "UNKNOWN"



def extract_apple_app_id(user_input):
    user_input = user_input.strip()
    if user_input.isdigit():
        return user_input

    match = APPLE_URL_ID_RE.search(user_input)
    if not match:
        raise ValueError("Could not extract Apple App ID from the provided URL.")
    return match.group(1)



def extract_apple_app_name(user_input, fallback_app_id):
    """Try to infer the app slug from an Apple URL; otherwise fall back to app-{id}."""
    if 'apps.apple.com' not in user_input:
        return f"app-{fallback_app_id}"

    path_parts = [part for part in urlparse(user_input).path.split('/') if part]
    if len(path_parts) >= 3:
        return path_parts[-2]
    return f"app-{fallback_app_id}"



def normalize_subreddit_name(user_input):
    user_input = user_input.strip()
    if user_input.startswith('r/'):
        return user_input[2:]
    if 'reddit.com' in user_input:
        parsed = urlparse(user_input)
        match = re.search(r'/r/([^/]+)/?', parsed.path)
        if match:
            return match.group(1)
    return user_input



def run_dispatcher(user_input, db_path=DEFAULT_DB_PATH, apple_count=100, reddit_limit=50):
    user_input = user_input.strip()
    source_type = identify_source(user_input)
    print(f"[*] Detected Source: {source_type}")

    if source_type == "APPLE":
        app_id = extract_apple_app_id(user_input)
        app_name = extract_apple_app_name(user_input, app_id)

        applestore.setup_database(db_path=db_path)
        df = applestore.fetch_apple_reviews(app_name=app_name, app_id=app_id, count=apple_count)
        inserted = applestore.load_data_to_sqlite(df, app_name, app_id, db_path=db_path)
        print(f"[+] Finished Apple pipeline. Inserted {inserted} rows into {db_path}.")
        return df

    if source_type == "REDDIT":
        reddit.setup_database(db_path=db_path)
        if "reddit.com" in user_input:
            df = reddit.fetch_by_url(user_input)
        else:
            sub_name = normalize_subreddit_name(user_input)
            df = reddit.fetch_subreddit_posts(sub_name, post_limit=reddit_limit)

        inserted = reddit.load_to_db(df, db_path=db_path)
        print(f"[+] Finished Reddit pipeline. Inserted {inserted} rows into {db_path}.")
        return df

    raise ValueError("Source not recognized. Use an Apple App Store URL/App ID, a Reddit URL, or r/community name.")


if __name__ == "__main__":
    target = input("Enter Apple App ID, Apple URL, Reddit URL, or Subreddit (e.g. r/technology): ")
    try:
        run_dispatcher(target)
    except Exception as exc:
        print(f"[-] Error: {exc}")
