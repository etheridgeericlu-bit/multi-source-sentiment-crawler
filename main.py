import sys
import re
# Match the filenames in your GitHub screenshot
import applestore
import reddit

def identify_source(input_string):
    """Detects if input is Apple App Store or Reddit."""
    if "apps.apple.com" in input_string or input_string.isdigit():
        return "APPLE"
    if "reddit.com" in input_string or input_string.startswith("r/"):
        return "REDDIT"
    return "UNKNOWN"

def run_dispatcher(user_input):
    source_type = identify_source(user_input)
    print(f"[*] Detected Source: {source_type}")

    if source_type == "APPLE":
        # For Apple, we need an ID. If input is a link, we extract numbers.
        app_id = re.findall(r'\d+', user_input)[0] if not user_input.isdigit() else user_input
        app_name = "target-app"
        
        # Calling functions from applestore.py
        df = applestore.fetch_apple_reviews(app_name=app_name, app_id=app_id, count=100)
        applestore.setup_database()
        applestore.load_data_to_sqlite(df, app_name, app_id)

    elif source_type == "REDDIT":
        # Check if it's a specific URL or a whole community
        if "reddit.com" in user_input:
            df = reddit.fetch_by_url(user_input)
            sub_name = "reddit_post"
        else:
            sub_name = user_input.replace("r/", "")
            df = reddit.fetch_subreddit_posts(sub_name)

        # Calling functions from reddit.py
        reddit.setup_database()
        reddit.load_to_db(df, sub_name, "reddit_auto_id")

    else:
        print("[-] Error: Source not recognized. Use an App ID or r/community name.")

if __name__ == "__main__":
    target = input("Enter Apple App ID, Reddit URL, or Subreddit (e.g. r/technology): ")
    run_dispatcher(target)