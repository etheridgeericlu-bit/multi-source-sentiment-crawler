import re
import sys
# Import your custom modules
import apple_scraper
import reddit_scraper

def identify_source(input_string):
    """
    Determines if the input is for Apple App Store or Reddit.
    """
    # Check for Apple App Store URL or numeric App ID
    if "apps.apple.com" in input_string or input_string.isdigit():
        return "APPLE"
    
    # Check for Reddit URL or Subreddit shorthand (r/name)
    if "reddit.com" in input_string or input_string.startswith("r/"):
        return "REDDIT"
    
    # Default/Unknown
    return "UNKNOWN"

def run_dispatcher(user_input):
    source_type = identify_source(user_input)
    
    print(f"[*] Detected Source Type: {source_type}")
    
    if source_type == "APPLE":
        print("[!] For Apple, we usually need both a name and an ID.")
        # If input is a link, you might need a helper to extract the ID
        # For now, we use placeholders to show logic flow
        app_id = user_input if user_input.isdigit() else "ENTER_ID_HERE"
        app_name = "generic-app-name" 
        
        df = apple_scraper.fetch_apple_reviews(app_name=app_name, app_id=app_id)
        apple_scraper.setup_database()
        apple_scraper.load_data_to_sqlite(df, app_name, app_id)
        
    elif source_type == "REDDIT":
        # Extract subreddit name if it starts with r/
        sub_name = user_input.replace("r/", "") if user_input.startswith("r/") else user_input
        # If it's a URL, you would use the URL-specific fetcher we discussed earlier
        if "reddit.com" in user_input:
             df = reddit_scraper.fetch_by_url(user_input)
        else:
             df = reddit_scraper.fetch_subreddit_posts(sub_name)
             
        reddit_scraper.setup_database()
        reddit_scraper.load_to_db(df, sub_name, "auto_gen_id")
        
    else:
        print("[-] Error: Could not determine if this is a Reddit or Apple link.")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        target = sys.argv[1]
    else:
        target = input("Enter a Reddit URL, Subreddit (r/name), or Apple App ID: ")
    
    run_dispatcher(target)