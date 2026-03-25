# Multi-Source Sentiment Data Scraper

A powerful and modular Python tool designed to simplify sentiment analysis data collection. This project can automatically detect whether the provided input is an Apple App Store app ID / app URL / Reddit subreddit name / subreddit URL / specific Reddit post URL, and then trigger the corresponding scraping pipeline.

It supports data collection from both **Apple App Store** and **Reddit**, performs basic NLP-oriented text cleaning, and stores the results in a structured **SQLite** database for later analysis.

---

## Features

### Intelligent Dispatcher
Automatically detects the type of input and routes it to the correct scraping pipeline:
- Apple App ID
- Apple App Store URL
- Reddit subreddit name
- Reddit subreddit URL
- Specific Reddit post URL

### Automated Reddit Scraper
Uses **PRAW** (Python Reddit API Wrapper) to:
- scrape hot posts from a subreddit
- scrape comments and content from a specific Reddit post
- store post metadata and cleaned text into the database

### Apple App Store Scraper
Uses **app-store-scraper** to:
- fetch a large number of app reviews efficiently
- process review text for sentiment analysis tasks
- save app and review data into SQLite

### Text Preprocessing for NLP
Automatically cleans raw text by removing:
- URLs and hyperlinks
- emojis and most non-ASCII characters
- excessive whitespace and line breaks

### Relational Database Storage
Saves data into a structured SQLite database (`.db`) with separate tables for:
- Users
- Apps
- Subreddits
- Reviews
- Posts

---

## Attention!!!

**MUST SUBSTITUTE API CODE IN `reddit.py` AND `applestore.py`, OTHERWISE IT WILL RETURN NULL.**

Before running the project, make sure you replace the placeholder API credentials or configuration values in:
- `reddit.py`
- `applestore.py`

If you do not update them properly, the scraper may fail to fetch data and return empty / null results.

---

## Project Structure

```bash
.
├── main.py           # Entry point and dispatcher
├── db_setup.py       # Database schema initialization
├── reddit.py         # Reddit scraping pipeline
├── applestore.py     # Apple App Store scraping pipeline
├── utils.py          # Text cleaning and helper functions
└── README.md