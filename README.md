# Multi-Source Sentiment Data Crawler

A robust, modular Python tool designed to streamline data collection for sentiment analysis. This project automatically identifies the source of a provided link or ID and triggers the appropriate scraping pipeline for either the **Apple App Store** or **Reddit**.

---

## 🚀 Features

* **Smart Dispatcher**: Automatically detects if the input is an Apple App ID, a Subreddit name, or a specific Reddit URL.
* **Automated Reddit Scraper**: Uses the PRAW (Python Reddit API Wrapper) to crawl "Hot" posts or specific discussion threads.
* **Apple App Store Scraper**: Efficiently fetches thousands of reviews using the `app-store-scraper` library.
* **NLP-Ready Preprocessing**: Automatically cleans text by removing:
    * URLs and Hyperlinks
    * Emojis and non-ASCII characters
    * Excessive whitespace and line breaks
* **Relational Database Storage**: Saves data into a structured SQLite database (`.db`) with separate tables for Users, Apps/Subreddits, and Content (Posts/Reviews).

---

## 🛠️ Tech Stack

* **Language**: Python 3.x
* **Database**: SQLite
* **Key Libraries**: 
    * `pandas` (Data Manipulation)
    * `praw` (Reddit API)
    * `app-store-scraper` (Apple App Store API)
    * `demoji` (Emoji removal)
    * `re` (Regular Expressions for cleaning)

---

## 📦 Installation & Setup

1. **Clone the Repository**
   ```bash
   git clone [https://github.com/YOUR_USERNAME/YOUR_REPO_NAME.git](https://github.com/YOUR_USERNAME/YOUR_REPO_NAME.git)
   cd YOUR_REPO_NAME