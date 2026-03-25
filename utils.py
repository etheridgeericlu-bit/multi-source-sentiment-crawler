import logging
import re
import unicodedata

import emoji

logger = logging.getLogger(__name__)


def clean_text(text: str) -> str:
    """Clean text for sentiment analysis while preserving non-English text."""
    if not isinstance(text, str) or not text.strip():
        return ""

    # Remove URLs first.
    text = re.sub(r"https?://\S+|www\.\S+", " ", text)

    # Remove emojis instead of converting them into token strings.
    text = emoji.replace_emoji(text, replace=" ")

    # Normalize unicode without forcing ASCII.
    text = unicodedata.normalize("NFKC", text)

    # Keep letters/numbers across languages, whitespace, and a small set of punctuation useful for sentiment.
    text = re.sub(r"[^\w\s!?,.'\-]", " ", text, flags=re.UNICODE)

    # Normalize whitespace.
    text = re.sub(r"\s+", " ", text).strip()
    return text