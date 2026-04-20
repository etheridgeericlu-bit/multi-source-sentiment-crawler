"""
Text preprocessing utilities for sentiment analysis data pipelines.

The module provides a layered approach so that callers can reach for the depth
of cleaning they actually need:

  1. ``clean_text``      - surface cleaning (URLs, emojis, unicode, punctuation).
                           Kept backward compatible with earlier versions of the
                           pipeline that imported it directly.
  2. ``normalize_text``  - character-level normalization (repeated letters,
                           repeated punctuation, English contractions, smart
                           quotes).
  3. ``assess_text``     - compute quality metadata (length, word count,
                           detected language, low-signal flag with reasons,
                           quality score in [0, 1]).
  4. ``preprocess_text`` - end-to-end: clean -> normalize -> assess. Returns a
                           ``TextQuality`` dataclass ready to be persisted as
                           DB columns alongside the cleaned text.

Design notes
------------
* CJK reduplication ("哈哈哈") is preserved: repeated-character collapsing is
  restricted to ASCII letters so that Chinese sentiment cues are not damaged.
* ``langdetect`` is imported lazily; if it is not installed the pipeline still
  works and simply records ``language=None``.
* All thresholds are module-level constants so that they can be tuned or unit
  tested without touching call sites.
"""

from __future__ import annotations

import logging
import re
import unicodedata
from dataclasses import dataclass
from typing import Optional

import emoji

try:
    from langdetect import DetectorFactory, LangDetectException, detect

    # Deterministic results across runs (langdetect is non-deterministic by default).
    DetectorFactory.seed = 0
    _LANGDETECT_AVAILABLE = True
except ImportError:  # pragma: no cover - optional dependency
    _LANGDETECT_AVAILABLE = False

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tunable thresholds
# ---------------------------------------------------------------------------

#: Reviews shorter than this (after cleaning) are flagged as too short.
MIN_CHARS: int = 10

#: Reviews with fewer tokens than this are flagged as having too few words.
MIN_WORDS: int = 3

#: If the ratio of alphabetic characters drops below this, the text is
#: considered non-alphabetic (e.g. "!!!", "12345", "????").
MIN_ALPHA_RATIO: float = 0.30

#: Minimum text length required before we attempt language detection.
#: langdetect is unreliable on very short snippets.
LANGDETECT_MIN_CHARS: int = 20

#: Phrases that are technically valid English but convey no sentiment signal.
#: Matched case-insensitively against the *stripped* cleaned text.
LOW_SIGNAL_PHRASES: frozenset[str] = frozenset(
    {
        "",
        "first",
        "test",
        "testing",
        "hi",
        "hello",
        "ok",
        "okay",
        "k",
        "good",
        "bad",
        "nice",
        "cool",
        "lol",
        "meh",
        "yes",
        "no",
        "na",
        "wow",
        "n/a",
    }
)

#: Common English contractions. Applied case-insensitively; the suffix forms
#: (``n't``, ``'re`` ...) are intentionally ordered after the exact forms so
#: the specific rules win.
_CONTRACTIONS: tuple[tuple[str, str], ...] = (
    (r"\bwon't\b", "will not"),
    (r"\bcan't\b", "cannot"),
    (r"\bshan't\b", "shall not"),
    (r"\b(\w+)n't\b", r"\1 not"),
    (r"\b(\w+)'re\b", r"\1 are"),
    (r"\b(\w+)'ve\b", r"\1 have"),
    (r"\b(\w+)'ll\b", r"\1 will"),
    (r"\b(\w+)'d\b", r"\1 would"),
    (r"\b(\w+)'m\b", r"\1 am"),
    # "'s" intentionally left alone because it can be possessive OR "is" and
    # the wrong expansion hurts sentiment signal more than leaving it does.
)

# Pre-compile regexes that run on every row for speed.
_URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
_WHITESPACE_RE = re.compile(r"\s+")
_NON_TEXT_RE = re.compile(r"[^\w\s!?,.'\-]", re.UNICODE)
_REPEATED_ASCII_LETTER_RE = re.compile(r"([A-Za-z])\1{2,}")
_REPEATED_PUNCT_RE = re.compile(r"([!?.,])\1+")
_CJK_RE = re.compile(r"[\u4e00-\u9fff]")

_SMART_QUOTES = {
    "\u2018": "'",
    "\u2019": "'",
    "\u201a": "'",
    "\u201b": "'",
    "\u201c": '"',
    "\u201d": '"',
    "\u201e": '"',
    "\u201f": '"',
    "\u2032": "'",
    "\u2033": '"',
}
_SMART_QUOTES_TABLE = str.maketrans(_SMART_QUOTES)


# ---------------------------------------------------------------------------
# Dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TextQuality:
    """The output of ``preprocess_text``.

    Stored alongside the review/post so downstream labeling and training
    pipelines can filter, weight, or stratify by data quality.
    """

    cleaned_text: str
    char_length: int
    word_count: int
    language: Optional[str]
    is_low_signal: bool
    quality_score: float  # 0.0 (worst) to 1.0 (best)
    reasons: str  # comma-separated reason codes; empty when is_low_signal is False


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _expand_contractions(text: str) -> str:
    for pattern, replacement in _CONTRACTIONS:
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
    return text


def _collapse_repeated_letters(text: str) -> str:
    """Collapse 3+ consecutive ASCII letters to 2 ("goooood" -> "good").

    Restricted to ASCII letters so CJK reduplication ("哈哈哈", "好好") is
    preserved.
    """
    return _REPEATED_ASCII_LETTER_RE.sub(r"\1\1", text)


def _collapse_repeated_punctuation(text: str) -> str:
    """Collapse runs of the same punctuation ("!!!!" -> "!")."""
    return _REPEATED_PUNCT_RE.sub(r"\1", text)


def _count_words(text: str) -> int:
    """Token count that is robust to CJK text.

    For text dominated by CJK characters we fall back to a character-based
    count because whitespace tokenization under-counts those scripts.
    """
    if not text:
        return 0
    whitespace_tokens = text.split()
    cjk_chars = len(_CJK_RE.findall(text))
    if cjk_chars and cjk_chars / max(len(text), 1) > 0.3:
        # Heuristic: every 1-2 CJK chars ~= one word.
        return max(len(whitespace_tokens), cjk_chars)
    return len(whitespace_tokens)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def clean_text(text: str) -> str:
    """Light surface cleaning. Preserved for backward compatibility."""
    if not isinstance(text, str) or not text.strip():
        return ""

    # Smart quotes -> straight quotes so that later contraction expansion works.
    text = text.translate(_SMART_QUOTES_TABLE)

    # URLs and emojis first: they contain characters we don't want to
    # accidentally keep during the punctuation filter step below.
    text = _URL_RE.sub(" ", text)
    text = emoji.replace_emoji(text, replace=" ")

    # Unicode normalization (compatibility form) but no ASCII coercion so we
    # keep non-English characters intact.
    text = unicodedata.normalize("NFKC", text)

    # Keep word characters (letters/digits across scripts), whitespace, and a
    # small set of punctuation useful for sentiment.
    text = _NON_TEXT_RE.sub(" ", text)

    return _WHITESPACE_RE.sub(" ", text).strip()


def normalize_text(text: str) -> str:
    """Character-level normalization on top of ``clean_text`` output."""
    if not text:
        return ""

    text = _expand_contractions(text)
    text = _collapse_repeated_letters(text)
    text = _collapse_repeated_punctuation(text)
    return _WHITESPACE_RE.sub(" ", text).strip()


def detect_language(text: str) -> Optional[str]:
    """Best-effort ISO 639-1 language code; ``None`` when undetermined."""
    if not _LANGDETECT_AVAILABLE or not text or len(text) < LANGDETECT_MIN_CHARS:
        return None
    try:
        return detect(text)
    except LangDetectException:
        return None


def assess_text(cleaned: str) -> TextQuality:
    """Compute quality metadata for an already-cleaned piece of text."""
    char_length = len(cleaned)
    word_count = _count_words(cleaned)
    language = detect_language(cleaned)

    alpha_chars = sum(1 for c in cleaned if c.isalpha())
    alpha_ratio = alpha_chars / max(char_length, 1)

    reasons: list[str] = []
    if char_length == 0:
        reasons.append("empty")
    else:
        if char_length < MIN_CHARS:
            reasons.append("too_short")
        if word_count < MIN_WORDS:
            reasons.append("too_few_words")
        if alpha_ratio < MIN_ALPHA_RATIO:
            reasons.append("non_alphabetic")
        if cleaned.strip(" .!?,-").lower() in LOW_SIGNAL_PHRASES:
            reasons.append("low_signal_phrase")

    is_low_signal = bool(reasons)

    # Quality score: a bounded combination of length and alpha density.
    # Anything flagged as low-signal is heavily discounted so the score is a
    # usable single-number filter for downstream code.
    base_score = min(1.0, word_count / 20.0) * 0.7 + alpha_ratio * 0.3
    if is_low_signal:
        base_score *= 0.3

    return TextQuality(
        cleaned_text=cleaned,
        char_length=char_length,
        word_count=word_count,
        language=language,
        is_low_signal=is_low_signal,
        quality_score=round(base_score, 3),
        reasons=",".join(reasons),
    )


def preprocess_text(text: str) -> TextQuality:
    """End-to-end preprocessing: ``clean -> normalize -> assess``."""
    cleaned = clean_text(text)
    normalized = normalize_text(cleaned)
    return assess_text(normalized)
