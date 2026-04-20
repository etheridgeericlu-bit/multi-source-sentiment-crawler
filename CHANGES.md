# Phase-I follow-up: Text cleaning & data-quality layer

This iteration addresses the reviewer's request for **deeper text cleaning and
preprocessing** before we scale the pipeline further. The focus was on
making progress on three concrete data-quality issues — low-signal filtering,
normalization, and filtering — and on making that progress **measurable** so
we can see the effect of cleaning choices in the database itself.

## What changed

### 1. Layered preprocessing in `utils.py`

The old `clean_text` did URL / emoji / unicode / whitespace cleanup in one
step. That has been split into a four-stage pipeline:

1. **`clean_text`** — URL removal, emoji removal, smart-quote folding,
   NFKC unicode normalization, punctuation filtering. *Back-compatible with
   the previous signature so existing imports continue to work.*
2. **`normalize_text`** — repeated-character collapsing (`"goooood"` → `"good"`,
   `"!!!!"` → `"!"`), English contractions expanded (`"don't"` → `"do not"`).
   ASCII-only collapse so CJK reduplication (`"哈哈哈"`, `"好好"`) is preserved.
3. **`detect_language`** — best-effort ISO 639-1 language code via
   `langdetect`. Falls back to `None` when the dependency is absent or the
   text is too short to classify confidently.
4. **`assess_text`** — emits a `TextQuality` dataclass with
   `cleaned_text, char_length, word_count, language, is_low_signal,
   quality_score, reasons`.

Low-signal detection combines four orthogonal rules:

| reason code         | trigger                                          |
| ------------------- | ------------------------------------------------ |
| `too_short`         | cleaned length < 10 chars                        |
| `too_few_words`     | word count < 3                                   |
| `non_alphabetic`    | alphabetic ratio < 30 %                          |
| `low_signal_phrase` | exact-match against a curated stop list ("good", "test", "first", "lol", ...) |
| `empty`             | nothing left after cleaning                      |

`quality_score ∈ [0, 1]` is a length/alpha-density blend, heavily discounted
when any low-signal rule fires. It is a single number downstream labeling or
training code can sort / threshold on.

### 2. Quality metadata persisted in SQLite

`Reviews` and `Posts` now carry six new columns, populated on every insert:

```
cleaned_length      INTEGER
word_count          INTEGER
language            TEXT          -- e.g. 'en', 'zh-cn'
quality_score       REAL          -- 0.0 .. 1.0
is_low_signal       INTEGER       -- 0 / 1
low_signal_reasons  TEXT          -- comma-separated reason codes
```

Indexes added on `language` and `is_low_signal` for fast filtering.

`db_setup.py` is **idempotent and migration-aware**: on startup it detects any
missing columns and issues `ALTER TABLE ADD COLUMN` so an existing database
upgrades in place without data loss. Verified with a round-trip test that
starts from the old schema.

### 3. In-batch deduplication & optional filtering

During ingestion we now:

- drop rows whose cleaned text is empty (no signal at all);
- deduplicate **identical review / post from the same user within one fetch**
  (common spam pattern);
- optionally drop low-signal rows, rows below a quality threshold, or rows
  outside a language whitelist.

These filters are exposed as CLI flags so we can experiment without code
changes.

### 4. New CLI flags in `main.py`

```
--drop-low-signal          Drop items flagged as low-signal.
--min-quality 0.2          Drop items with quality_score < 0.2.
--keep-languages en,zh     Keep only these detected languages
                            (rows with language=None are always kept).
```

Default behavior is unchanged: without any of these flags we clean, annotate,
and store every row — nothing is dropped silently.

### 5. Ingestion logs a per-run health snapshot

Each pipeline run now emits a compact stats line per stage, e.g.

```
[after_clean] rows=187 | low_signal=24 | avg_quality=0.612 | langs={'en': 171, None: 14, 'zh-cn': 2}
[final]       rows=159 | low_signal=0  | avg_quality=0.681 | langs={'en': 147, None: 10, 'zh-cn': 2}
```

so the effect of cleaning and filtering is visible directly in stdout.

### 6. New `quality_report.py` utility

A standalone CLI that queries the DB and prints an auditable data-health
report:

- total / low-signal counts per table, with percentages
- language distribution (top 10)
- reason-code breakdown for low-signal rows
- `quality_score` histogram in 0.1 buckets
- optional sample of the lowest and highest quality rows for eyeballing

```
python quality_report.py --show-examples 5
```

This satisfies the "basic verification queries or utilities to inspect data
health and completeness" bullet in the original project brief, and makes it
easy to compare dataset health before / after tuning cleaning thresholds.

## Why this is the right next step

The reviewer asked for a **stronger baseline** before scaling. The changes
above:

- **Raise the quality floor** of what goes into the DB by identifying and
  optionally removing content that carries no sentiment signal.
- **Preserve optionality**: nothing is deleted by default. Rows are annotated
  rather than discarded so cleaning thresholds can be tuned after the fact
  without re-scraping.
- **Make quality measurable**: per-run logs, per-row metadata, and a
  standalone report turn cleaning from an invisible regex step into a
  first-class, inspectable part of the pipeline.

## Files touched

```
utils.py              rewritten (layered API + TextQuality dataclass)
db_setup.py           +6 quality columns, idempotent migration, +4 indexes
applestore.py         uses preprocess_text, in-batch dedup, filter knobs, stats log
reddit.py             uses preprocess_text, in-batch dedup, filter knobs, stats log
main.py               new CLI flags --drop-low-signal / --min-quality / --keep-languages
quality_report.py     NEW - data-health verification utility
```

`utils.clean_text` keeps its original signature, so anything else importing
it continues to work without changes.
