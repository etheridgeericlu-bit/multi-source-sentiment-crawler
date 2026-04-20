"""
Data-health verification utility.

Prints a compact report of the current state of the pipeline's SQLite store.
Meant to satisfy the "basic verification queries or utilities to inspect data
health and completeness" deliverable from the Phase-I project brief, and to
give a reviewer a one-glance picture of how effective the cleaning layer is.

Usage::

    python quality_report.py
    python quality_report.py --db-path /path/to/sentiment_pipeline.db
    python quality_report.py --show-examples 5
"""

from __future__ import annotations

import argparse
import sqlite3
from typing import List, Sequence, Tuple

DEFAULT_DB_PATH = "sentiment_pipeline.db"


def _run(cursor: sqlite3.Cursor, sql: str, params: Sequence = ()) -> List[Tuple]:
    cursor.execute(sql, params)
    return cursor.fetchall()


def _print_header(title: str) -> None:
    bar = "=" * len(title)
    print(f"\n{bar}\n{title}\n{bar}")


def _print_table(rows: List[Tuple], headers: Sequence[str]) -> None:
    if not rows:
        print("  (no data)")
        return
    widths = [len(h) for h in headers]
    for row in rows:
        for idx, value in enumerate(row):
            widths[idx] = max(widths[idx], len(str(value)))
    fmt = "  " + " | ".join(f"{{:<{w}}}" for w in widths)
    print(fmt.format(*headers))
    print("  " + "-+-".join("-" * w for w in widths))
    for row in rows:
        print(fmt.format(*(str(v) if v is not None else "-" for v in row)))


def _summary_for(cursor: sqlite3.Cursor, table: str, text_col: str) -> None:
    _print_header(f"{table} - overview")
    total = _run(cursor, f"SELECT COUNT(*) FROM {table}")[0][0]
    if total == 0:
        print("  (table is empty)")
        return
    low_signal = _run(cursor, f"SELECT COUNT(*) FROM {table} WHERE is_low_signal = 1")[0][0]
    avg_quality = _run(cursor, f"SELECT AVG(quality_score) FROM {table}")[0][0]
    avg_len = _run(cursor, f"SELECT AVG(cleaned_length) FROM {table}")[0][0]
    avg_words = _run(cursor, f"SELECT AVG(word_count) FROM {table}")[0][0]

    print(f"  rows                 : {total}")
    print(f"  low_signal rows      : {low_signal} ({(low_signal / total) * 100:.1f}%)")
    print(f"  avg quality_score    : {avg_quality:.3f}" if avg_quality is not None else "  avg quality_score    : -")
    print(f"  avg cleaned_length   : {avg_len:.1f}" if avg_len is not None else "  avg cleaned_length   : -")
    print(f"  avg word_count       : {avg_words:.1f}" if avg_words is not None else "  avg word_count       : -")

    _print_header(f"{table} - language distribution")
    rows = _run(
        cursor,
        f"""
        SELECT COALESCE(language, 'unknown') AS lang, COUNT(*) AS n,
               ROUND(AVG(quality_score), 3) AS avg_q
        FROM {table}
        GROUP BY lang
        ORDER BY n DESC
        LIMIT 10
        """,
    )
    _print_table(rows, ("language", "count", "avg_quality"))

    _print_header(f"{table} - low-signal reason breakdown")
    rows = _run(
        cursor,
        f"""
        SELECT COALESCE(NULLIF(low_signal_reasons, ''), '(clean)') AS reason,
               COUNT(*) AS n
        FROM {table}
        WHERE is_low_signal = 1
        GROUP BY reason
        ORDER BY n DESC
        LIMIT 10
        """,
    )
    _print_table(rows, ("reasons", "count"))

    _print_header(f"{table} - quality_score histogram (0.1 buckets)")
    rows = _run(
        cursor,
        f"""
        SELECT CAST(quality_score * 10 AS INTEGER) AS bucket, COUNT(*) AS n
        FROM {table}
        WHERE quality_score IS NOT NULL
        GROUP BY bucket
        ORDER BY bucket
        """,
    )
    display = [
        (f"[{b / 10:.1f} - {(b + 1) / 10:.1f})" if b < 10 else "[1.0]", n) for b, n in rows
    ]
    _print_table(display, ("range", "count"))


def _show_examples(cursor: sqlite3.Cursor, table: str, text_col: str, n: int) -> None:
    if n <= 0:
        return

    _print_header(f"{table} - sample low-signal rows (n={n})")
    rows = _run(
        cursor,
        f"""
        SELECT quality_score, low_signal_reasons, SUBSTR({text_col}, 1, 120)
        FROM {table}
        WHERE is_low_signal = 1
        ORDER BY quality_score ASC
        LIMIT ?
        """,
        (n,),
    )
    _print_table(rows, ("quality", "reasons", "text preview"))

    _print_header(f"{table} - sample high-quality rows (n={n})")
    rows = _run(
        cursor,
        f"""
        SELECT quality_score, language, SUBSTR({text_col}, 1, 120)
        FROM {table}
        WHERE is_low_signal = 0
        ORDER BY quality_score DESC
        LIMIT ?
        """,
        (n,),
    )
    _print_table(rows, ("quality", "lang", "text preview"))


def generate_report(db_path: str = DEFAULT_DB_PATH, show_examples: int = 0) -> None:
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        for table, text_col in (("Reviews", "review_text"), ("Posts", "post_text")):
            _summary_for(cursor, table, text_col)
            _show_examples(cursor, table, text_col, show_examples)


def main() -> None:
    parser = argparse.ArgumentParser(description="Data-quality report for the sentiment pipeline DB")
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH, help="SQLite database path")
    parser.add_argument(
        "--show-examples",
        type=int,
        default=0,
        help="Also show N sample rows at each end of the quality spectrum.",
    )
    args = parser.parse_args()
    generate_report(db_path=args.db_path, show_examples=args.show_examples)


if __name__ == "__main__":
    main()
