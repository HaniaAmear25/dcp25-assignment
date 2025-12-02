# Starter code for Data Centric Programming Assignment 2025

import os
import sqlite3
from typing import Any, Dict, List, Optional

import mysql.connector
import pandas as pd

DB_PATH = "tunes.db"
BOOKS_DIR = "abc_books"


def init_db(db_path: str = DB_PATH) -> sqlite3.Connection:
    """Initialise the SQLite database and create the tunes table if needed."""

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS tunes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            book INTEGER,
            file_path TEXT,
            x INTEGER,
            title TEXT,
            rhythm TEXT,
            meter TEXT,
            key TEXT,
            raw_text TEXT
        )
        """
    )

    conn.commit()
    return conn


def clear_tunes_table(conn: sqlite3.Connection) -> None:
    """Delete all rows from the tunes table (used when rebuilding the DB)."""

    cursor = conn.cursor()
    cursor.execute("DELETE FROM tunes")
    conn.commit()


def parse_abc_file(file_path: str, book_number: int) -> List[Dict[str, Any]]:
    """Parse an ABC file into a list of tune dictionaries.

    This is a simple parser that looks for header lines like X:, T:, R:, M:, K:
    and groups lines into tunes separated by new X: fields.
    """

    with open(file_path, "r", encoding="utf-8") as f:
        lines = [line.rstrip("\n") for line in f]

    tunes: List[Dict[str, Any]] = []
    current: Dict[str, Any] = {}
    body_lines: List[str] = []

    def finish_current() -> None:
        if current:
            current["raw_text"] = "\n".join(body_lines)
            tunes.append(current.copy())

    for line in lines:
        if line.startswith("X:"):
            # Start of a new tune
            finish_current()
            current = {
                "book": book_number,
                "file_path": file_path,
                "x": _safe_int(line[2:].strip()),
                "title": None,
                "rhythm": None,
                "meter": None,
                "key": None,
            }
            body_lines = [line]
        elif not current:
            # Skip lines before the first X: header
            continue
        else:
            body_lines.append(line)
            if line.startswith("T:"):
                current["title"] = line[2:].strip()
            elif line.startswith("R:"):
                current["rhythm"] = line[2:].strip()
            elif line.startswith("M:"):
                current["meter"] = line[2:].strip()
            elif line.startswith("K:"):
                current["key"] = line[2:].strip()

    finish_current()
    return tunes


def _safe_int(value: str) -> Optional[int]:

    try:
        return int(value)
    except ValueError:
        return None
