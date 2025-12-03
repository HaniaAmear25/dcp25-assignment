# Starter code for Data Centric Programming Assignment 2025

import os
import sqlite3
from typing import Any, Dict, List, Optional

import mysql.connector
import pandas as pd
from tabulate import tabulate


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
    """Convert a string to int, returning None if it fails."""

    try:
        return int(value)
    except ValueError:
        return None


def insert_tunes(conn: sqlite3.Connection, tunes: List[Dict[str, Any]]) -> None:
    """Insert a list of tune dictionaries into the tunes table."""

    cursor = conn.cursor()
    rows = [
        (
            tune.get("book"),
            tune.get("file_path"),
            tune.get("x"),
            tune.get("title"),
            tune.get("rhythm"),
            tune.get("meter"),
            tune.get("key"),
            tune.get("raw_text"),
        )
        for tune in tunes
    ]

    cursor.executemany(
        """
        INSERT INTO tunes (book, file_path, x, title, rhythm, meter, key, raw_text)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()


def build_database_from_files(books_dir: str = BOOKS_DIR, db_path: str = DB_PATH) -> None:
    """Traverse abc_books, parse ABC files and populate the SQLite database."""

    conn = init_db(db_path)
    clear_tunes_table(conn)

    total_files = 0
    total_tunes = 0

    # Iterate over directories in abc_books
    for item in os.listdir(books_dir):
        # item is the dir name, this makes it into a path
        item_path = os.path.join(books_dir, item)

        # Check if it's a directory and has a numeric name
        if os.path.isdir(item_path) and item.isdigit():
            book_number = int(item)
            print(f"Found numbered directory (book): {item}")

            # Iterate over files in the numbered directory
            for file_name in os.listdir(item_path):
                # Check if file has .abc extension
                if file_name.endswith(".abc"):
                    file_path = os.path.join(item_path, file_name)
                    print(f"  Parsing abc file: {file_name}")
                    tunes = parse_abc_file(file_path, book_number)
                    insert_tunes(conn, tunes)
                    total_files += 1
                    total_tunes += len(tunes)

    conn.close()
    print(f"Finished. Processed {total_files} files and inserted {total_tunes} tunes.")


def load_tunes_dataframe(db_path: str = DB_PATH) -> pd.DataFrame:
    """Load all tunes from the SQLite database into a pandas DataFrame."""

    conn = sqlite3.connect(db_path)
    df = pd.read_sql("SELECT * FROM tunes", conn)
    conn.close()
    return df


def get_tunes_by_book(df: pd.DataFrame, book_number: int) -> pd.DataFrame:
    """Get all tunes from a specific book."""

    return df[df["book"] == book_number]


def get_tunes_by_type(df: pd.DataFrame, tune_type: str) -> pd.DataFrame:
    """Get all tunes of a specific type (uses the rhythm column)."""

    return df[df["rhythm"].str.lower() == tune_type.lower()]


def search_tunes(df: pd.DataFrame, search_term: str) -> pd.DataFrame:
    """Search tunes by title case insensitive."""

    mask = df["title"].fillna("").str.contains(search_term, case=False, na=False)
    return df[mask]


def _print_results(df: pd.DataFrame, max_rows: int = 20) -> None:
    if df.empty:
        print("No results.")
        return

    
    if "raw_text" in df.columns:
        df = df.drop(columns=["raw_text"])

    print(tabulate(df.head(max_rows), headers="keys", tablefmt="fancy_grid"))




def run_menu(db_path: str = DB_PATH) -> None:
    """Simple text-based user interface for querying the tunes database."""

    while True:
        print("\n=== ABC Tunes Database ===")
        print("1. (Re)build database from abc_books")
        print("2. Show tunes by book")
        print("3. Show tunes by type (rhythm)")
        print("4. Search tunes by title")
        print("5. Show basic stats (tunes per book)")
        print("0. Quit")

        choice = input("Enter choice: ").strip()

        if choice == "1":
            build_database_from_files()
        elif choice == "2":
            try:
                book_str = input("Enter book number: ").strip()
                book_number = int(book_str)
            except ValueError:
                print("Invalid book number.")
                continue
            df = load_tunes_dataframe(db_path)
            result = get_tunes_by_book(df, book_number)
            _print_results(result)
        elif choice == "3":
            tune_type = input("Enter tune type (e.g. reel, jig): ").strip()
            df = load_tunes_dataframe(db_path)
            result = get_tunes_by_type(df, tune_type)
            _print_results(result)
        elif choice == "4":
            term = input("Enter search term for title: ").strip()
            df = load_tunes_dataframe(db_path)
            result = search_tunes(df, term)
            _print_results(result)
        elif choice == "5":
            df = load_tunes_dataframe(db_path)
            if df.empty:
                print("Database is empty. Build it first.")
            else:
                stats = df.groupby("book")["id"].count().rename("tune_count").reset_index()
                _print_results(stats)
        elif choice == "0":
            print("Goodbye!")
            break
        else:
            print("Unknown choice, please try again.")


if __name__ == "__main__":
    # Entry point for running the assignment code.
    # You can comment out run_menu and call individual functions when testing.
    run_menu()