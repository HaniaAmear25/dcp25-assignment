import os
import sqlite3
import pandas as pd
import mysql.connector



def find_abc_files(books_dir: str = "abc_books"):
    """
    Traverse the abc_books folder and return a list of dictionaries,
    each containing:
        - full file path
        - file name
        - book number
    """
    abc_file_list = []

    for item in os.listdir(books_dir):
        item_path = os.path.join(books_dir, item)

        if os.path.isdir(item_path) and item.isdigit():
            book_number = int(item)

            for file in os.listdir(item_path):
                if file.endswith(".abc"):
                    file_path = os.path.join(item_path, file)

                    abc_file_list.append({
                        "book_number": book_number,
                        "file_name": file,
                        "full_path": file_path
                    })

    return abc_file_list



def do_databasse_stuff():
    pass

def my_sql_database():
    pass

def process_file(file):
    pass


#execution
if __name__ == "__main__":
    abc_files = find_abc_files()

    print("Found the following ABC files:")
    for entry in abc_files:
        print(entry)
