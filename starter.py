import os
from typing import List, Tuple

def find_abc_files(root_folder: str = "abc_books") -> List[Tuple[str, int]]:
    """
    Recursively find all .abc files inside the abc_books folder.

    Returns a list of tuples: (full_file_path, book_number)
    book_number is extracted from the immediate parent folder name.
    """

    abc_files = []

    # Walk through all subfolders and files
    for root, dirs, files in os.walk(root_folder):
        for file in files:
            if file.endswith(".abc"):
                full_path = os.path.join(root, file)

                # Extract book number from the parent folder
                folder_name = os.path.basename(root)

                try:
                    book_number = int(folder_name)
                except ValueError:
                    continue

                abc_files.append((full_path, book_number))

    return abc_files


# ---------------------------------------------------
# DO NOT put this inside the function
# This part runs ONLY when you execute the file
# ---------------------------------------------------
if __name__ == "__main__":
    files = find_abc_files("abc_books")
    for path, book in files:
        print(f"Found in book {book}: {path}")

                
