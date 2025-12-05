# Data Centric Programming Assignment 2025

- [Assignment Brief](assignment.md)

Name: Hania Amear

Student Number: C24405892
Lecturer: Bryan Duggan

# Screenshots

![image](images/abc.png)



# Description of the project
A Python application for managing traditional music tunes in ABC notation. Parses ABC files, stores them in SQLite, and provides an interactive interface to search and analyze tunes by book, type, or title.


# Instructions for use
Place your ABC notation files in the abc_books directory. run the starter_code.py make sure to install required libraries. you will be presented with a interactive menu.Choose the option you want to see run.

# How it works:

Database Connection:
i used SQLite to store tune info. 
The init_db() function establishes a connection to the tunes.db file and creates a table with columns for book number, file path, tune number, title, rhythm, meter, key, and the complete raw ABC notation text.Database connections are opened at the start of each function that needs database access.

File Processing:
The build_database_from_files() function scans the abc_books directory for numbered folders, where each folder represents a book collection. For each ABC file found, the parse_abc_file() function reads it line by line, identifying individual tunes by their "X:" headers and extracting metadata like title, rhythm, meter, and key from the ABC notation tags.

Data Analysis:
load_tunes_dataframe() function loads all tune data into a pandas DataFrame.
Allowing for:
filtering, case insensitive searches(strings),stats.



# List of files in the project

| Files | Source |
|-----------|-----------|
| starter_code.py| main code |
| abc_books_folder| from assignment |
| assignment.md | From assignment) |
| image folder| from assignment |
| LICENSE| from assignment |
| .gitignore | From assignment |


# References
Bryan Duggan Lecture Notes
Python Tabulate:
https://www.datacamp.com/tutorial/python-tabulate
Accessing SQLite Databases Using Python and Pandas:
https://datacarpentry.github.io/python-ecology-lesson/instructor/09-working-with-sql.html
Python sqlite:
https://www.geeksforgeeks.org/python/python-sqlite/
Type Hints:
https://www.geeksforgeeks.org/python/type-hints-in-python/





# What I am most proud of in the assignment
I didn't know how to use GitHub before starting this assignment. I attended a hackathon by Give(a)Go where there were developers at many different stages of their careers. My friends and I were the youngest and least experienced developers there, but we decided to work on this assignment during the hackathon. The more experienced developers were incredibly helpful and patient, teaching us Git basics, debugging strategies, and best practices for structuring code. This support allowed me to push through the challenging parts and complete a project I wouldn't have thought possible at the start.
I'm particularly proud of integrating multiple technologies:file parsing, SQLite, pandas, and tabulate into a cohesive application that actually works. Successfully parsing complex ABC notation files with varying formats and extracting structured metadata felt like a real achievement, especially considering I was learning as I went. I'm also proud of implementing the clear_tunes_table() function after realizing through testing that duplicate data was being created when rebuilding the database—identifying that problem and solving it showed me the importance of testing and thinking through edge cases.

# What I learned

This assignment taught me far more than just Python programming. I learned the fundamentals of version control with Git and GitHub, which was completely new territory for me. Understanding how to commit changes, write meaningful commit messages, and push to a repository gave me insight into how professional developers work collaboratively. The hackathon experience also taught me that asking for help and learning from more experienced developers is not only okay but essential for growth.




