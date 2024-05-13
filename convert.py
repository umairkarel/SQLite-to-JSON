"""
This script converts sqlite3 db file to JSON format

author: umairkarel
date: 2024-05-13

Usage:
   convert.py -d <database> -o <output_folder>
"""

import os
import sys
import getopt
import json
import sqlite3


def dict_factory(cursor: sqlite3.Cursor, row: tuple) -> dict:
    """
    Creates a dictionary from the given cursor and row.

    Args:
        cursor (sqlite3.Cursor): The cursor object used to fetch the data.
        row (tuple): The row of data to be converted into a dictionary.

    Returns:
        dict: A dictionary representing the row data.
    """
    data_columns = {}
    for idx, col in enumerate(cursor.description):
        data_columns[col[0]] = row[idx]
    return data_columns


def open_connection(db_path: str) -> tuple:
    """
    Opens a connection to the SQLite database.

    Args:
        db_path (str): The path to the SQLite database.

    Returns:
        tuple: A tuple containing the connection and cursor objects.
    """
    connection = sqlite3.connect(db_path)
    connection.row_factory = dict_factory
    cursor = connection.cursor()

    return connection, cursor


def get_all_records(table_name: str, db_path: str) -> str:
    """
    Retrieves all records from the specified table in the given SQLite database.

    Parameters:
        table_name (str): The name of the table to retrieve records from.
        db_path (str): The path to the SQLite database file.

    Returns:
        str: A JSON string representing the retrieved records.
    """
    connection, cursor = open_connection(db_path)

    cursor.execute(f"SELECT * FROM '{table_name}'")
    results = cursor.fetchall()
    connection.close()

    return json.dumps(results)


def sqlite_to_json(db_path, output_folder):
    """
    Converts an SQLite database to JSON format.

    Parameters:
        db_path (str): The path to the SQLite database file.

    Returns:
        None
    """
    connection, cursor = open_connection(db_path)
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")

    tables = cursor.fetchall()
    for table_name in tables:
        results = get_all_records(table_name["name"], db_path)

        with open(
            f"{output_folder}{table_name['name']}.json", "w", encoding="utf-8"
        ) as db_file:
            db_file.write(results)

    connection.close()


if __name__ == "__main__":
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hd:o:", ["database=", "output="])
    except getopt.GetoptError as err:
        print(err)
        print("Usage: convert.py -d <database> -o <output_folder>")
        sys.exit(2)

    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print("Usage: convert.py -d <database> -o <output_folder>")
            sys.exit()
        elif opt in ("-d", "--database"):
            DB_PATH = arg
        elif opt in ("-o", "--output"):
            OUTPUT_FOLDER = arg

    if "DB_PATH" not in locals():
        print(
            "Please provide the path to the SQLite database using the -d or --database option."
        )
        sys.exit(2)
    if "OUTPUT_FOLDER" not in locals():
        print("Please provide the output folder using the -o or --output option.")
        sys.exit(2)

    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)

    sqlite_to_json(DB_PATH, OUTPUT_FOLDER)
