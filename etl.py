import sqlite3
import argparse

import pandas


def main(original_data_file, db_file="b2b_google_analytics.db"):
    # sheet names are the index
    table_names = ["browse_navigation", "keyword_search", "suggested_search", "content_preview", "content_retrievals"]

    # no primary keys on these tables other than the sqlite3-supplied rowid
    # since any combination of (user_id, event_time) can occur multiple times

    browse_navigation_create_table = """CREATE TABLE IF NOT EXISTS browse_navigation (
    user_id TEXT NOT NULL,
    event_time TEXT NOT NULL,
    event_label TEXT NOT NULL,
    second_page BLOB NOT NULL,
    session_duration FLOAT NOT NULL);"""
    
    keyword_search_create_table = """CREATE TABLE IF NOT EXISTS keyword_search (
    user_id TEXT NOT NULL,
    event_time TEXT NOT NULL,
    query BLOB NOT NULL,
    media_types TEXT NULL,
    session_duration FLOAT NOT NULL);"""
    
    suggested_search_create_table = """CREATE TABLE IF NOT EXISTS suggested_search (
    user_id TEXT NOT NULL,
    event_time TEXT NOT NULL,
    event_category TEXT NOT NULL,
    event_label BLOB NOT NULL,
    second_page BLOB NOT NULL,
    session_duration FLOAT NOT NULL);"""
    
    content_preview_create_table = """CREATE TABLE IF NOT EXISTS content_preview (
    user_id TEXT NOT NULL,
    event_time TEXT NOT NULL,
    event_category TEXT NOT NULL,
    event_label BLOB NOT NULL,
    page BLOB NOT NULL,
    session_duration FLOAT NOT NULL);"""
    
    content_retrievals_create_table = """CREATE TABLE IF NOT EXISTS content_retrievals (
    user_id TEXT NOT NULL,
    event_time TEXT NOT NULL,
    event_category TEXT NOT NULL,
    event_label BLOB NOT NULL,
    retrievals_count INTEGER NOT NULL,
    session_duration FLOAT NOT NULL);"""
    

    with sqlite3.connect(db_file) as connection:
        cursor = connection.cursor()
        
        cursor.execute(browse_navigation_create_table)
        connection.commit()
        cursor.execute(keyword_search_create_table)
        connection.commit()
        cursor.execute(suggested_search_create_table)
        connection.commit()
        cursor.execute(content_preview_create_table)
        connection.commit()
        cursor.execute(content_retrievals_create_table)
        connection.commit()
        
        cursor.close()

    with sqlite3.connect(db_file) as connection:
        for (i, table) in enumerate(table_names):
            print(table)
        
            df = pandas.read_excel(original_data_file, sheet_name=i,
                                   dtype={"Event time" : str, "User_Id" : str})
            if "Session Duration" in df.columns:
                df["Session Duration"] = df["Session Duration"].fillna(0)
            if "Event time" in df.columns:
                df["Event time"] = df["Event time"].apply(
                    lambda x : x[0:2] + ":" + x[2:])

            insert_statement = ("INSERT INTO {} VALUES (".format(table)
                                + "?, " * (4 if i <= 1 else 5) + "?);")
            to_insert = (tuple(row) for (j, row) in df.iterrows())
            cursor = connection.cursor()
            cursor.executemany(insert_statement, to_insert)
            connection.commit()
            cursor.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=("Create and load the Data Science "
                                                  "Manager interview data set into a "
                                                  "sqlite3 database with minimal alterations."))
    parser.add_argument("data_file", default="Data Science Manager interview data set.xlsx")
    args = parser.parse_args()

    main(args.data_file)
