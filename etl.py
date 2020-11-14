import sqlite3

import pandas


# sheet names are the index
table_names = ["browse_navigation", "keyword_search", "suggested_search", "content_preview", "content_retrievals"]

browse_navigation_create_table = """CREATE TABLE IF NOT EXISTS browse_navigation (
user_id TEXT NOT NULL,
event_time TEXT NOT NULL,
event_label TEXT NOT NULL,
second_page BLOB NOT NULL,
session_duration INTEGER NOT NULL,
PRIMARY KEY (user_id, event_time) );"""

keyword_search_create_table = """CREATE TABLE IF NOT EXISTS keyword_search (
user_id TEXT NOT NULL,
event_time TEXT NOT NULL,
query BLOB NOT NULL,
media_types TEXT NOT NULL,
session_duration INTEGER NOT NULL,
PRIMARY KEY (user_id, event_time) );"""

suggested_search_create_table = """CREATE TABLE IF NOT EXISTS suggested_search (
user_id TEXT NOT NULL,
event_time TEXT NOT NULL,
event_category TEXT NOT NULL,
event_label BLOB NOT NULL,
second_page BLOB NOT NULL,
session_duration INTEGER NOT NULL,
PRIMARY KEY (user_id, event_time) );"""

content_preview_create_table = """CREATE TABLE IF NOT EXISTS content_preview (
user_id TEXT NOT NULL,
event_time TEXT NOT NULL,
event_category TEXT NOT NULL,
event_label BLOB NOT NULL,
page BLOB NOT NULL,
session_duration INTEGER NOT NULL,
PRIMARY KEY (user_id, event_time) );"""

content_retrievals_create_table = """CREATE TABLE IF NOT EXISTS content_retrievals (
user_id TEXT NOT NULL,
event_time TEXT NOT NULL,
event_category TEXT NOT NULL,
event_label BLOB NOT NULL,
retrievals_count INTEGER NOT NULL,
session_duration INTEGER NOT NULL,
PRIMARY KEY (user_id, event_time) );"""


with sqlite3.connect("b2b_google_analytics.db") as connection:
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
