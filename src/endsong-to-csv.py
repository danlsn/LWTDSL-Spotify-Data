"""
endsong-to-csv.py
Load endsong from SQLite database, remove columns, and save to CSV file.
table = endsong
columns = ['index', 'ts', 'ms_played', 'spotify_track_uri', 'reason_start', 'reason_end', 'shuffle', 'skipped', 'offline', 'offline_timestamp', 'filename']
"""

import pandas as pd
import sqlite3
from sqlite3 import Error

DATABASE = "../data/spotify.db"


def create_database_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = sqlite3.connect(db_file)
    return conn


def endsong_rows_to_dataframe(conn):
    """
    Get all rows from endsong table using pandas, then drop columns not in:
    columns=['index', 'ts', 'ms_played', 'spotify_track_uri', 'reason_start', 'reason_end', 'shuffle', 'skipped', 'offline', 'offline_timestamp', 'filename']
    :param conn: the Connection object
    :return: dataframe
    """
    endsong_df = pd.read_sql_query(
        "SELECT spotify_track_uri, master_metadata_album_artist_name as artist_name, master_metadata_album_album_name as album_name, master_metadata_track_name as track_name, ts, ms_played, conn_country, reason_start, reason_end FROM endsong WHERE spotify_track_uri IS NOT NULL",
        conn)

    # Extrack track id from spotify_track_uri
    endsong_df['track_id'] = endsong_df['spotify_track_uri'].str.split(':').str[2]
    # Convert ts to Melbourne time as ts_local
    endsong_df['ts_local'] = pd.to_datetime(endsong_df['ts']).dt.tz_localize('UTC').dt.tz_convert('Australia/Melbourne')

    return endsong_df


def dataframe_to_csv(df):
    df.to_csv('../data/endsong.csv', index=False)
    return df


def main():
    # create a database connection
    conn = create_database_connection(DATABASE)
    with conn:
        df = endsong_rows_to_dataframe(conn)
        dataframe_to_csv(df)


if __name__ == '__main__':
    main()
