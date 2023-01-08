"""
Get rows from track_audio_features table from SQLite database with pandas
"""

import pandas as pd
import sqlite3

DATABASE = '../data/spotify.db'


def create_database_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = sqlite3.connect(db_file)
    return conn


def track_audio_features_rows_to_dataframe(conn):
    """
    Get all rows from track_audio_features table using pandas
    :param conn: the Connection object
    :return: dataframe
    """
    track_audio_features_df = pd.read_sql_query(
        "SELECT * FROM track_audio_features", conn)

    return track_audio_features_df


def dataframe_to_csv(df):
    df.to_csv('../data/track_audio_features.csv', index=False)
    return df


def main():
    # create a database connection
    conn = create_database_connection(DATABASE)
    with conn:
        df = track_audio_features_rows_to_dataframe(conn)
        dataframe_to_csv(df)


if __name__ == '__main__':
    main()
