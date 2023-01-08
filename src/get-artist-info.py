"""
Step 1: Get rows from track_info table from SQLite database with pandas.
Step 2: Get artist uris from track_info table.
Step 3: Split artist uris into a list.
Step 4: Get unique artist uris.
Step 5: Get artist info from Spotify API.
Step 6: Write artist info to artist_info table in SQLite database.
"""

import pandas as pd
import sqlite3
import spotipy
import configparser

DATABASE = '../data/spotify.db'


def create_spotipy_connection():
    """
    Create a Spotipy connection
    :return:
    """
    # Get SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET from config.ini
    config = configparser.ConfigParser()
    config.read('../config.ini')
    SPOTIFY_CLIENT_ID = config['SPOTIFY']['SPOTIFY_CLIENT_ID']
    SPOTIFY_CLIENT_SECRET = config['SPOTIFY']['SPOTIFY_CLIENT_SECRET']
    SPOTIPY_REDIRECT_URI = 'http://127.0.0.1:8888/callback'

    # Specify the scope
    scope = 'user-library-read'

    # Create a Spotipy object
    sp = spotipy.Spotify(
        auth_manager=spotipy.SpotifyOAuth(client_id=SPOTIFY_CLIENT_ID, client_secret=SPOTIFY_CLIENT_SECRET, scope=scope,
            redirect_uri=SPOTIPY_REDIRECT_URI))

    return sp


def create_database_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = sqlite3.connect(db_file)
    return conn


def get_artist_uris(conn):
    """
    Get artist_uris rows from track_info table
    :param conn:
    :return: list
    """
    # Get artist_uris from track_info table
    artist_uri_df = pd.read_sql_query("SELECT artist_uris FROM track_info", conn)
    # Create list of artist_uris
    artist_uri_list = artist_uri_df['artist_uris'].tolist()
    # Split artist_uris into a list
    artist_uri_list = [artist_uri.split(';') for artist_uri in artist_uri_list]
    # Flatten list of lists
    artist_uri_list = [item for sublist in artist_uri_list for item in sublist]
    # Get unique artist_uris
    artist_uri_list = list(set(artist_uri_list))
    return artist_uri_list


def get_artist_info(sp, artist_uri_df):
    """
    Get artist info from Spotify API
    :param artist_uri_df:
    :return:
    """

    artist_uri_batches = [artist_uri_df[i:i + 50] for i in range(0, len(artist_uri_df), 50)]
    artist_info = []

    for batch in artist_uri_batches:
        # Print batch number
        print('Batch number: {}'.format(artist_uri_batches.index(batch) + 1))
        # Print Batches remaining
        print('Batches remaining: {}'.format(len(artist_uri_batches) - artist_uri_batches.index(batch)))
        artist_info.extend(sp.artists(batch)['artists'])

    # Drop external_urls, href, and images from dicts in artist_info
    for artist in artist_info:
        del artist['external_urls']
        del artist['href']
        del artist['images']
        # followers.total
        artist['total_followers'] = artist['followers']['total']
        del artist['followers']
        # Merge genres into a string separated by ;
        artist['genres'] = ';'.join(artist['genres'])
    return artist_info


def create_artist_info_dataframe(artist_info):
    """
    Create artist_info dataframe
    :param artist_info:
    :return: dataframe
    """
    artist_info_df = pd.DataFrame(artist_info)

    #Set index to id
    artist_info_df.set_index('id', inplace=True)
    return artist_info_df


def dataframe_to_sqlite(conn, df):
    """
    Write dataframe to SQLite database
    :param df:
    :return:
    """
    df.to_sql('artist_info', con=conn, if_exists='append', index=True)
    return df


def main():
    # create a Spotipy connection
    sp = create_spotipy_connection()
    # create a database connection
    conn = create_database_connection(DATABASE)
    with conn:
        artist_uris = get_artist_uris(conn)
        artist_info = get_artist_info(sp, artist_uris)
        artist_info_df = create_artist_info_dataframe(artist_info)
        dataframe_to_sqlite(conn, artist_info_df)


if __name__ == '__main__':
    main()
