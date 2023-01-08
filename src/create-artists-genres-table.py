"""
Create table artists_genres in SQLite database. This table will be used to create a many-to-many relationship between artists and genres.
Step 1: Get all artist_uris, genres from artist_info table
Step 2: Split genres by ; and create a list of tuples with artist_uri and genre
Step 3: Create artists_genres table in SQLite database
Step 4: Insert rows into artists_genres table
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
    SPOTIPY_REDIRECT_URI = 'http://localhost:8888/callback'

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


def get_artist_uris_genres(conn):
    """
    Get artist_uris and genres from artist_info table
    :param conn:
    :return: list
    """
    # Get artist_uris and genres from artist_info table
    artist_uri_genre_df = pd.read_sql_query("SELECT uri, genres FROM artist_info", conn)
    # Create list of artist_uris and genres
    artist_uri_genre_list = artist_uri_genre_df.values.tolist()

    return artist_uri_genre_list


def main():
    # Create a database connection
    conn = create_database_connection(DATABASE)

    # Get artist_uris and genres from artist_info table
    artist_uri_genre_list = get_artist_uris_genres(conn)

    # Create list of tuples with artist_uri and genre
    artist_uri_genre_tuples = []
    for artist_uri_genre in artist_uri_genre_list:
        artist_uri = artist_uri_genre[0]
        genres = artist_uri_genre[1]
        if genres is not None:
            genres = genres.split(';')
            for genre in genres:
                artist_uri_genre_tuples.append((artist_uri, genre))

    # Create artists_genres table in SQLite database
    c = conn.cursor()
    c.execute('DROP TABLE IF EXISTS artists_genres')
    c.execute('CREATE TABLE artists_genres (artist_uri text, genre text)')

    # Insert rows into artists_genres table
    c.executemany('INSERT INTO artists_genres VALUES (?, ?)', artist_uri_genre_tuples)

    # Commit changes
    conn.commit()

    # Close connection
    conn.close()


if __name__ == '__main__':
    main()
