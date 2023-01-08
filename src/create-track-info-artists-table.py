"""
Create table tracks_artists in SQLite database. This table will be used to create a many-to-many relationship between track_info and artists.
Step 1: Get all uri, artists from track_info table
Step 2: Split artists by ; and create a list of tuples with track_uri and artist
Step 3: Create tracks_artists table in SQLite database
Step 4: Insert rows into tracks_artists table
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


def get_track_uris_artists(conn):
    """
    Get track_uris and artists from track_info table
    :param conn:
    :return: list
    """
    # Get track_uris and artists from track_info table
    track_uri_artist_df = pd.read_sql_query("SELECT uri, artist_uris FROM track_info", conn)
    # Create list of track_uris and artists
    track_uri_artist_list = track_uri_artist_df.values.tolist()

    return track_uri_artist_list


def create_tracks_artists_table(conn, track_uri_artist_list):
    """
    Create tracks_artists table in SQLite database
    :param conn:
    :param track_uri_artist_list:
    :return:
    """
    # Create tracks_artists table in SQLite database
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS tracks_artists")
    cur.execute("CREATE TABLE tracks_artists (track_uri text, artist_uri text)")
    conn.commit()

    # Insert rows into tracks_artists table
    cur.executemany("INSERT INTO tracks_artists VALUES (?, ?)", track_uri_artist_list)
    conn.commit()


def main():
    # Create a Spotipy connection
    sp = create_spotipy_connection()
    # Create a database connection
    conn = create_database_connection(DATABASE)
    # Get track_uris and artists from track_info table
    track_uri_artist_list = get_track_uris_artists(conn)
    # Create a list of tuples with track_uri and artist
    track_uri_artist_list = [(track_uri, artist) for track_uri, artists in track_uri_artist_list for artist in artists.split(';')]
    # Create tracks_artists table in SQLite database
    create_tracks_artists_table(conn, track_uri_artist_list)
    # Close database connection
    conn.close()


if __name__ == '__main__':
    main()
