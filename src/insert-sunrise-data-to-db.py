"""
Get sunrise data from API and insert into SQLite database for each unique date in the endsong table
Step 1: Get all unique dates from endsong table
Step 2: Get sunrise data from API for each unique date
Step 3: Create sunrise table in SQLite database
Step 4: Insert rows into sunrise table
"""

import pandas as pd
import sqlite3
import requests
import configparser

LAT = -37.8136
LON = 144.9631

URL= "https://api.sunrise-sunset.org/json"

DATABASE = '../data/spotify.db'


def create_database_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = sqlite3.connect(db_file)
    return conn


def get_unique_dates(conn):
    """
    Get unique dates from endsong table
    :param conn:
    :return: list
    """
    # Get unique dates from endsong table formatted as YYYY-MM-DD
    unique_dates_df = pd.read_sql_query("SELECT DISTINCT date(ts) FROM endsong", conn)

    # Create list of unique dates
    unique_dates_list = unique_dates_df.values.tolist()
    return unique_dates_list


def get_sunrise_data(date):
    """
    Get sunrise data from API
    :param date:
    :return: dict
    """
    # Get sunrise data from API
    params = {
        'lat': LAT,
        'lng': LON,
        'date': date,
        'formatted': 0
    }
    response = requests.get(URL, params=params)
    data = response.json()
    # As pandas dataframe
    df = pd.json_normalize(data['results'])
    return df


def create_sunrise_table(conn):
    """
    Create sunrise table in SQLite database
    :param conn:
    :return:
    """
    # Create sunrise table in SQLite database
    sql_create_sunrise_table = """ CREATE TABLE IF NOT EXISTS sunrise (
                                        date text PRIMARY KEY,
                                        sunrise text,
                                        sunset text,
                                        solar_noon text,
                                        day_length integer,
                                        civil_twilight_begin text,
                                        civil_twilight_end text,
                                        nautical_twilight_begin text,
                                        nautical_twilight_end text,
                                        astronomical_twilight_begin text,
                                        astronomical_twilight_end text
                                    ); """
    cur = conn.cursor()
    cur.execute(sql_create_sunrise_table)


def insert_sunrise_data(conn, df):
    """
    Insert pandas dataframe into sunrise table in SQLite database
    :param conn:
    :param df:
    :return:
    """
    # Insert pandas dataframe into sunrise table in SQLite database
    df.to_sql('sunrise', conn, if_exists='append', index=False)
    return


def main():
    # Create a database connection
    conn = create_database_connection(DATABASE)

    # Get unique dates from endsong table
    unique_dates_list = get_unique_dates(conn)

    # Create sunrise table in SQLite database
    create_sunrise_table(conn)

    # Insert rows into sunrise table
    for unique_date in unique_dates_list:
        date = unique_date[0]
        df = get_sunrise_data(date)
        df['date'] = date
        # Convert to Melbourne time, using tz_convert('Australia/Melbourne')
        for col in df.columns:
            if col not in ['date', 'day_length']:
                df[col] = pd.to_datetime(df[col]).dt.tz_convert('Australia/Melbourne')
        insert_sunrise_data(conn, df)
        # Print summary of data inserted, number of dates remaining
        print(f"Inserted sunrise data for {date}. {len(unique_dates_list) - unique_dates_list.index(unique_date) - 1} dates remaining.")
        # Commit changes
        conn.commit()



    # Close connection
    conn.close()


if __name__ == '__main__':
    main()
