import requests
import pandas as pd
import json
from datetime import datetime
import datetime
import psycopg2
from sqlalchemy import Table,create_engine,MetaData,Column,Integer,String
from sqlalchemy.orm import sessionmaker

#This app gets songs played in last 24 hours from my Spotify account using Spotifys API, tables them using Pandas dataframe
# and sends them to my PostgreSQL database. This is practice project about ETL process.
#APIs URL = https://developer.spotify.com/console/get-recently-played/?limit=10&after=1484811043508&before=
 

# Spotify user id and token
SPOTIFY_ID = 'your_spotify_id'
SPOTIFY_TOKEN = 'unique_spotify_token_for_you'

# DB location
DATABASE = 'postgresql+psycopg2://username:password@localhost/database'    

def is_data_valid(df: pd.DataFrame) -> bool:
    
    # Check if dataframe empty
    if df.empty:
        print('No songs to download. Data corrupted. Program stopped')
        return False
    
    # Primary key check
    if pd.Series(df['Last time played']).is_unique:
        pass
    else:
        raise Exception('Incorrect primary key!')
    
    # Check for nulls
    if df.isnull().values.any():
        raise Exception('Null value found!')

if __name__ == '__main__':

    # ETL part one; Extract 
    
    # Headers which spotify api requires
    headers = {
        
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {SPOTIFY_TOKEN}' 
    }

    # Yesterdays time from seconds to unixms
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1) # <-- How many days we are going to go back to get played songs
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    # Getting all songs I have listened in last 24 hours
    r = requests.get('https://api.spotify.com/v1/me/player/recently-played?after{time}'.format(time=yesterday_unix_timestamp), headers=headers)

    data = r.json()

    # ETL part two; Transform
    
    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []
    
    # Iterating relevant data from json and putting them to their own list variables
    for song in data['items']:
        song_names.append(song['track']['name'])
        artist_names.append(song['track']['album']['artists'][0]['name'])
        played_at_list.append(song['played_at'])
        timestamps.append(song['played_at'][0:10])
    
    # Creating dictionary for pandas dataframe
    song_dict = {
        'Song' : song_names,
        'Artist' : artist_names,
        'Last time played' : played_at_list,
        'Date' : timestamps
    }

    song_df = pd.DataFrame(song_dict, columns = ['Song', 'Artist', 'Last time played', 'Date'])
    print(song_df)
    
    # Validate song tables data
    if is_data_valid(song_df):
        print('Data is valid, continue to Load part!')

    
    # ETL part three; Load
    
    # Sqlalchemy engine
    engine = create_engine(DATABASE)

    # Opening connection to DB
    connect = psycopg2.connect(user='username',password='password',host='localhost',database='database')
    
    # Creating data table with sqlalchemy to my Postrges DB 
    meta = MetaData()

    playlist = Table(
        'playlist', meta,
        Column('Song', String),
        Column('Artist', String),
        Column('Last time played', String, primary_key = True),
        Column('Date',String),
    )
    meta.create_all(engine)

    # Appending new songs to Postgres DB if table is not dublicate
    try:
        song_df.to_sql('playlist', engine, index=False, if_exists='append')
    except:
        print('Data(table) duplicate, not appended!')

    # Closing connection
    connect.close()
