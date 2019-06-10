'''
-----------------------------------------------------------
--daily_movie_process.py
--Manages the daily process of moviedb api calls to manage
--database, as well as monthly processing trigger
--cleans up all csv files generated into a new folder
-----------------------------------------------------------
--Lead Data Engineer test for Genscape, sent 20190529
--CJB 20190529
--last updated 20190605
-----------------------------------------------------------
'''

import movie_api #import initialize_moviedb_schema, db_connect_cursor 
#Code for creating the initial database 
#as well as most of the functionality needed for daily & monthly processing  
import json, requests #used for api call and results
from csv import writer #writing results
import os #handles the file operations used for copying files into the postgres database
from time import sleep #used for sleep for throttling API requests 
#                       --> would be better to error handle this, but making it simpler for this implementation
import datetime #using todays date and test for the first of the month to run the movie count operations
from movie_api_queries import queries as query #kept queries seperate from python code

        
def daily_movie_process(dbName='GenscapeTest'):
    '''Runs through the steps necessary to load all tables from movie api
       for a daily process
    '''
    connection, cursor = movie_api.db_connect_cursor(dbname=dbName)
    movie_api.load_NowPlaying_Genre(cursor)
    cursor.execute(query['select_missing_movies'])
    missingMovieIds = cursor.fetchall()
    movie_api.load_MissingMovies(cursor,missingMovieIds)
    movie_api.load_MissingCastCrew(cursor,missingMovieIds)    
    cursor.execute(query['select_all_movies'])
    allMovieIds = cursor.fetchall()
    movie_api.load_allReviews(cursor,allMovieIds)
    cursor.close()
    connection.close()

def monthly_movie_process(cursor):
    cursor.execute(query['monthly_process'])

if __name__ == "__main__":
    today = datetime.date.today()
    connection, cursor = movie_api.db_connect_cursor()
    daily_movie_process()
    if(today.day == 1):
        monthly_movie_process(cursor)        