'''
-----------------------------------------------------------
movie_db_api.py
Creates the GenscapeTest database, Movie Schema
Loads default values
Manages the processing of the themoviedb.org API calls
-----------------------------------------------------------
--Lead Data Engineer test for Genscape, sent 20190529
--CJB 20190529
--last updated 20190607 (Spanish Time)
-----------------------------------------------------------
'''

from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from pathlib import Path
import json, requests #used for api call and results
from csv import writer #writing results
import os #handles the file operations used for copying files into the postgres db
from time import sleep #used for sleep for throttling API requests 
#                       --> would be better to error handle this, but making it 
#                           simpler for this implementation
import datetime #using todays date and test for the first of the month to run the movie 
#                count operations
from movie_api_queries import queries as query #kept queries seperate from python code
import sys
#Global variables used for the moviedb api API
API_KEY = '7ae01c9f0d82875aa7d5de1a6083064f'
CURRENT_LANGUAGE = 'en-US'
BASE_MOVIEDB_URL =  'https://api.themoviedb.org/3/'
#for more info on the movie api, check here:
# https://developers.themoviedb.org/3/getting-started/introduction


def db_connect_cursor(dbname='GenscapeTest',db_user='postgres',dbhost='localhost',autocommit=True):
    try:
        connection = connect(dbname=dbname, user=db_user, host=dbhost)
        connection.set_session(autocommit=autocommit)
        cursor = connection.cursor()
        return connection, cursor
    except Exception as connection_exception:
        print('''Unable to connect to the default db, you may need to ensure the password
               is stored correcting in .pgpass''')
        print(connection_exception)     # arguments stored in .args
        return False

def create_new_database(default_db='postgres',create_db_user='postgres',
        dbhost='localhost', createDBname='GenscapeTest'):
    try:
        connection, cursor = db_connect_cursor(dbname=default_db, 
            db_user=create_db_user, dbhost=dbhost) 
        cursor.execute('Drop DATABASE if exists %s'%(createDBname))
        cursor.execute('CREATE DATABASE ' + createDBname)
        cursor.close()
        connection.close()
        return True
    except Exception as create_inst:
        print('''Unable to connect or create db using autocommit
               Be sure the user specified has create db privileges''')
        print(create_inst)
        return 'False'    


def initialize_moviedb_schema(default_db='GenscapeTest',create_db_user='postgres',
        dbhost='localhost'):
    '''
        Initializes the creation of GenscapeTest database, and creates Movie schema.
        Assumptions: the DDL script createMovieSchema.sql exists in the same folder where process runs.
        Postgres is properly installed where it is needed, and the password to the db
             is stored in pgpass.conf. If on *nix based OS, make sure pgpass.conf has correct file permissions
    '''
    try:
        connection, cursor = db_connect_cursor(dbname=default_db, db_user=create_db_user, dbhost=dbhost) 
        schemaSQLfile = Path("./createMovieSchema.sql")
        cursor.execute(schemaSQLfile.read_text())
        result = True
    except Exception as create_inst:
        print('''Unable to set isolation level / autocommit to true, create db, or create movie schema. 
               Be sure the user specified has create db privileges''')
        print(create_inst)
        result = 'False'
    finally:
        cursor.close()
        connection.close()
        return result

def get_api_results(url_postfix='movie/now_playing', 
                    params='?api_key=%s&language=%s'%(API_KEY,CURRENT_LANGUAGE)):
    '''Simple method that combines BASE_MOVIEDB_URL ('https://api.themoviedb.org/3/')
       with parameters url_postfix (configuration/countries) and api parameter list (for example api_key)
       Returns a results dictionary from the json object returned from the API call
       For example:
        https://api.themoviedb.org/3/configuration/countries?api_key=7ae01c9f0d82875aa7d5de1a6083064f
    '''
    api_url = BASE_MOVIEDB_URL + url_postfix + params
    try:
        results = requests.get(api_url)
        results_dict = results.json()
        sleep(.25) #pausing to ensure that no problems with API request triggering 
        #           the request threshold (4 per second) This is not the best way to do this but
        #           did not want to handle the exceptions exceeding requests
        return results_dict
    except:
        print('API called failed')
        print(api_url)
        return None
    
def write_result_csv(results,fileName,fields_used):
    ''' Takes the results from the API call and writes them into a csv file'''
    load_file = open(fileName,'w', newline='')
    rowWriter = writer(load_file)
    #originally used a dictWriter object but this choked so switched to this method
    #where you loop through all the results, and for every result you filter out the fields
    # that you want to return, and write to the csv with the values of the fields you chose 
    for result in results: 
        row = [str(item)  for field in fields_used for (key, item) 
               in result.items() if field == key ]    
        rowWriter.writerow(row)
    load_file.close()

def table_load(db_cursor, fileName, tableName,delimiter=',',format=''):
    '''loads a delimited (by delimeter) file (fileName) into database table tableName
       using a cursor object with any special format instructions (such as NULL)      
    '''
    csv_data = os.path.realpath(fileName)
    db_cursor.execute(query['truncate_table']%(tableName))
    db_cursor.execute(query['copy_from']%(tableName, csv_data,delimiter,format)) 

def get_production_companies(results,fileName,fields_used,id):
    '''Similar to write_result_csv but because of the structure of API results, had to 
       separate this one out of the general pattern.
    '''
    load_file = open(fileName,'w', newline='')
    rowWriter = writer(load_file,delimiter='|') 
    #Had problems here -- had to insert NULL values for empty strings 
    # because postgres copy from didn't like empty value strings
    for row in results['production_companies']:
        result = [str(item) if str(item) !='' else 'NULL' for field in fields_used 
                  for (key, item) in row.items() if field == key ]
        if len(result)>0:
            result.insert(0,id[0])    
            rowWriter.writerow(result)
    load_file.close()

def get_genres(results, fileName):
    '''like write_result_csv but yet another pattern of results to copy from API
       no need to pass fields used because it uses just movieid and genreid
    '''
    load_file = open(fileName,'w', newline='')
    rowWriter = writer(load_file) 
    rowValues = [(item['id'],genre) for item in results for genre in item['genre_ids']]
    rowWriter.writerows(rowValues)
    load_file.close()      

def initialize_genre_type(cursor):
    '''This function runs an api request for a list of genre types and loads into the 
       database
    '''
    results = get_api_results(url_postfix='genre/movie/list', 
                    params='?api_key=%s&language=%s'%(API_KEY,CURRENT_LANGUAGE))
    write_result_csv(results['genres'],'genre_type.csv',fields_used = ['id', 'name'])
    table_load(cursor,'genre_type.csv','Movie.GenreType',)

def load_NowPlaying_Genre(cursor):
    '''Loops through all the countries where it is required to collect which movies 
       are playing today. For each country, loads the data into the staging table 
       for now playing. Finally inserts into NowPlaying table using the country 
       value for countryID and today's date for nowPlaying Date
    '''
    cursor.execute(query['delete_now_playing_today'])#added just in case need to rerun process same day
    #sys.exit()
    cursor.execute(query['truncate_table']%('Movie.NowPlayingStage'))
    cursor.execute(query['select_country']) #selects countryid, countrycode  
    for record in cursor.fetchall(): 
        results = get_api_results(url_postfix='movie/now_playing', 
        params='?api_key=%s&language=%s&region=%s'%(API_KEY,CURRENT_LANGUAGE,record[1]))
        write_result_csv(results['results'],'now_playing.csv',
                        fields_used = ['id', 'vote_count', 'vote_average', 'popularity'])
        get_genres(results['results'],'genre.csv')
        table_load(cursor, 'genre.csv',tableName = 'Movie.GenreStage')
        cursor.execute(query['delete_from_genre'])
        cursor.execute(query['insert_into_genre'])
        cursor.execute(query['truncate_table']%('Movie.GenreStage'))
        table_load(cursor, 'now_playing.csv',tableName = 'Movie.NowPlayingStage')
        cursor.execute(query['insert_into_NowPlaying']%(record[0]))

def load_MissingMovies(cursor,missingMovieIds):
    '''This loads a list of movies into the movie table.
    The problem is that you don't know when a new movie is coming out, so every
    day you load into the NowPlaying table, you'll need to see which newly released
    movies need to be loaded into both the movie and production tables
    '''
    load_file = open('movies.csv','w', newline='')
    fields_used = ['id', 'title', 'overview', 'adult', 'budget', 
                   'homepage','imdb_id','status','release_date','tagline','video']
    rowWriter = writer(load_file, delimiter='~')
    for movieID in missingMovieIds: 
        results = get_api_results(url_postfix='movie/%s'%(movieID[0]), 
                        params='?api_key=%s&language=%s'%(API_KEY,CURRENT_LANGUAGE))
        row = [str(item)  for field in fields_used for (key, item) in results.items()
               if field == key ]    
        if(len(row) >0):
            rowWriter.writerow(row)
        get_production_companies(results,'production.csv',
                                 ['id','name','origin_country'],movieID) 
        table_load(cursor, 'production.csv',tableName = 'Movie.ProductionCompanyStage',
                   delimiter='|',format=" NULL AS 'NULL' ")        
    load_file.close()
    table_load(cursor, 'movies.csv',tableName = 'Movie.MovieStage',delimiter='~')
    cursor.execute(query['insert_into_Movie'])
    cursor.execute(query['truncate_table']%('Movie.MovieStage'))
    cursor.execute(query['insert_into_productioncompany'])
    cursor.execute(query['truncate_table']%('Movie.productionCompanyStage'))                  


def load_MissingCastCrew(cursor,missingMovieIds):
    '''Like movies, need to load every cast and crew detail for every movie
    once it has been inserted into now playing.
    '''
    cast_file = open('cast.csv','w', newline='')
    crew_file = open('crew.csv','w', newline='')
    cast_fields_used = ['cast_id', 'id','credit_id','gender','name','order',
                        'character']
    crew_fields_used = ['credit_id', 'id','gender','department','job', 'name']
    castWriter = writer(cast_file, delimiter='~')
    crewWriter = writer(crew_file, delimiter='~')
    for movieID in missingMovieIds:
        results = get_api_results(url_postfix='movie/%s/credits'%(movieID[0]), 
                    params='?api_key=%s&language=%s'%(API_KEY,CURRENT_LANGUAGE))
        for cast in results['cast']:
            row= [str(item)  for field in cast_fields_used for (key, item) 
                  in cast.items() if field == key ]    
            row.insert(0,movieID[0])
            castWriter.writerow(row)
        for crew in results['crew']:
            row = [str(item)  for field in crew_fields_used for (key, item) 
                   in crew.items() if field == key ]
            row.insert(0,movieID[0])
            crewWriter.writerow(row)
    cast_file.close()
    crew_file.close()
    table_load(cursor, 'cast.csv',tableName = 'Movie.CastStage',delimiter='~')
    cursor.execute(query['insert_into_cast'])
    cursor.execute(query['truncate_table']%('Movie.CastStage'))
    table_load(cursor, 'crew.csv',tableName = 'Movie.CrewStage',delimiter='~')
    cursor.execute(query['insert_into_crew'])
    cursor.execute(query['truncate_table']%('Movie.CrewStage'))

def load_allReviews(cursor,allMovieIds):
    '''Loads all reviews every day -- there is no process to identify new reviews 
       from the api, need to loop through all movies in database to determine if 
       any new reviews exist. In the future, may need to adjust for all movies 
       now playing...
    '''
    load_file = open('reviews.csv','w', newline='')
    fields_used = ['id', 'author','content','url']
    rowWriter = writer(load_file, delimiter='|')
    for movieID in allMovieIds:
        results = get_api_results(url_postfix='movie/%s/reviews'%(movieID[0]), 
                    params='?api_key=%s&language=%s'%(API_KEY,CURRENT_LANGUAGE))
        if results['total_results']>0:
            for review in results['results']:
               row = [str(item) if item is not None else '' for field in fields_used 
                      for (key, item) in review.items() if field == key ]    
               row.insert(0,movieID[0])
               if(len(row) >0):
                   rowWriter.writerow(row)        
    load_file.close()
    table_load(cursor, 'reviews.csv',tableName = 'Movie.ReviewStage',delimiter='|',
               format='CSV')
    cursor.execute(query['delete_from_reviews'])
    cursor.execute(query['insert_into_reviews'])
    cursor.execute(query['truncate_table']%('Movie.ReviewStage'))

def initialize_movie_database(default_db='postgres',create_db_user='postgres',
        dbhost='localhost', createDBname='GenscapeTest'):
    if(create_new_database()):
        initialize_moviedb_schema(default_db=createDBname, create_db_user=create_db_user,
            dbhost=dbhost)
        connection, cursor = db_connect_cursor(dbname='GenscapeTest')
        initialize_genre_type(cursor)
        cursor.close()
        connection.close()

if __name__ == "__main__":
    initialize_movie_database()

