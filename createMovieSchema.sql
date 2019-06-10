-----------------------------------------------------------
--Single DDL Script to create the Movie Schema including
--table creation & default values
-----------------------------------------------------------
--Lead Data Engineer test for Genscape, sent 20190529
--CJB 20190529
--last updated 20190530
-----------------------------------------------------------
--Notes on my style/design:  
--You'll notice that most tables have a stage/staging
--version of the table, and then the table itself
--I've found it convenient when updating tables being collected
--over time to insert into a batch/daily version of the table
--before inserting into the final table.  This is primarily
--because there are often processing reasons for keeping the data
--seperate until some verification or deletion process works in
--the base table to keep results clean.  Sometimes its not
--completely necessary, but included for consistency of design
-----------------------------------------------------------

drop schema if exists Movie
cascade;

create schema Movie;

-----------------------------------------------------------
--FullCountryList are all the countries available to the API
--This table is included for the purposes of getting additional 
--country info if requirements change
-----------------------------------------------------------
--https://api.themoviedb.org/3/configuration/countries?api_key=7ae01c9f0d82875aa7d5de1a6083064f
-----------------------------------------------------------

drop table if exists movie.FullCountryList;

CREATE TABLE movie.FullCountryList (
	countryCode    CHAR(2)  NOT NULL,
	englishName VARCHAR(100)
);

--Creating an auto incremented internal ID
ALTER TABLE movie.FullCountryList 
ADD COLUMN CountryID SERIAL PRIMARY key;

--My interest is that I can write a method to read from the configuration API
--but for now, this can just contain the default country values until that python method is created
insert into movie.FullCountryList (countryCode, englishName)
values ('AU','Australia'),
	   ('CA','Canada'),
	   ('GB','United Kingdom'),
	   ('IE','Ireland'),
	   ('US','United States of America');
--select * from movie.FullCountryList;

-----------------------------------------------------------	  
--TrackedCountryList
--default values for database build process, 
--could be updated later to include more countries
-----------------------------------------------------------	  
drop table if exists movie.TrackedCountryList
;

CREATE TABLE movie.TrackedCountryList (
   CountryID          int NOT NULL primary key,
   CountryCode        CHAR(2) not null, 
   initialDateTracked DATE             
);
-----------------------------------------------------------
--because api calls require CountryCode, I am keeping this here
--so that I don't have to join full country list every time I use it
-----------------------------------------------------------	  

insert into movie.TrackedCountryList (CountryID, CountryCode, initialDateTracked) 
select CountryID, 
       CountryCode, 
       now() as initialDateTracked
from movie.FullCountryList
--Just for initial table creation, take the default values from fullCountryList
;
--select * from movie.TrackedCountryList;

-----------------------------------------------------------	 
--I might play around with language translation at some point in time, 
--so adding these details if I pull in other languages in the future.  
--It is commented out for now
-----------------------------------------------------------	 
--https://api.themoviedb.org/3/configuration/primary_translations?api_key=7ae01c9f0d82875aa7d5de1a6083064f
-----------------------------------------------------------	 
/*
drop table if exists movie.LanguageSupported
;

CREATE TABLE movie.LanguageSupported (
   translationID  CHAR(5) NOT NULL primary key
);
*/

-----------------------------------------------------------	 
--Keeping a single place to read within the database for the 
--parameter for language used when calling API.
-----------------------------------------------------------	 
drop table if exists movie.CurrentLanguage;

CREATE TABLE movie.CurrentLanguage (
   translationID  CHAR(5) not null primary key,
   initialDateTracked DATE
);

insert into movie.CurrentLanguage (translationID, initialDateTracked) 
values ('en-US',now()); --per requirements, US english language default


-----------------------------------------------------------	 
--Now Playing with History
--Answer a question which movies are playing as well as 
--popularity/rating etc. in a particular country on a 
--particular date
-----------------------------------------------------------
--https://developers.themoviedb.org/3/movies/get-now-playing
-----------------------------------------------------------
--Will need to run this for every country (parameter is region)
--No default values here, need the daily process update to insert into this table
-----------------------------------------------------------

drop table if exists movie.NowPlayingStage;
CREATE TABLE movie.NowPlayingStage(
   MovieID        Integer not null,
   voteCount      float,
   voteAverage    float,
   popularity     float,
   primary key(MovieID)
);   


drop table if exists movie.NowPlaying;

CREATE TABLE movie.NowPlaying(
   MovieID        Integer not null,
   CountryID      Integer not null,
   NowPlayingDate Date not null,
   voteCount      float,
   voteAverage    float,
   popularity     float,
   primary key(MovieID,CountryID,NowPlayingDate)
);   
-----------------------------------------------------------
--No default values here, need the daily process update to insert into this table
-----------------------------------------------------------


-----------------------------------------------------------
--Base Movie table -- work in progress on what details
--need to be stored in the base table vs other tables
-----------------------------------------------------------
--Which movies are directed by a particular Director
--Which country have the most movies been produced
-----------------------------------------------------------
--https://api.themoviedb.org/3/movie/{movie_id}?api_key=7ae01c9f0d82875aa7d5de1a6083064f&language=en-US
-----------------------------------------------------------
drop table if exists movie.MovieStage;

CREATE TABLE movie.MovieStage(
   MovieID        Integer not null,
   title          varchar(200),
   overview       varchar(1000),
   adult          bool,
   budget         bigint,
   homepage       varchar(200),
   imdb_id        varchar(50),
   status         varchar(30),
   releaseDate    date,
   tagline        varchar(300),
   video          bool,
   primary key(MovieID)
);

drop table if exists movie.Movie;

CREATE TABLE movie.Movie(
   MovieID        Integer not null,
   title          varchar(200),
   overview       varchar(1000),
   adult          bool,
   budget         bigint,
   homepage       varchar(200),
   imdb_id        varchar(50),
   status         varchar(30),
   releaseDate    date,
   tagline        varchar(300),
   video          bool,
   primary key(MovieID)
);

drop table if exists movie.ProductionCompanyStage;

CREATE TABLE movie.ProductionCompanyStage(
   MovieID        Integer not null,
   CompanyID      integer,
   name           varchar(100),
   originCountry  char(2),
   primary key(MovieID,CompanyID)
);

drop table if exists movie.ProductionCompany 
;
CREATE TABLE movie.ProductionCompany(
   MovieID        Integer not null,
   CompanyID      integer,
   name           varchar(100),
   originCountry  char(2),
   primary key(MovieID,CompanyID)
);

-----------------------------------------------------------
--GenreType
--A list of different genres stored in the database
-----------------------------------------------------------
--How many movies of a particular genre are playing 
--in a particular country
-----------------------------------------------------------
--https://api.themoviedb.org/3/genre/movie/list?language=en-US&api_key=7ae01c9f0d82875aa7d5de1a6083064f
-----------------------------------------------------------
drop table if exists movie.GenreType;

CREATE TABLE movie.GenreType(
   GenreID        Integer not null primary key,
   GenreName      varchar(50)
);

--will identify from API for default values

-----------------------------------------------------------
--Genre
--A list of different genres stored in the database by Movie
-----------------------------------------------------------
--How many movies of a particular genre are playing 
--in a particular country
-----------------------------------------------------------
--https://api.themoviedb.org/3/movie/{movie_id}?api_key=7ae01c9f0d82875aa7d5de1a6083064f&language=en-US
-----------------------------------------------------------
drop table if exists movie.GenreStage;

CREATE TABLE movie.GenreStage(
   MovieID Integer not null,
   GenreID Integer not null,
   primary key(MovieID,GenreID)
);

drop table if exists movie.Genre;

CREATE TABLE movie.Genre(
   MovieID Integer not null,
   GenreID Integer not null,
   primary key(MovieID,GenreID)
);

--Is collected daily from the movie Now Playing api

-----------------------------------------------------------
--Cast
--A list of all the actors appearing in the Movie
-----------------------------------------------------------
-- Which movies are directed by a particular director?
-----------------------------------------------------------
--https://api.themoviedb.org/3/movie/420817/credits?api_key=7ae01c9f0d82875aa7d5de1a6083064f
-----------------------------------------------------------
drop table if exists movie.CastStage;

CREATE TABLE movie.CastStage(
   MovieID     Integer not null,
   CastID      Integer,
   personID    Integer,
   creditID    char(25),
   gender      integer,
   name        varchar(50),
   "order"     integer,
   "character" varchar(50), 
   primary key(MovieID,CastID)
);


drop table if exists movie.Cast;

CREATE TABLE movie.Cast(
   MovieID     Integer not null,
   CastID      Integer,
   personID    Integer,
   creditID    char(25),
   gender      integer,
   name        varchar(50),
   "order"     integer,
   "character" varchar(50), 
   primary key(MovieID,CastID)
);

-----------------------------------------------------------
--Crew
--A list of all the crew associated with the production of Movie
-----------------------------------------------------------
-- Which movies are directed by a particular director?
-----------------------------------------------------------
--https://api.themoviedb.org/3/movie/420817/credits?api_key=7ae01c9f0d82875aa7d5de1a6083064f
-----------------------------------------------------------
drop table if exists movie.CrewStage;

CREATE TABLE movie.CrewStage(
   MovieID    Integer not null,
   creditID   char(25),
   personID   Integer,
   gender     integer,
   department varchar(50),
   job        varchar(50),
   "name"     varchar(50),
   primary key(MovieID,creditID)
);

drop table if exists movie.Crew;

CREATE TABLE movie.Crew(
   MovieID    Integer not null,
   creditID   char(25),
   personID   Integer,
   gender     integer,
   department varchar(50),
   job        varchar(50),
   "name"     varchar(50),
   primary key(MovieID,creditID)
);


-----------------------------------------------------------
--Reviews
--A list of all the crew associated with the production of Movie
-----------------------------------------------------------
-- Which review authors write the most reviews per country?
-----------------------------------------------------------
--https://api.themoviedb.org/3/movie/299534/reviews?api_key=7ae01c9f0d82875aa7d5de1a6083064f&language=en-US
-----------------------------------------------------------
drop table if exists movie.ReviewStage;

CREATE TABLE movie.ReviewStage(
   MovieID    Integer not null,
   ReviewID char(25),
   author varchar(50), 
   ReviewText text,
   url varchar(75),
   primary key(MovieID,ReviewID)
);

drop table if exists movie.Reviews;

CREATE TABLE movie.Reviews(
   MovieID    Integer not null,
   ReviewID char(25),
   author varchar(50), 
   ReviewText text,
   url varchar(75),
   primary key(MovieID,ReviewID)
);

-----------------------------------------------------------
--MonthlyCountryReviewCount
--A monthly distinct summary count of all the movies playing 
--as of the month reported/summarized
-----------------------------------------------------------
drop table if exists movie.MonthlyCountryMovieCount;

CREATE TABLE movie.MonthlyCountryMovieCount(
   yearmonth Integer,
   country    char(2) not null,
   numberofmovies int,
   primary key(yearmonth,country)
);
