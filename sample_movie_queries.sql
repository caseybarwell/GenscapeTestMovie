
-- Which movies were playing on a particular date in a particular country? [Assume that the
--question will not be asked for dates before the process started to be regularly executed]

select distinct tcl.countrycode,
       np.nowplayingdate,
       m.Title
from movie.nowplaying np
inner join movie.movie m
  on np.movieid = m.movieid
inner join movie.trackedcountrylist tcl
  on tcl.countryid = np.countryid
where tcl.countrycode = 'GB'
and np.nowplayingdate = '2019-06-06'
;

--Which movies are directed by a particular director?
select m.title, 
       crew.department,
       crew.job,
       crew."name"
from movie.movie m
inner join movie.Crew crew
  on crew.movieid = m.movieid
where crew.department = 'Directing'
and crew.job = 'Director'
and crew."name" like '%Russo'
--These are two brothers who only directed one movie
;

--How many movies of a particular genre are now playing in a particular country?
--List of movies by genre for a particular country, commented out a particular genre
select tcl.countrycode,
       np.nowplayingdate,
	   gt.genrename,
       count(distinct np.movieid) as movie_count
from movie.nowplaying np
inner join movie.movie m
  on np.movieid = m.movieid
inner join movie.trackedcountrylist tcl
  on tcl.countryid = np.countryid
inner join movie.genre g
  on g.movieid = m.movieid
inner join movie.genretype gt
  on gt.genreid = g.genreid -- cast( g.genreid as int)
where tcl.countrycode = 'GB'
and np.nowplayingdate = '2019-06-06'
--and gt.genrename = 'Science Fiction'
group by tcl.countrycode,
         np.nowplayingdate,
	     gt.genrename
order by count(distinct np.movieid) desc	 
;

--Which review authors write the most reviews per country? 
-- My note:
--  Reviewers are not tagged to countries, so this is not unique to country 
--  but is unique to reviewers, so when doing a count by country, you have
--  duplicate review counts
--  however, because not every country has every movie the counts may differ
select r.Author,
       tcl.countrycode,
       np.nowplayingdate,
       count(distinct r.reviewid) as count_country_reviews
from movie.nowplaying np
inner join movie.movie m
  on np.movieid = m.movieid
inner join movie.reviews r 
  on np.movieid = r.movieid
inner join movie.trackedcountrylist tcl
  on tcl.countryid = np.countryid
--where tcl.countrycode = 'GB'
--and np.nowplayingdate = '2019-06-06'
group by tcl.countrycode,
       np.nowplayingdate,
       r.Author
order by count(distinct r.reviewid) desc, tcl.countrycode
;


--In which country are most movies produced?
select m.title, 
       crew.department,
       crew.job,
       crew."name"
from movie.movie m
inner join movie.productioncompany p
  on p.movieid = m.movieid
where 
--What was the popularity of a particular movie on a particular date?
select distinct m.Title,
                np.popularity,
                np.nowplayingdate    
from movie.nowplaying np
inner join movie.movie m
  on np.movieid = m.movieid
inner join movie.trackedcountrylist tcl
  on tcl.countryid = np.countryid
where --tcl.countrycode = 'GB'
--and 
np.nowplayingdate = '2019-06-06'
and m.title = 'Bharat' 
--and m.title like 'Avengers%'
;

