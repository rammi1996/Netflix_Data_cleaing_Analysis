use movies ;
desc netflix_raw

select * from netflix_raw 
where show_id='s5023'

-- handling  foreign charcaters

-- remove duplicates

select show_id, count(*)
from netflix_raw
group by show_id
having count(*)>1;

-- remove  duplicates 

SELECT * 
FROM netflix_raw nr
join( select upper(title) as upper_title,upper(type) as upper_type
      from netflix_raw nrw
      group by upper(title),upper(type)
      having count(*)>1
      ) dup
on  upper(nr.title)=dup.upper_title
AND upper(nr.type)=dup.upper_type
order by nr.title,nr.type   

with cte as (
select *,row_number() over(partition by title , type order by show_id) as rn 
from netflix_raw )
select * from cte where rn=1;


-- new table for listed_in , director, cast , country ,cast

  create table  netflix_cast AS
 with recursive splitcast as(
 select show_id ,
 Trim(substring_index(cast ,',',1)) as castname,
 Trim(substring(cast,length(substring_index(cast,',',1))+2)) as remaining_string
 from netflix_raw
 where cast is not null
 union All
 select show_id ,
 Trim(substring_index(remaining_string ,',',1)) as castname,
 Trim(substring(remaining_string,length(substring_index(remaining_string,',',1))+2)) as remaining_string
 from splitcast
 where remaining_string <> ''
 )
 select show_id , castname as cast 
 from splitcast;
 
 
 CREATE TABLE netflix_director AS
WITH RECURSIVE splitname AS (
    -- Base case: Extract the first part before the comma
    SELECT 
        show_id,
        TRIM(SUBSTRING_INDEX(director, ',', 1)) AS name,
        TRIM(SUBSTRING(director, LENGTH(SUBSTRING_INDEX(director, ',', 1)) + 2)) AS remaining_string
    FROM netflix_raw
    WHERE director IS NOT NULL

    UNION ALL

    
    SELECT 
        show_id,
        TRIM(SUBSTRING_INDEX(remaining_string, ',', 1)) AS name,
        TRIM(SUBSTRING(remaining_string, LENGTH(SUBSTRING_INDEX(remaining_string, ',', 1)) + 2)) AS remaining_string
    FROM splitname
    WHERE remaining_string <> ''
)
SELECT show_id, name AS director
FROM splitname;


 create table  netflix_country AS
 with recursive splitcountry as(
 select show_id ,
 Trim(substring_index(country ,',',1)) as country,
 Trim(substring(country,length(substring_index(country,',',1))+2)) as remaining_string
 from netflix_raw
 where country is not null
 union All
 select show_id ,
 Trim(substring_index(remaining_string ,',',1)) as country,
 Trim(substring(remaining_string,length(substring_index(remaining_string,',',1))+2)) as remaining_string
 from splitcountry
 where remaining_string <> ''
 )
 select show_id , country 
 from splitcountry;

 
  create table  netflix_genre AS
 with recursive splitlistedin as(
 select show_id ,
 Trim(substring_index(listed_in ,',',1)) as genre,
 Trim(substring(listed_in,length(substring_index(listed_in,',',1))+2)) as remaining_string
 from netflix_raw
 where listed_in is not null
 union All
 select show_id ,
 Trim(substring_index(remaining_string ,',',1)) as genre,
 Trim(substring(remaining_string,length(substring_index(remaining_string,',',1))+2)) as remaining_string
 from splitlistedin
 where remaining_string <> ''
 )
 select show_id , genre 
 from splitlistedin;
 
 
 -- convert date column 
 create table netflix
 with cte as (
 select * , row_number()over(partition by title , type order by show_id) as rn 
 from netflix_raw
 )
 select show_id , type ,str_to_date(date_added,'%M %d,%Y') as date_added,release_year, 
 case when duration is null then rating else duration end as duration ,rating , description 
 from cte
 

 
 
 -- populated missing value on country 
 insert into netflix_country
 select show_id , m.country from netflix_raw nr
 inner join ( select country , director from netflix_country nc 
 inner join netflix_director nd 
 on nc.show_id=nd.show_id
 group by  country , director
 order by director)m on nr.director=m.director
 where nr.country is null

----------------------------

-- data Analysis 

--netflix data analysis

/*1  for each director count the no of movies and tv shows created by them in separate columns 
for directors who have created tv shows and movies both */

select nd.director 
,COUNT(distinct case when n.type='Movie' then n.show_id end) as no_of_movies
,COUNT(distinct case when n.type='TV Show' then n.show_id end) as no_of_tvshow
from netflix n
inner join netflix_director nd on n.show_id=nd.show_id
group by nd.director
having COUNT(distinct n.type)>1


--2 which country has highest number of comedy movies 
select  top 1 nc.country , COUNT(distinct ng.show_id ) as no_of_movies
from netflix_genre ng
inner join netflix_country nc on ng.show_id=nc.show_id
inner join netflix n on ng.show_id=nc.show_id
where ng.genre='Comedies' and n.type='Movie'
group by  nc.country
order by no_of_movies desc


--3 for each year (as per date added to netflix), which director has maximum number of movies released
with cte as (
select nd.director,YEAR(date_added) as date_year,count(n.show_id) as no_of_movies
from netflix n
inner join netflix_directors nd on n.show_id=nd.show_id
where type='Movie'
group by nd.director,YEAR(date_added)
)
, cte2 as (
select *
, ROW_NUMBER() over(partition by date_year order by no_of_movies desc, director) as rn
from cte
--order by date_year, no_of_movies desc
)
select * from cte2 where rn=1



--4 what is average duration of movies in each genre
select ng.genre , avg(cast(REPLACE(duration,' min','') AS int)) as avg_duration
from netflix n
inner join netflix_genre ng on n.show_id=ng.show_id
where type='Movie'
group by ng.genre

--5  find the list of directors who have created horror and comedy movies both.
-- display director names along with number of comedy and horror movies directed by them 
select nd.director
, count(distinct case when ng.genre='Comedies' then n.show_id end) as no_of_comedy 
, count(distinct case when ng.genre='Horror Movies' then n.show_id end) as no_of_horror
from netflix n
inner join netflix_genre ng on n.show_id=ng.show_id
inner join netflix_director nd on n.show_id=nd.show_id
where type='Movie' and ng.genre in ('Comedies','Horror Movies')
group by nd.director
having COUNT(distinct ng.genre)=2;

select * from netflix_genre where show_id in 
(select show_id from netflix_director where director='Steve Brill')
order by genre

