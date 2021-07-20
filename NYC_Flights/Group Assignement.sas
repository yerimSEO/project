libname NYC "C:\Users\yseo1\OneDrive - IESEG\Desktop\IESEG\BusinessReportingTool\group assignment\NYC flights";
/*date and windspeed and number of delay*/
proc
sql;
select distinct F.year, F.month, F.day, W.wind_speed, count(F.arr_delay) as total_of_delays
	
	from NYC.Flights F ,NYC.Weather W
	where (arr_delay>0)
		  and (F.year=W.year)
		  and (F.month=W.month)
		  and (F.day=W.day)
	group by F.year, F.month, F.day, W.wind_speed;
quit;
/*date and windgust and number of delay*/
proc
sql;
select distinct F.year, F.month, F.day, W.wind_gust, count(F.arr_delay) as number_of_delays
	
	from NYC.Flights F ,NYC.Weather W
	where (arr_delay>0)
		  and (F.year=W.year)
		  and (F.month=W.month)
		  and (F.day=W.day)
	group by F.year, F.month, F.day, W.wind_gust;
quit;

/*date and temp and number of delay*/
/*New*/
proc
sql;
select distinct F.year, F.month, F.day, round(W.temp) as round_temp, count(F.arr_delay) as total_of_delays
	
	from NYC.Flights F ,NYC.Weather W
	where (arr_delay>0)
		  and (F.year=W.year)
		  and (F.month=W.month)
		  and (F.day=W.day)
	group by F.year, F.month, F.day, round_temp;
quit;
/*temp and number of delay*/
/*New*/
proc
sql;
select distinct round(W.temp) as round_temp, count(F.arr_delay) as total_of_delays
	
	from NYC.Flights F ,NYC.Weather W
	where (arr_delay>0)
		  and (F.year=W.year)
		  and (F.month=W.month)
		  and (F.day=W.day)
	group by round_temp;
quit;
/*date and temp and number of delay*/
/*New*/
proc
sql;
select distinct F.year, F.month, F.day, round(W.dewp) as round_dewp, count(F.arr_delay) as total_of_delays
	
	from NYC.Flights F ,NYC.Weather W
	where (arr_delay>0)
		  and (F.year=W.year)
		  and (F.month=W.month)
		  and (F.day=W.day)
	group by F.year, F.month, F.day, round_dewp;
quit;
/*dewp and number of delay*/
/*New*/
proc
sql;
select distinct round(W.dewp) as round_dewp, count(F.arr_delay) as total_of_delays
	
	from NYC.Flights F ,NYC.Weather W
	where (arr_delay>0)
		  and (F.year=W.year)
		  and (F.month=W.month)
		  and (F.day=W.day)
	group by round_dewp;
quit;
/*date and humid and number of delay*/
/*New*/
proc
sql;
select distinct F.year, F.month, F.day, round(W.humid) as round_humid, count(F.arr_delay) as total_of_delays
	
	from NYC.Flights F ,NYC.Weather W
	where (arr_delay>0)
		  and (F.year=W.year)
		  and (F.month=W.month)
		  and (F.day=W.day)
	group by F.year, F.month, F.day, round_humid;
quit;
/*humid and number of delay*/
/*New*/
proc
sql;
select distinct round(W.humid) as round_humid, count(F.arr_delay) as total_of_delays
	
	from NYC.Flights F ,NYC.Weather W
	where (arr_delay>0)
		  and (F.year=W.year)
		  and (F.month=W.month)
		  and (F.day=W.day)
	group by round_humid;
quit;
/*date and wind_dir and number of delay*/
/*New*/
proc
sql;
select distinct F.year, F.month, F.day, round(W.wind_dir) as round_wind_dir, count(F.arr_delay) as total_of_delays
	
	from NYC.Flights F ,NYC.Weather W
	where (arr_delay>0)
		  and (F.year=W.year)
		  and (F.month=W.month)
		  and (F.day=W.day)
	group by F.year, F.month, F.day, round_wind_dir;
quit;
/*precip and number of delay*/
/*New*/
proc
sql;
select distinct round(W.precip) as round_precip, count(F.arr_delay) as total_of_delays
	
	from NYC.Flights F ,NYC.Weather W
	where (arr_delay>0)
		  and (F.year=W.year)
		  and (F.month=W.month)
		  and (F.day=W.day)
	group by round_precip;
quit;
/*date and precip and number of delay*/
/*New*/
proc
sql;
select distinct F.year, F.month, F.day, round(W.precip) as round_precip, count(F.arr_delay) as total_of_delays
	
	from NYC.Flights F ,NYC.Weather W
	where (arr_delay>0)
		  and (F.year=W.year)
		  and (F.month=W.month)
		  and (F.day=W.day)
	group by F.year, F.month, F.day, round_precip;
quit;
/*precip and number of delay*/
/*New*/
proc
sql;
select distinct round(W.precip) as round_precip, count(F.arr_delay) as total_of_delays
	
	from NYC.Flights F ,NYC.Weather W
	where (arr_delay>0)
		  and (F.year=W.year)
		  and (F.month=W.month)
		  and (F.day=W.day)
	group by round_precip;
quit;
/*date and pressure and number of delay*/
/*New*/
proc
sql;
select distinct F.year, F.month, F.day, round(W.pressure) as round_pressure, count(F.arr_delay) as total_of_delays
	
	from NYC.Flights F ,NYC.Weather W
	where (arr_delay>0)
		  and (F.year=W.year)
		  and (F.month=W.month)
		  and (F.day=W.day)
	group by F.year, F.month, F.day, round_pressure;
quit;
/*pressure and number of delay*/
/*New*/
proc
sql;
select distinct round(W.pressure) as round_pressure, count(F.arr_delay) as total_of_delays
	
	from NYC.Flights F ,NYC.Weather W
	where (arr_delay>0)
		  and (F.year=W.year)
		  and (F.month=W.month)
		  and (F.day=W.day)
	group by round_pressure;
quit;
/*date and visib and number of delay*/
/*New*/
proc
sql;
select distinct F.year, F.month, F.day, round(W.visib) as round_visib, count(F.arr_delay) as total_of_delays
	
	from NYC.Flights F ,NYC.Weather W
	where (arr_delay>0)
		  and (F.year=W.year)
		  and (F.month=W.month)
		  and (F.day=W.day)
	group by F.year, F.month, F.day, round_visib;
quit;
/*pressure and number of delay*/
/*New*/
proc
sql;
select distinct round(W.visib) as round_visib, count(F.arr_delay) as total_of_delays
	
	from NYC.Flights F ,NYC.Weather W
	where (arr_delay>0)
		  and (F.year=W.year)
		  and (F.month=W.month)
		  and (F.day=W.day)
	group by round_visib;
quit;
/*routes and wind direction and delay*/
/*new*/
PROC SQL;

SELECT distinct F.origin, F.dest, round(W.wind_dir) as round_wind_dir, count(F.arr_delay) as delays
FROM nyc.flights F, nyc.weather W
where(F.arr_delay>0)
group by F.origin
ORDER BY F.origin, F.dest;
QUIT;

/* Evaluating the delays depending on the destination airports and distances */
PROC SQL OUTOBS=50000;

SELECT dest, distance, dep_delay+arr_delay as delays
FROM nyc.flights
ORDER BY delays DESC;
QUIT;
/*origin_destination and delay*/
PROC SQL OUTOBS=100;
SELECT origin, dest, dep_delay+arr_delay as delays
FROM nyc.flights
ORDER BY delays DESC;
QUIT;
/* Total delay based on manufacture year of planes */
/*changed to order by year*/ 

PROC SQL OUTOBS=1000;
SELECT DISTINCT p.year, sum(f.dep_delay+f.arr_delay) as delays 
FROM nyc.flights as f,
	 nyc.planes as p
WHERE f.tailnum = p.tailnum
GROUP BY 1
ORDER BY p.year DESC;
QUIT;
/* Total delay percent based on manufacturer of planes */ 

proc sql; 
select count/3322 FORMAT=Percent8.2 as percentage, engine, sum(f.dep_delay+f.arr_delay) as delays
from
(select count (*) as count, engine
from nyc.planes as p
group by engine), 
nyc.flights as f
where f.tailnum = p.tailnum
group by engine
order by percentage desc ;
/*quit;order by percentage desc*/
/* Total delay percent based on manufacturer of planes */ 
proc sql; 
select P.engine,  count(F.tailnum) as flight_num_tail, count(f.arr_delay) as total_number_of_delays,
count(f.arr_delay)/count(F.tailnum)FORMAT=Percent8.2 as rate

from nyc.planes as p,
	 nyc.flights as f


where
f.tailnum = p.tailnum
and f.arr_delay>0

group by engine
 ;
quit;

/* Total delay based on manufacturer of planes */ 
/*check*/
PROC SQL OUTOBS=50000;
SELECT DISTINCT p.engine, sum(f.dep_delay+f.arr_delay) as delays, 
(SELECT  
FROM nyc.flights as f,
	 nyc.planes as p
WHERE f.tailnum = p.tailnum
GROUP BY 1
ORDER BY delays DESC;
QUIT;
/*delay time per airline*/
PROC SQL;
select distinct al.name, count(f.arr_delay) as delays

			
		FROM nyc.airlines AS al,
		     nyc.flights AS f
where (al.carrier = f.carrier) and
	(f.arr_delay>0)
group by al.name;
QUIT;
proc sql; 
select count/3322 FORMAT=Percent8.2 as percentage, engine
from
(select count (*) as count, engine
from nyc.planes
group by engine)
group by engine
order by percentage desc ;
quit;

proc
sql;
select distinct month, count(*), count(arr_delay) as total_of_delays
	
	from NYC.Flights 
	where (arr_delay>0)
		
	group by month;
quit;
/*total delay per month*/
PROC SQL;
SELECT 
distinct f.month, count(f.flight) as total_flight, count(f.arr_delay) as total_delay

FROM nyc.flights AS f

GROUP BY 1

having total_delay>
(select count(f.arr_delay) as total_delay
	from nyc.flights 
	where arr_delay > 0 
	Group by f.month) ;

QUIT;

/*delay time per airline*/
PROC SQL;
create table nb_of_delays_per_arl as 
select distinct al.name, count(f.arr_delay) as delays
		FROM nyc.airlines AS al,
		     nyc.flights AS f
where (al.carrier = f.carrier) and
	(f.arr_delay>0)
group by al.name;
QUIT;