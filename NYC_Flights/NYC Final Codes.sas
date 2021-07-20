libname nyc "C:\Users\yseo1\OneDrive - IESEG\Desktop\IESEG\BusinessReportingTool\group assignment\NYC flights";

/******** Evaluating the delays for the different airlines ********/

/* arrival delays per airline */ 
PROC SQL;
create table arrival_delays_per_airline as 
SELECT distinct al.carrier, 
		al.name, 
		count(f.arr_delay) as arrival_delays
from nyc.airlines AS al,
	 nyc.flights AS f
WHERE arr_delay > 0 AND
al.carrier = f.carrier
Group by 1;

QUIT;

/* departure_delays_per_airline */ 
PROC SQL;
create table departure_delays_per_airline as 
SELECT DISTINCT al.carrier as Carrier, al.name As Airline, count(flight) AS Nbr_of_flights_delayed
FROM nyc.airlines AS al,
	 nyc.flights AS f
WHERE al.carrier = f.carrier
GROUP BY 1
HAVING f.dep_delay > 0;

QUIT;

/* Total flights per airline */

PROC SQL;
create table Total_flights_per_airline  as 
SELECT carrier, count(flight)
FROM nyc.flights
GROUP BY 1;

QUIT;


/********* Evaluating the delays depending on the destination airports and distances ********/

PROC SQL;
create table delays_depending_on_the_destination_airports as
SELECT dest as Destination_Airport, arr_delay as Delays
FROM nyc.flights
WHERE arr_delay > 0
GROUP BY 1
ORDER BY delays DESC;
QUIT;


/******** valuating reasons for delays *********/


/* Total delay based on manufacture year of planes */ 

PROC SQL;
create table delay_based_on_manufacture_year as
SELECT DISTINCT p.year, count(f.arr_delay) as delays 
FROM nyc.flights as f,
	 nyc.planes as p
WHERE f.tailnum = p.tailnum
GROUP BY 1
ORDER BY delays DESC;
QUIT;

/* Total planes per manufacture year */

PROC SQL;
create table Total_planes_per_manufacture_year as
SELECT DISTINCT p.year, count(f.flight) as Nbr_of_Flights
FROM nyc.flights as f,
	 nyc.planes as p
WHERE f.tailnum = p.tailnum
GROUP BY 1
ORDER BY 1;
QUIT;


/* Total delay based on manufacturer of planes */ 

PROC SQL;
create table delay_based_on_manufactureras as
SELECT DISTINCT p.manufacturer as Manufacturer, count(f.arr_delay) as delays 
FROM nyc.flights as f,
	 nyc.planes as p
WHERE f.tailnum = p.tailnum
GROUP BY 1
ORDER BY delays DESC;
QUIT;

/* Total flights per manufacturer */

PROC SQL; 
create table Total_flights_per_manufacturer as
SELECT DISTINCT p.manufacturer as Manufacturer, count(f.flight) as Nbr_of_Flights
FROM nyc.flights as f,
	 nyc.planes as p
WHERE f.tailnum = p.tailnum
GROUP BY 1
ORDER BY 2 DESC;
QUIT;

/* Total flights per year */

PROC SQL;
create table Total_flights_per_year as
SELECT DISTINCT p.year, count(f.flight) as Nbr_of_Flights
FROM nyc.flights as f,
	 nyc.planes as p
WHERE f.tailnum = p.tailnum
GROUP BY 1
ORDER BY 1;
QUIT;


/******** Changes in delays over time **********/

/* delays per month */ 

PROC SQL;
create table delays_per_month as
SELECT 
distinct f.month, count(f.arr_delay) as total_delay, count(f.flight) as total_flights 

FROM nyc.flights AS f

GROUP BY 1

having total_delay>
(select count(f.arr_delay) as total_delay
	from nyc.flights 
	where arr_delay > 0 
	Group by f.month) ;

QUIT;


/******** Plot the worst routes (routes with highest delays) *******/ 

/* Evaluating the delays depending on the destination airports and distances */
PROC SQL 
OUTOBS=50000;
Create table delays_dest_dist as
SELECT dest, distance, dep_delay+arr_delay as delays, lat, lon, airport 
FROM nyc.flights F, NYC.airports A
where a.faa = f.dest 
ORDER BY delays DESC;
QUIT;

/*origin_destination and delay*/

PROC SQL OUTOBS=100;
Create table origin_destination_delays as
SELECT origin, dest, dep_delay+arr_delay as delays, lat, lon
FROM nyc.flights F, NYC.airports A
where a.faa = f.dest
ORDER BY delays DESC;
QUIT;
/* Total delay based on 



/********** Codes for weather analysis *******/

/* date and temp and number of delay*/
proc
sql;
create table delay_temp_total as
select distinct round(W.temp) as round_temp, count(F.arr_delay) as total_flights
	
	from NYC.Flights F ,NYC.Weather W
	where (F.year=W.year)
		  and (F.month=W.month)
		  and (F.day=W.day)
	group by round_temp;
quit;
/* temp and number of delay */
proc
sql;
create table delay_per_temp as
select distinct round(W.temp) as round_temp, count(F.arr_delay) as total_of_delays
	
	from NYC.Flights F ,NYC.Weather W
	where (arr_delay>0)
		  and (F.year=W.year)
		  and (F.month=W.month)
		  and (F.day=W.day)
	group by round_temp;
quit;
/* date and temp and number of delay */
proc
sql;
create table delay_per_temp_and_date as
select distinct F.year, F.month, F.day, round(W.dewp) as round_dewp, count(F.arr_delay) as total_of_delays
	
	from NYC.Flights F ,NYC.Weather W
	where (arr_delay>0)
		  and (F.year=W.year)
		  and (F.month=W.month)
		  and (F.day=W.day)
	group by F.year, F.month, F.day, round_dewp;
quit;
/*dewp and number of delay*/

proc
sql;
create table delay_per_dewp as
select distinct round(W.dewp) as round_dewp, count(F.arr_delay) as total_of_delays
	
	from NYC.Flights F ,NYC.Weather W
	where (arr_delay>0)
		  and (F.year=W.year)
		  and (F.month=W.month)
		  and (F.day=W.day)
	group by round_dewp;
quit;
proc
sql;
create table total_per_dewp as
select distinct round(W.dewp) as round_dewp, count(F.arr_delay) as total_per_dew
	
	from NYC.Flights F ,NYC.Weather W
	where  (F.year=W.year)
		  and (F.month=W.month)
		  and (F.day=W.day)
	group by round_dewp;
quit;
/*date and humid and number of delay/
/New*/
proc
sql;
create table delay_per_humid as
select distinct F.year, F.month, F.day, round(W.humid) as round_humid, count(F.arr_delay) as total_of_delays
	
	from NYC.Flights F ,NYC.Weather W
	where (arr_delay>0)
		  and (F.year=W.year)
		  and (F.month=W.month)
		  and (F.day=W.day)
	group by F.year, F.month, F.day, round_humid;
quit;
/*humid and number of delay*/

proc
sql; 
create table delay_per_humid_and_date as
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
create table delay_per_wind_dir as
select distinct F.year, F.month, F.day, round(W.wind_dir) as round_wind_dir, count(F.arr_delay) as total_of_delays
	
	from NYC.Flights F ,NYC.Weather W
	where (arr_delay>0)
		  and (F.year=W.year)
		  and (F.month=W.month)
		  and (F.day=W.day)
	group by F.year, F.month, F.day, round_wind_dir;
quit;
/*precip and number of delay/
/New*/
proc
sql;
create table delay_per_precip as
select distinct round(W.precip) as round_precip, count(F.arr_delay) as total_of_delays
	
	from NYC.Flights F ,NYC.Weather W
	where (arr_delay>0)
		  and (F.year=W.year)
		  and (F.month=W.month)
		  and (F.day=W.day)
	group by round_precip;
quit;
/*date and precip and number of delay*/
proc
sql;
create table delay_per_precip_and_date as
select distinct F.year, F.month, F.day, round(W.precip) as round_precip, count(F.arr_delay) as total_of_delays
	
	from NYC.Flights F ,NYC.Weather W
	where (arr_delay>0)
		  and (F.year=W.year)
		  and (F.month=W.month)
		  and (F.day=W.day)
	group by F.year, F.month, F.day, round_precip;
quit;
/*precip and number of delay*/
proc
sql;
create table delay_per_precip as
select distinct round(W.precip) as round_precip, count(F.arr_delay) as total_of_delays
	
	from NYC.Flights F ,NYC.Weather W
	where (arr_delay>0)
		  and (F.year=W.year)
		  and (F.month=W.month)
		  and (F.day=W.day)
	group by round_precip;
quit;
/*date and pressure and number of delay*/
proc
sql;
create table delay_per_pressure_and_date
select distinct F.year, F.month, F.day, round(W.pressure) as round_pressure, count(F.arr_delay) as total_of_delays
	
	from NYC.Flights F ,NYC.Weather W
	where (arr_delay>0)
		  and (F.year=W.year)
		  and (F.month=W.month)
		  and (F.day=W.day)
	group by F.year, F.month, F.day, round_pressure;
quit;
/*pressure and number of delay*/
proc
sql;
create table delay_per_pressure as 
select distinct round(W.pressure) as round_pressure, count(F.arr_delay) as total_of_delays
	
	from NYC.Flights F ,NYC.Weather W
	where (arr_delay>0)
		  and (F.year=W.year)
		  and (F.month=W.month)
		  and (F.day=W.day)
	group by round_pressure;
quit;
/*date and visib and number of delay*/
proc
sql;
create table delay_per_visib_and_date as
select distinct F.year, F.month, F.day, round(W.visib) as round_visib, count(F.arr_delay) as total_of_delays
	
	from NYC.Flights F ,NYC.Weather W
	where (arr_delay>0)
		  and (F.year=W.year)
		  and (F.month=W.month)
		  and (F.day=W.day)
	group by F.year, F.month, F.day, round_visib;
quit;
/*visib and number of delay*/
proc
sql;
create table delay_per_visib as
select distinct round(W.visib) as round_visib, count(F.arr_delay) as total_of_delays
	
	from NYC.Flights F ,NYC.Weather W
	where (arr_delay>0)
		  and (F.year=W.year)
		  and (F.month=W.month)
		  and (F.day=W.day)
	group by round_visib;
quit;
/* Total number of planes per manufacturer */

PROC SQL;
create table number_of_planes_per_manufacturer as
SELECT DISTINCT p.manufacturer, count(*) as Nbr_of_Planes
FROM nyc.planes as p
GROUP BY 1
ORDER BY 2 DESC;

QUIT;

/*delay day by_day*/
proc
sql;
create table delay_day_by_day as
select distinct month, day, count(arr_delay) as total_of_delays
	
	from NYC.Flights 
	where (arr_delay>0)
		
	group by month, day;
quit;

/*delay day by_day*/
proc
sql;
create table delay_day_by_day as
select distinct month, day, count(arr_delay) as total_of_delays
	
	from NYC.Flights 
	where (arr_delay>0)
		
	group by month, day;
quit;