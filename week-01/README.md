## Week 1 Homework

In this homework we'll prepare the environment 
and practice with Docker and SQL


## Question 1. Knowing docker tags

Run the command to get information on Docker 

```docker --help```

Now run the command to get help on the "docker build" command

Which tag has the following text? - *Write the image ID to the file* 

- [ ] `--imageid string`
- [x] `--iidfile string`
- [ ] `--idimage string`
- [ ] `--idfile string`

Command used to get the answer: ```docker build --help | grep 'Write the image ID to the file'```

## Question 2. Understanding docker first run 

Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash.
Now check the python modules that are installed ( use pip list). 
How many python packages/modules are installed?

- [ ] 1
- [ ] 6
- [x] 3
- [ ] 7

Commands used to get the answer:
- ```docker run -it python:3.9 /bin/bash```
- ```pip list```

# Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from January 2019:

```wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz```

You will also need the dataset with zones:

```wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv```

Download this data and put it into Postgres (with jupyter notebooks or with a pipeline)


## Question 3. Count records 

How many taxi trips were totally made on January 15?

Tip: started and finished on 2019-01-15. 

Remember that `lpep_pickup_datetime` and `lpep_dropoff_datetime` columns are in the format timestamp (date and hour+min+sec) and not in date.

- [ ] 20689
- [x] 20530
- [ ] 17630
- [ ] 21090

Query used:
``` sql
SELECT
	COUNT(*)
FROM public.green_taxi_trips
WHERE 1=1
	AND DATE(lpep_pickup_datetime) = '2019-01-15'
	AND DATE(lpep_dropoff_datetime) = '2019-01-15';
```


## Question 4. Largest trip for each day

Which was the day with the largest trip distance
Use the pick up time for your calculations.

- [ ] 2019-01-18
- [ ] 2019-01-28
- [x] 2019-01-15
- [ ] 2019-01-10

Query used:
``` sql
SELECT
	DATE(lpep_pickup_datetime)
FROM public.green_taxi_trips
ORDER BY trip_distance DESC
LIMIT 1;
```

## Question 5. The number of passengers

In 2019-01-01 how many trips had 2 and 3 passengers?
 
- [ ] 2: 1282 ; 3: 266
- [ ] 2: 1532 ; 3: 126
- [x] 2: 1282 ; 3: 254
- [ ] 2: 1282 ; 3: 274

Query used:
``` sql
SELECT
	passenger_count,
	COUNT(index)
FROM public.green_taxi_trips
WHERE 1=1
	AND DATE(lpep_pickup_datetime) = '2019-01-01'
	AND passenger_count BETWEEN 2 AND 3
GROUP BY passenger_count;
```

## Question 6. Largest tip

For the passengers picked up in the Astoria Zone which was the drop off zone that had the largest tip?
We want the name of the zone, not the id.

Note: it's not a typo, it's `tip` , not `trip`

- [ ] Central Park
- [ ] Jamaica
- [ ] South Ozone Park
- [X] Long Island City/Queens Plaza

Query used:
``` sql
SELECT
	tzld."Zone",
	tip_amount
FROM public.green_taxi_trips gtt,
public.taxi_zone_lookup tzlp,
public.taxi_zone_lookup tzld
WHERE 1=1
	AND tzlp."LocationID" = gtt."PULocationID"
	AND tzld."LocationID" = gtt."DOLocationID"
	AND tzlp."Zone" = 'Astoria'
ORDER BY tip_amount DESC;
 ```

## Submitting the solutions

* Form for submitting: [form](https://forms.gle/EjphSkR1b3nsdojv7)
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 26 January (Thursday), 22:00 CET


## Solution

We will publish the solution here
