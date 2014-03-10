DROP TABLE IF EXISTS movie_titles;

CREATE EXTERNAL TABLE IF NOT EXISTS movie_titles (
 mid INT,
 yearofrelease INT,
 title STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${INPUT}/movie_titles/';

DROP TABLE IF EXISTS movie_ratings;

CREATE EXTERNAL TABLE IF NOT EXISTS movie_ratings (
 mid INT,
 customer_id INT,
 rating INT,
 date STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${INPUT}/movie_ratings/';

DROP TABLE IF EXISTS variance;

CREATE EXTERNAL TABLE IF NOT EXISTS variance(
 mid INT,
 title STRING,
 variance DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${OUTPUT}/movie_ratings/variance/';

INSERT OVERWRITE TABLE variance
SELECT d.mid, d.title, d.variances
FROM(
	SELECT b.mid as mid, b.title as title, c.variance as variances
	FROM movie_titles b
	JOIN(
	SELECT a.mid as mid, var_pop(a.rating) as variance
	FROM movie_ratings a
	GROUP BY a.mid) c
	ON b.mid = c.mid
) d
ORDER BY d.variances DESC;

DROP TABLE IF EXISTS trend;

CREATE EXTERNAL TABLE IF NOT EXISTS trend(
 yearofrelease INT,
 amount INT,
 averating DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${OUTPUT}/movie_ratings/trend/';

INSERT OVERWRITE TABLE trend
SELECT e.yearofrelease, e.amount, f.averating
FROM
(SELECT yearofrelease, SUM(DISTINCT mid) as amount
FROM movie_titles
GROUP BY yearofrelease) e
JOIN(
	SELECT c.yearofrelease as yearofrelease, AVG(c.avgratings) as averating
	FROM
	(SELECT a.yearofrelease as yearofrelease, b.avgrating as avgratings
	FROM movie_titles a
	JOIN
	(SELECT d.mid, AVG(d.rating) as avgrating
	FROM movie_ratings d
	GROUP BY d.mid) b
	ON a.mid = b.mid) c
	GROUP BY c.yearofrelease) f
ON e.yearofrelease = f.yearofrelease;

DROP TABLE IF EXISTS fraud;

CREATE EXTERNAL TABLE IF NOT EXISTS fraud(
 customer_id STRING,
 factor DOUBLE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${OUTPUT}/movie_ratings/fraud';

INSERT OVERWRITE TABLE fraud
SELECT h.customer_id, h.avgDisparity
FROM
	(SELECT g.customer_id as customer_id, AVG(g.disparity) as avgDisparity
	FROM
		(SELECT d.customer_id as customer_id, abs(d.rating - f.avgRating) as disparity
		FROM
			(SELECT c.mid as mid, c.customer_id as customer_id, c.rating as rating
	 			FROM movie_ratings c
	 		JOIN
	 		(SELECT a.customer_id as customer_id
				FROM movie_ratings a
				GROUP BY a.customer_id
				HAVING SUM(a.mid) > 100) b
			ON b.customer_id = c.customer_id) d
		JOIN
		(SELECT e.mid as mid, AVG(e.rating) as avgRating
	 		FROM movie_ratings e
	 		GROUP BY e.mid) f
		ON d.mid = f.mid) g
	GROUP BY g.customer_id) h
ORDER BY h.avgDisparity DESC
LIMIT 100;