DROP TABLE IF EXISTS emails;

CREATE EXTERNAL TABLE IF NOT EXISTS emails (
 eid STRING,
 timestamp STRING,
 sender STRING, 
 receiver STRING,
 cc STRING,
 subject STRING,
 context STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${INPUT}';

DROP TABLE IF EXISTS enron;

CREATE EXTERNAL TABLE IF NOT EXISTS enron(
sender STRING,
numEmails STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${OUTPUT}/enron/emailsAfterWork/';

INSERT OVERWRITE TABLE enron
SELECT c.sender, c.numEmails FROM(
SELECT b.sender, count(*) as numEmails
FROM (
	SELECT a.sender, substr(split(a.timestamp, ' ')[5], 1, 2) as hour
	FROM emails a
) b
WHERE b.hour > '17' or b.hour < '09'
GROUP BY b.sender
ORDER BY numEmails DESC
) c
LIMIT 1000;

DROP TABLE IF EXISTS blacklist;

CREATE TABLE IF NOT EXISTS blacklist(
word STRING)
row format delimited
LOCATION '${HELPER}';

DROP TABLE IF EXISTS enron2_temp;

CREATE TABLE IF NOT EXISTS enron2_temp(
newitem STRUCT<ngram:ARRAY<STRING>, estfrequency:DOUBLE>);

DROP TABLE IF EXISTS enron2;

CREATE EXTERNAL TABLE IF NOT EXISTS enron2(
 word STRING,
 frequency DOUBLE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${OUTPUT}/enron/mostfrequentwords/';

INSERT OVERWRITE TABLE enron2_temp
SELECT explode(ngrams(sentences(lower(emails.context)), 1, 100 )) as asisrequired FROM emails;

INSERT OVERWRITE TABLE enron2
select a.word, a.frequency 
from (
SELECT newitem.ngram[0] as word, newitem.estfrequency as frequency
FROM enron2_temp) a 
LEFT OUTER JOIN blacklist b 
ON (a.word = b.word) 
WHERE b.word is NULL 
ORDER BY a.frequency DESC;

DROP TABLE IF EXISTS enron3;

CREATE EXTERNAL TABLE IF NOT EXISTS enron3(
sender STRING,
avelen FLOAT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${OUTPUT}/enron/largestlength/';

INSERT OVERWRITE TABLE enron3
SELECT b.sender, b.avelen
FROM(
	SELECT a.sender as sender, SUM(LENGTH(a.context))/COUNT(a.sender) as avelen FROM emails a
	GROUP BY a.sender 
	HAVING COUNT(a.sender) > 100) b
ORDER BY b.avelen DESC
LIMIT 1000;