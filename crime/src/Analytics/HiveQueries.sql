CREATE TABLE CRIME_ZIP (zip_code int, severe_one int, severe_two int, attempted int, completed int, violation int, misdemeanor int, felony int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOAD DATA LOCAL INPATH '../../../../Naman-Data/CRIME_ZIPCODE.csv' OVERWRITE INTO TABLE CRIME_ZIP;
SELECT SUM(severe_one) AS s_o
FROM crime_zip
GROUP BY crime_zip.zip_code
LIMIT 1;
