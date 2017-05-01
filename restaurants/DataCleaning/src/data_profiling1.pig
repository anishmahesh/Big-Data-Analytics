rest = LOAD 'inspection/input' USING PigStorage('\t') AS (name:chararray, zipcode:chararray, cuisine:chararray, violation_code:chararray, critical:int, score:int, grade:chararray);
distinct_cuisines = DISTINCT(FOREACH rest GENERATE cuisine);
distinct_violation_codes = DISTINCT(FOREACH rest GENERATE violation_code);
STORE distinct_cuisines INTO 'inspection/output/cuisines';
STORE distinct_violation_codes INTO 'inspection/output/vio';