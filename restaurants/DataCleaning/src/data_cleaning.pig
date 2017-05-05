REGISTER 'piggybank-0.15.0.jar';
rest = LOAD 'inspection/input' USING org.apache.pig.piggybank.storage.CSVLoader();
filtered = FOREACH rest GENERATE $1 AS name:chararray, $5 AS zipcode:chararray, $7 AS cuisine:chararray, $10 AS violation_code:chararray, $12 AS critical_flag:chararray, $13 AS score:int, $14 AS grade:chararray;
STORE filtered INTO 'inspection/output' USING PigStorage('\t','-schema');

rest = LOAD 'inspection/input' USING PigStorage('\t') AS (name:chararray, zipcode:chararray, cuisine:chararray, violation_code:chararray, critical_flag:chararray, score:int, grade:chararray);
filtered = FILTER rest BY zipcode IS NOT NULL;
filtered = FILTER rest BY score IS NOT NULL;
cleaned = FOREACH filtered GENERATE ((name IS NULL) ? 'NA' : name) AS name, zipcode, ((cuisine IS NULL) ? 'NA' : cuisine) AS cuisine, ((violation_code IS NULL) ? 'NA' : violation_code) AS violation_code, ((LOWER(critical_flag) == 'critical') ? 1 : 0) AS critical:int, score, ((grade IS NULL) ? 'NA' : grade) AS grade;
STORE cleaned INTO 'inspection/output' USING PigStorage('\t','-schema');