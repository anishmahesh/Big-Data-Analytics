rest = LOAD 'inspection/input' USING PigStorage('\t') AS (name:chararray, zipcode:chararray, cuisine:chararray, violation_code:chararray, critical_flag:chararray, score:int, grade:chararray);
filtered = FILTER rest BY zipcode IS NOT NULL;
filtered = FILTER rest BY score IS NOT NULL;
cleaned = FOREACH filtered GENERATE ((name IS NULL) ? 'NA' : name) AS name, zipcode, ((cuisine IS NULL) ? 'NA' : cuisine) AS cuisine, ((violation_code IS NULL) ? 'NA' : violation_code) AS violation_code, ((LOWER(critical_flag) == 'critical') ? 1 : 0) AS critical:int, score, ((grade IS NULL) ? 'NA' : grade) AS grade;
STORE cleaned INTO 'inspection/output' USING PigStorage('\t','-schema');