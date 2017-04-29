REGISTER 'piggybank-0.15.0.jar';
rest = LOAD 'inspection/input' USING org.apache.pig.piggybank.storage.CSVLoader();
filtered = FOREACH rest GENERATE $1 AS name:chararray, $5 AS zipcode:chararray, $7 AS cuisine:chararray, $10 AS violation_code:chararray, $12 AS critical_flag:chararray, $13 AS score:int, $14 AS grade:chararray;
STORE filtered INTO 'inspection/output' USING PigStorage('\t','-schema');