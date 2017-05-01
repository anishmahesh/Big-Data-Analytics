user_data = LOAD ' project/input/CRIME_ZIPCODE.csv' USING PigStorage(',') AS (zipcode: int, severityone: int, severitytwo: int, attempted: int, completed: int, violation: int, misdemeanor: int, felony: int);
user_data_zip_grp = GROUP user_data BY zipcode;

analytic_data = FOREACH user_data_zip_grp GENERATE group,SUM(user_data.severityone) AS zip_ns_count, SUM(user_data.severitytwo) AS zip_s_count, SUM(user_data.attempted) AS zip_attempted_count, SUM(user_data.completed) AS zip_completed_count, SUM(user_data.violation) AS zip_violation_count, SUM(user_data.misdemeanor) AS zip_misdemeanor_count, SUM(user_data.felony) AS zip_felony_count ;
STORE analytic_data into 'project/output/Analytic' using PigStorage(',' , '-schema');



