user_data = LOAD ' project/input/CRIME_ZIPCODE.csv' USING PigStorage(',');
user_data_zip_grp = GROUP user_data BY $0;

analytic_data = FOREACH user_data_zip_grp GENERATE group,SUM(user_data.$1) AS zip_ns_count, SUM(user_data.$2) AS zip_s_count, SUM(user_data.$3) AS zip_attempted_count, SUM(user_data.$4) AS zip_completed_count, SUM(user_data.$5) AS zip_violation_count, SUM(user_data.$6) AS zip_misdemeanor_count, SUM(user_data.$7) AS zip_felony_count ;
STORE analytic_data into 'project/output/Analytic' using PigStorage(',' , '-schema');



