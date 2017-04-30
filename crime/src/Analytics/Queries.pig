user_data = LOAD '/project/input/Zipcode.csv' USING PigStorage(',');
user_data_by_zip = GROUP user_data BY $0;

murder_data = FOREACH user_data_by_zip GENERATE group,SUM(user_data.$65) AS zip_murder_count;
STORE murder_data into '/project/output/MurderData' using PigStorage(',' , '-schema');

murder_data_police = FOREACH user_data_by_zip GENERATE group,SUM(user_data.$65) AS zip_murder_count_police;
STORE murder_data_police into '/project/output/MurderPoliceData' using PigStorage(',' , '-schema');

robbery_data = FOREACH user_data_by_zip GENERATE group,SUM(user_data.$65) AS zip_robbery_count;
STORE robbery_data into '/project/output/RobberyData' using PigStorage(',' , '-schema');

robbery_police_data = FOREACH user_data_by_zip GENERATE group,SUM(user_data.$65) AS zip_robbery_police_count;
STORE robbery_police_data into '/project/output/RobberyPoliceData' using PigStorage(',' , '-schema');

loitering_data = FOREACH user_data_by_zip GENERATE group,SUM(user_data.$65) AS zip_loitering_count;
STORE loitering_data into '/project/output/LoiteringData' using PigStorage(',' , '-schema');

loitering_police_data = FOREACH user_data_by_zip GENERATE group,SUM(user_data.$65) AS zip_loitering_police_count;
STORE loitering_police_data into '/project/output/LoiteringPoliceData' using PigStorage(',' , '-schema');

health_code_violation_data = FOREACH user_data_by_zip GENERATE group,SUM(user_data.$65) AS zip_hcv_count;
STORE health_code_violation_data into '/project/output/HCVData' using PigStorage(',' , '-schema');

health_code_violation_police_data = FOREACH user_data_by_zip GENERATE group,SUM(user_data.$65) AS zip_hcv_police_count;
STORE health_code_violation_police_data into '/project/output/HCVPoliceData' using PigStorage(',' , '-schema');

admins_code_violation_data = FOREACH user_data_by_zip GENERATE group,SUM(user_data.$65) AS zip_acv_count;
STORE admins_code_violation_data into '/project/output/ACVData' using PigStorage(',' , '-schema');

admins_code_violation_police_data = FOREACH user_data_by_zip GENERATE group,SUM(user_data.$65) AS zip_acv_police_count;
STORE admins_code_violation_police_data into '/project/output/ACVPoliceData' using PigStorage(',' , '-schema');
