
/* Joining Crime and 311 sorted datasets*/
crime_data = Load 'crime' USING PigStorage(',');
gp_crime_data = GROUP crime_data BY $0;
gp_crime_bag = FOREACH gp_crime_data GENERATE group,$1..;
final_crime = FOREACH gp_crime_bag GENERATE $0,FLATTEN($1);
final_crime = FOREACH final_crime GENERATE $0,$2..;
STORE final_crime INTO 'final_crime' USING PigStorage(',');


three_one_one_data = Load '311' USING PigStorage(',');
gp_three_one_one_data = GROUP crime_data BY $0;
gp_three_one_one_bag = FOREACH gp_three_one_one_data GENERATE group,$1..;
final_311 = FOREACH gp_three_one_one_bag GENERATE $0,FLATTEN($1);
final_311 = FOREACH final_311 GENERATE $0,$2..;
STORE final_311 INTO 'final_311' USING PigStorage(',');

merged_1 = JOIN gp_crime_data by $0, gp_three_one_one_data by $0;
merged_1 = FOREACH merged_1 GENERATE $0,FLATTEN($1);


/*
STORE merged_1 INTO 'crime311' USING PigStorage(',');

cab_data = Load 'cab' USING PigStorage(',');
 */