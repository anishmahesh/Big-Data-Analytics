
/* Joining Crime and 311 sorted datasets*/
crime_data = Load 'crime' USING PigStorage(',');
gp_crime_data = GROUP crime_data BY $0;
gp_crime_bag = FOREACH gp_crime_data GENERATE group,$1..;
final_crime = FOREACH gp_crime_bag GENERATE $0,FLATTEN($1);

three_one_one_data = Load '311' USING PigStorage(',');
gp_three_one_one_data = GROUP crime_data BY $0;
gp_three_one_one_bag = FOREACH gp_three_one_one_data GENERATE group,$0..;
final_311 = FOREACH gp_three_one_one_bag GENERATE $0,FLATTEN($1);

merged_1 = JOIN gp_crime_data by $0, gp_three_one_one_data by $0 USING 'merge';

DUMP merged_1;

/*
STORE merged_1 INTO 'crime311' USING PigStorage(',');

cab_data = Load 'cab' USING PigStorage(',');
 */