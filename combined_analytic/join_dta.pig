
/* Joining Crime and 311 sorted datasets*/
crime_data = Load 'crime' USING PigStorage(',');
gp_crime_data = GROUP crime_data BY $0;
gp_crime_bag = FOREACH gp_crime_data GENERATE group,$1..;
final_crime = FOREACH gp_crime_bag GENERATE $0,FLATTEN($1);
final_crime = FOREACH final_crime GENERATE $0,$2..;
/*STORE final_crime INTO 'final_crime' USING PigStorage(',');*/


three_one_one_data = Load '311' USING PigStorage(',');
gp_three_one_one_data = GROUP three_one_one_data BY $0;
gp_three_one_one_bag = FOREACH gp_three_one_one_data GENERATE group,$1..;
final_311 = FOREACH gp_three_one_one_bag GENERATE $0,FLATTEN($1);
final_311 = FOREACH final_311 GENERATE $0,$2..;
/*STORE final_311 INTO 'final_311' USING PigStorage(',');*/

merged_1 = JOIN gp_crime_data by $0, gp_three_one_one_data by $0;
merged_1 = FOREACH merged_1 GENERATE FLATTEN($1),FLATTEN($3);
merged_1 = FOREACH merged_1 GENERATE ..$7,$9..;

/* Merging all cab datasets*/
cab_one = Load 'cab_1' USING PigStorage(',');
gp_cab_one = GROUP cab_one BY $0;
cab_one_bag = FOREACH gp_cab_one GENERATE group,$1..;
final_cab_one = FOREACH cab_one_bag GENERATE $0,FLATTEN($1);
final_cab_one = FOREACH final_cab_one GENERATE $0,$2..;
/*STORE final_cab_one INTO 'final_cab_one' USING PigStorage(',');*/

cab_two = Load 'cab_2' USING PigStorage(',');
gp_cab_two = GROUP cab_two BY $0;
cab_two_bag = FOREACH gp_cab_two GENERATE group,$1..;
final_cab_two = FOREACH cab_two_bag GENERATE $0,FLATTEN($1);
final_cab_two = FOREACH final_cab_two GENERATE $0,$2..;
/*STORE final_cab_two INTO 'final_cab_two' USING PigStorage(',');*/


merged_cab_one_two = JOIN cab_one by $0, cab_two by $0;
merged_cab_one_two = FOREACH merged_cab_one_two GENERATE $0,$1,$3;

cab_three = Load 'cab_3' USING PigStorage(',');
gp_cab_three = GROUP cab_three BY $0;
cab_three_bag = FOREACH gp_cab_three GENERATE group,$1..;
final_cab_three = FOREACH cab_three_bag GENERATE $0,FLATTEN($1);
final_cab_three = FOREACH final_cab_three GENERATE $0,$2..;
/*STORE final_cab_three INTO 'final_cab_three' USING PigStorage(',');*/

merged_cab_final = JOIN merged_cab_one_two by $0, final_cab_three by $0;
merged_cab_final = FOREACH merged_cab_final GENERATE $0,$1,$2,$4;
/*STORE merged_cab_final INTO 'final_cab' USING PigStorage(',');*/


/*
    Cab,311,Crime Merge
 */
merged_external = JOIN merged_1 by $0, merged_cab_final by $0;
merged_external = FOREACH merged_external GENERATE ..$11,$13..;
/*STORE merged_external INTO 'merged_external' USING PigStorage(',');*/
/*DUMP merged_external;*/

/*
    Loading Restaurants health data
 */
restaurants = Load 'rest' USING PigStorage(',');

/*
    Merging All datasets
 */
merged_final = JOIN merged_external by $0, restaurants by $1;
merged_final = FOREACH merged_final GENERATE ..$15,$18..;
STORE merged_final INTO 'merged_final' USING PigStorage(',');

/*
    Loading zip level Restaurant's data
 */
 restaurants = Load 'zip_lvl' USING PigStorage(',');
 merged_zip = JOIN merged_external BY $0 LEFT OUTER, restaurants BY $0;
 merged_zip = FOREACH merged_zip GENERATE ..$14,$29..;
 STORE merged_zip INTO 'merged_zip' USING PigStorage(',');

