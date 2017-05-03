
/* Joining Crime and 311 sorted datasets*/
crime_data = Load 'crime' USING PigStorage(',');
gp_crime_data = GROUP crime_data BY $0;

three_one_one_data = Load '311' USING PigStorage(',');
gp_three_one_one_data = GROUP crime_data BY $0;

merged_1 = JOIN gp_crime_data by $0, gp_three_one_one_data by $0 USING 'merge';