rest = LOAD 'inspection/input' USING PigStorage(',') AS (name:chararray, zipcode:chararray, american:int, asian:int, continental:int, indian:int, italian:int, mexican:int, middle_eastern:int, vio_facilities:int, vio_food_handling:int, vio_hygiene:int, vio_vermin:int, critical:int, score:int, grade:chararray);

rest_name_grp = GROUP rest BY (name, zipcode);

name_agg = FOREACH rest_name_grp GENERATE group.name, group.zipcode, (SUM(rest.american) > 0 ? 1 : 0) as american, (SUM(rest.asian) > 0 ? 1 : 0) as asian, (SUM(rest.continental) > 0 ? 1: 0) as continental, (SUM(rest.indian) > 0 ? 1 : 0) as indian, (SUM(rest.italian) > 0 ? 1 : 0) as italian, (SUM(rest.mexican) > 0 ? 1 : 0) as mexican, (SUM(rest.middle_eastern) > 0 ? 1 : 0) as middle_eastern, SUM(rest.vio_facilities) as vio_facilities, SUM(rest.vio_food_handling) as vio_food_handling, SUM(rest.vio_hygiene) as vio_hygiene, SUM(rest.vio_vermin) as vio_vermin, COUNT(rest.critical) as total_violations, SUM(rest.critical) as critical_violations, ROUND(AVG(rest.score)) as score, (ROUND(AVG(rest.score)) < 14 ? 1 : 0) as grade_A, (ROUND(AVG(rest.score)) > 13 ? (ROUND(AVG(rest.score)) < 28 ? 1 : 0) : 0) as grade_B, (ROUND(AVG(rest.score)) > 27 ? 1 : 0) as grade_C;

zip_grp = GROUP name_agg BY zipcode;

zip_agg = FOREACH zip_grp GENERATE group as zipcode, SUM(name_agg.american) as american, SUM(name_agg.asian) as asian, SUM(name_agg.continental) as continental, SUM(name_agg.indian) as indian, SUM(name_agg.italian) as italian, SUM(name_agg.mexican) as mexican, SUM(name_agg.middle_eastern) as middle_eastern, SUM(name_agg.vio_facilities) as vio_facilities, SUM(name_agg.vio_food_handling) as vio_food_handling, SUM(name_agg.vio_hygiene) as vio_hygiene, SUM(name_agg.vio_vermin) as vio_vermin, SUM(name_agg.total_violations) as total_violations, SUM(name_agg.critical_violations) as critical_violations, SUM(name_agg.grade_A) as grade_A_count, SUM(name_agg.grade_B) as grade_B_count, SUM(name_agg.grade_C) as grade_C_count;

STORE name_agg INTO 'inspection/output/rest_level' USING PigStorage(',', '-schema');
STORE zip_agg INTO 'inspection/output/zip_level' USING PigStorage(',', '-schema');