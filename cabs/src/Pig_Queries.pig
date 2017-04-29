data_taxi = LOAD 'data_filter.csv' USING PigStorage(',') AS (drop_lat:double, drop_long:double, pcount:int, pick_lat:double, pick_long:double, tip:float, totalA:float, dist:float);
test_lim = LIMIT data_taxi 10;
DUMP test_lim;
data_taxi = FILTER data_taxi by (NOT ( drop_lat == 0.0 ) OR ( drop_lat == NULL ) or ( drop_long == 0.0 ) or ( drop_long == NULL ) or ( pick_lat == 0.0 ) or ( pick_lat == NULL ) or ( pick_long == 0.0 ) or ( pick_long == NULL ));
data_taxi_count = GROUP data_taxi BY pcount;
DESCRIBE data_taxi_count
psum = FOREACH data_taxi_count GENERATE group,COUNT(data_taxi.pcount) AS mycount;
DUMP psum;
STORE psum into 'csvcount_1' using PigStorage(',','-schema');
-- command line
--hadoop fs -getmerge /user/rrm404/csvcount_1 ./fil.csv
--rm csvcount_1
--rm csvcount




data_taxi = LOAD 'zipcode_filter.csv' USING PigStorage(',') AS (drop_zip:int, pick_zip:int,drop_lat:double, drop_long:double, pcount:int, pick_lat:double, pick_long:double, tip:float, totalA:float, dist:float);
test_lim = LIMIT data_taxi 10;
DUMP test_lim;
data_taxi = FILTER data_taxi by (NOT ( drop_lat == 0.0 ) OR ( drop_lat == NULL ) or ( drop_long == 0.0 ) or ( drop_long == NULL ) or ( pick_lat == 0.0 ) or ( pick_lat == NULL ) or ( pick_long == 0.0 ) or ( pick_long == NULL ));
data_taxi_count = GROUP data_taxi BY drop_zip;
DESCRIBE data_taxi_count
zsum = FOREACH data_taxi_count GENERATE group,COUNT(data_taxi.drop_zip) AS mycount;
order_by_zsum = ORDER zsum BY group ASC;
STORE order_by_zsum into 'csvcount_o' using PigStorage(',','-schema');
-- command line
--hadoop fs -getmerge /user/rrm404/csvcount_o ./drop_zip_count_o.csv
--hdfs dfs -rm csvcount



data_taxi = LOAD 'zipcode_filter.csv' USING PigStorage(',') AS (drop_zip:int, pick_zip:int,drop_lat:double, drop_long:double, pcount:int, pick_lat:double, pick_long:double, tip:float, totalA:float, dist:float);
test_lim = LIMIT data_taxi 10;
DUMP test_lim;
data_taxi = FILTER data_taxi by (NOT ( drop_lat == 0.0 ) OR ( drop_lat == NULL ) or ( drop_long == 0.0 ) or ( drop_long == NULL ) or ( pick_lat == 0.0 ) or ( pick_lat == NULL ) or ( pick_long == 0.0 ) or ( pick_long == NULL ));
data_taxi_count = GROUP data_taxi BY pick_zip;
DESCRIBE data_taxi_count
zsum = FOREACH data_taxi_count GENERATE group,COUNT(data_taxi.pick_zip) AS mycount;
order_by_zsum = ORDER zsum BY group ASC;
STORE order_by_zsum into 'csvcount_o_p' using PigStorage(',','-schema');
-- command line
--hadoop fs -getmerge /user/rrm404/csvcount_o_p ./drop_zip_count_o_p.csv
--hdfs dfs -rm csvcount
