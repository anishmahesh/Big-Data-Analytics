--Individual Analytics
--1.
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



--2.
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


--3.
data_taxi = LOAD 'zipcode_filter.csv' USING PigStorage(',') AS (drop_zip:int, pick_zip:int,drop_lat:double, drop_long:double, pcount:int, pick_lat:double, pick_long:double, tip:float, totalA:float, dist:float);
data_taxi = FILTER data_taxi by (NOT ( drop_lat == 0.0 ) OR ( drop_lat == NULL ) or ( drop_long == 0.0 ) or ( drop_long == NULL ) or ( pick_lat == 0.0 ) or ( pick_lat == NULL ) or ( pick_long == 0.0 ) or ( pick_long == NULL ));
data_taxi_count = GROUP data_taxi BY pick_zip;
DESCRIBE data_taxi_count
zsum = FOREACH data_taxi_count GENERATE group,COUNT(data_taxi.pick_zip) AS mycount;
order_by_zsum = ORDER zsum BY group ASC;
STORE order_by_zsum into 'csvcount_o_p' using PigStorage(',','-schema');
-- command line
--hadoop fs -getmerge /user/rrm404/csvcount_o_p ./pick_zip_count_o_p.csv
--hdfs dfs -rm csvcount


--GROUP Analytics
--4.
-- Total passenger count per zip code(pick zip)
data_taxi = LOAD 'zipcode_filter.csv' USING PigStorage(',') AS (drop_zip:int, pick_zip:int,drop_lat:double, drop_long:double, pcount:int, pick_lat:double, pick_long:double, tip:float, totalA:float, dist:float);
data_taxi_count = GROUP data_taxi BY pick_zip;
DESCRIBE data_taxi_count
psum = FOREACH data_taxi_count GENERATE group as zipcode,SUM(data_taxi.pcount) AS Pass_count;
order_by_psum = ORDER psum BY zipcode ASC;
STORE order_by_psum into 'passenger_count_pick' using PigStorage(',','-schema');

-- command line
--hadoop fs -getmerge /user/rrm404/passenger_count_pick ./passenger_count_pick.csv
--hdfs dfs -rm csvcount



--5.
-- Total passenger count per zip code(drop zip)
data_taxi = LOAD 'zipcode_filter.csv' USING PigStorage(',') AS (drop_zip:int, pick_zip:int,drop_lat:double, drop_long:double, pcount:int, pick_lat:double, pick_long:double, tip:float, totalA:float, dist:float);
data_taxi_count = GROUP data_taxi BY drop_zip;
DESCRIBE data_taxi_count
dsum = FOREACH data_taxi_count GENERATE group as zipcode,SUM(data_taxi.pcount) AS Pass_count;
order_by_dsum = ORDER dsum BY zipcode ASC;
STORE order_by_dsum into 'passenger_count_drop' using PigStorage(',','-schema');

-- command line
--hadoop fs -getmerge /user/rrm404/passenger_count_drop ./passenger_count_drop.csv
--hdfs dfs -rm csvcount

--6.
--(Total cost per zip code)/(total number of trips) - pick
data_taxi = LOAD 'zipcode_filter.csv' USING PigStorage(',') AS (drop_zip:int, pick_zip:int,drop_lat:double, drop_long:double, pcount:int, pick_lat:double, pick_long:double, tip:float, totalA:float, dist:float);
data_taxi_count = GROUP data_taxi BY pick_zip;
DESCRIBE data_taxi_count
costsum = FOREACH data_taxi_count GENERATE group as zipcode,SUM(data_taxi.totalA)/COUNT(data_taxi.totalA) AS TotalAVGCost;
order_by_cost = ORDER costsum BY zipcode ASC;
STORE order_by_cost into 'AverageCost_pertrip_pick' using PigStorage(',','-schema');

-- command line
--hadoop fs -getmerge /user/rrm404/AverageCost_pertrip_pick ./AverageCost_pertrip_pick.csv
--hdfs dfs -rm csvcount


--7.
--(Total cost per zip code)/(total number of trips) - drop
data_taxi = LOAD 'zipcode_filter.csv' USING PigStorage(',') AS (drop_zip:int, pick_zip:int,drop_lat:double, drop_long:double, pcount:int, pick_lat:double, pick_long:double, tip:float, totalA:float, dist:float);
data_taxi_count = GROUP data_taxi BY drop_zip;
DESCRIBE data_taxi_count
costsum = FOREACH data_taxi_count GENERATE group as zipcode,SUM(data_taxi.totalA)/COUNT(data_taxi.totalA) AS TotalAVGCost;
order_by_cost = ORDER costsum BY zipcode ASC;
STORE order_by_cost into 'AverageCost_pertrip_drop' using PigStorage(',','-schema');

-- command line
--hadoop fs -getmerge /user/rrm404/AverageCost_pertrip_drop ./AverageCostPerTrip_drop.csv
--hdfs dfs -rm csvcount



--8.
--Average tip per zip code - pick

data_taxi = LOAD 'zipcode_filter.csv' USING PigStorage(',') AS (drop_zip:int, pick_zip:int,drop_lat:double, drop_long:double, pcount:int, pick_lat:double, pick_long:double, tip:float, totalA:float, dist:float);
data_taxi_count = GROUP data_taxi BY pick_zip;
DESCRIBE data_taxi_count
costsum = FOREACH data_taxi_count GENERATE group as zipcode,SUM(data_taxi.tip)/COUNT(data_taxi.tip) AS AvgTip;
order_by_cost = ORDER costsum BY zipcode ASC;
STORE order_by_cost into 'AverageTip_pertrip_pick' using PigStorage(',','-schema');
-- command line
--hadoop fs -getmerge /user/rrm404/AverageTip_pertrip_pick ./AverageTip_pertrip_pick.csv
--hdfs dfs -rm csvcount


--9.
--Average tip per zip code - drop

data_taxi = LOAD 'zipcode_filter.csv' USING PigStorage(',') AS (drop_zip:int, pick_zip:int,drop_lat:double, drop_long:double, pcount:int, pick_lat:double, pick_long:double, tip:float, totalA:float, dist:float);
data_taxi_count = GROUP data_taxi BY drop_zip;
DESCRIBE data_taxi_count
costsum = FOREACH data_taxi_count GENERATE group as zipcode,SUM(data_taxi.tip)/COUNT(data_taxi.tip) AS AvgTip;
order_by_cost = ORDER costsum BY zipcode ASC;
STORE order_by_cost into 'AverageTip_pertrip_drop' using PigStorage(',','-schema');
-- command line
--hadoop fs -getmerge /user/rrm404/AverageTip_pertrip_drop ./AverageTip_pertrip_drop.csv
--hdfs dfs -rm csvcount
