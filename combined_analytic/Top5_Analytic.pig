-- Getting the top 5 Zip codes:
-- 1. Top 5 Zip codes with maximum number of dropoff's
input_file = LOAD 'passenger_count_drop.csv' USING PigStorage(',') AS (col1:int, col2:int);
ranked = rank input_file;
NoHeader = Filter ranked by (rank_input_file > 1);
New_input_file = foreach NoHeader generate col1 as zipcode, col2 as Num_Drop;
order_by = ORDER New_input_file BY Num_Drop DESC;
order_by_lim =  LIMIT order_by 5;
STORE order_by_lim into 'Order_Num_Drop' using PigStorage(',');
--hadoop fs -getmerge /user/rrm404/Order_Num_Drop ./Order_Num_Drop.csv

-- 2. Top 5 Zip codes with maximum Average Tip
input_file = LOAD 'AverageTip_pertrip_drop.csv' USING PigStorage(',') AS (col1:int, col2:float);
ranked = rank input_file;
NoHeader = Filter ranked by (rank_input_file > 1);
New_input_file = foreach NoHeader generate col1 as zipcode, col2 as Tip_Drop;
order_by = ORDER New_input_file BY Tip_Drop DESC;
order_by_lim =  LIMIT order_by 5;
STORE order_by_lim into 'Order_Tip_Drop' using PigStorage(',');
--hadoop fs -getmerge /user/rrm404/Order_Tip_Drop ./Order_Tip_Drop.csv


-- 3. Top 5 Zip codes with maximum Average Total cost of ride of dropoff
input_file = LOAD 'AverageCostPerTrip_drop.csv' USING PigStorage(',') AS (col1:int, col2:float);
ranked = rank input_file;
NoHeader = Filter ranked by (rank_input_file > 1);
New_input_file = foreach NoHeader generate col1 as zipcode, col2 as Cost_Drop;
order_by = ORDER New_input_file BY Cost_Drop DESC;
order_by_lim =  LIMIT order_by 5;
STORE order_by_lim into 'Order_Cost_Drop' using PigStorage(',');
--hadoop fs -getmerge /user/rrm404/Order_Cost_Drop ./Order_Cost_Drop.csv


-- Getting All lattitudes and longitudes:
--Drop off zips
data_taxi = LOAD 'zipcode_filter.csv' USING PigStorage(',') AS (drop_zip:int, pick_zip:int,drop_lat:double, drop_long:double, pcount:int, pick_lat:double, pick_long:double, tip:float, totalA:float, dist:float);
selec = FOREACH data_taxi GENERATE drop_lat as Drop_lat,drop_long as Drop_long;
STORE selec into 'Lat_Long_Drop' using PigStorage(',');
--hadoop fs -getmerge /user/rrm404/Lat_Long_Drop ./Lat_Long_Drop.csv

--Pickup Zips
data_taxi = LOAD 'zipcode_filter.csv' USING PigStorage(',') AS (drop_zip:int, pick_zip:int,drop_lat:double, drop_long:double, pcount:int, pick_lat:double, pick_long:double, tip:float, totalA:float, dist:float);
selec = FOREACH data_taxi GENERATE pick_lat as Pick_lat,pick_long as Pick_long;
STORE selec into 'Lat_Long_Pick' using PigStorage(',');
--hadoop fs -getmerge /user/rrm404/Lat_Long_Pick ./Lat_Long_Pick.csv


--CRIME - RATIO:
data_pop = LOAD '2010+Census+Population+By+Zipcode+(ZCTA).csv' USING PigStorage(',') AS (col1:int,col2:int);
data_crime = LOAD 'crime_zipcode_analysis.csv' USING PigStorage(',');
X = FOREACH data_crime GENERATE $0 as zip, $1+$2 as sum;
ranked = rank data_pop;
NoHeader = Filter ranked by (rank_data_pop > 1);
New_data_pop = foreach NoHeader generate col1 as zipcode, col2 as Cnt;
Data_join = JOIN X BY zip, New_data_pop BY zipcode;
Final = FOREACH Data_join GENERATE $0 as zip, $1/$3 as ratio;
order_by = ORDER Final BY ratio DESC;
order_by_lim =  LIMIT order_by 5;
STORE order_by_lim into 'Crime_Top5_Ratio' using PigStorage(',');
--hadoop fs -getmerge /user/rrm404/Crime_Top5_Ratio ./Crime_Top5_Ratio.csv


--CRIME - SUM:
data_pop = LOAD '2010+Census+Population+By+Zipcode+(ZCTA).csv' USING PigStorage(',') AS (col1:int,col2:int);
data_crime = LOAD 'crime_zipcode_analysis.csv' USING PigStorage(',');
X = FOREACH data_crime GENERATE $0 as zip, $1+$2 as sum;
ranked = rank data_pop;
NoHeader = Filter ranked by (rank_data_pop > 1);
New_data_pop = foreach NoHeader generate col1 as zipcode, col2 as Cnt;
Data_join = JOIN X BY zip, New_data_pop BY zipcode;
Final = FOREACH Data_join GENERATE $0 as zip, $1 as S;
order_by = ORDER Final BY S DESC;
order_by_lim =  LIMIT order_by 5;
STORE order_by_lim into 'Crime_Top5_SUM' using PigStorage(',');
--hadoop fs -getmerge /user/rrm404/Crime_Top5_SUM ./Crime_Top5_SUM.csv


--311 - RATIO
data_pop = LOAD '2010+Census+Population+By+Zipcode+(ZCTA).csv' USING PigStorage(',') AS (col1:int,col2:int);
data_311 = LOAD '311.csv' USING PigStorage(',');
X = FOREACH data_311 GENERATE $0 as zip, $1+$2+$3+$4 as sum;
ranked = rank data_pop;
NoHeader = Filter ranked by (rank_data_pop > 1);
New_data_pop = foreach NoHeader generate col1 as zipcode, col2 as Cnt;
Data_join = JOIN X BY zip, New_data_pop BY zipcode;
Final = FOREACH Data_join GENERATE $0 as zip, $1/$3 as ratio;
order_by = ORDER Final BY ratio DESC;
order_by_lim =  LIMIT order_by 5;
STORE order_by_lim into '311_Top5_Ratio' using PigStorage(',');
--hadoop fs -getmerge /user/rrm404/311_Top5_Ratio ./311_Top5_Ratio.csv



--311 - SUM
data_pop = LOAD '2010+Census+Population+By+Zipcode+(ZCTA).csv' USING PigStorage(',') AS (col1:int,col2:int);
data_311 = LOAD '311.csv' USING PigStorage(',');
X = FOREACH data_311 GENERATE $0 as zip, $1+$2+$3+$4 as sum;
ranked = rank data_pop;
NoHeader = Filter ranked by (rank_data_pop > 1);
New_data_pop = foreach NoHeader generate col1 as zipcode, col2 as Cnt;
Data_join = JOIN X BY zip, New_data_pop BY zipcode;
Final = FOREACH Data_join GENERATE $0 as zip, $1 as S;
order_by = ORDER Final BY S DESC;
order_by_lim =  LIMIT order_by 5;
STORE order_by_lim into '311_Top5_Sum' using PigStorage(',');
--hadoop fs -getmerge /user/rrm404/311_Top5_Sum ./311_Top5_Sum.csv


-- cab - RATIO
data_pop = LOAD '2010+Census+Population+By+Zipcode+(ZCTA).csv' USING PigStorage(',') AS (col_p1:int,col_p2:int);
input_file = LOAD 'passenger_count_drop.csv' USING PigStorage(',') AS (col1:int, col2:int);
ranked = rank input_file;
NoHeader = Filter ranked by (rank_input_file > 1);
New_input_file = foreach NoHeader generate col1 as zip, col2 as Num_Drop;
ranked1 = rank data_pop;
NoHeader1 = Filter ranked1 by (rank_data_pop > 1);
New_data_pop = foreach NoHeader1 generate col_p1 as zipcode, col_p2 as Cnt;
Data_join = JOIN New_input_file BY zip, New_data_pop BY zipcode;
Final = FOREACH Data_join GENERATE $0 as zip, $1/$3 as ratio;
order_by = ORDER Final BY ratio DESC;
order_by_lim =  LIMIT order_by 15;
STORE order_by_lim into 'Top5_Num_Drop_Ratio' using PigStorage(',');
--hadoop fs -getmerge /user/rrm404/Top5_Num_Drop_Ratio ./Top5_Num_Drop_Ratio.csv
