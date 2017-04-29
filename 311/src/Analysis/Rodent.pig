data = LOAD 'output2' USING PigStorage(',') AS (agency:chararray, complaintType:chararray,locationType:chararray,zip:int,facilityType:chararray,lat:chararray,lng:chararray);
rodents = FILTER data BY complaintType MATCHES 'RODENT';
grouped_data = GROUP rodents by zip;
final_res = FOREACH grouped_data GENERATE group, COUNT(rodents.complaintType) AS CountRodentComplaints;
dump final_res;

