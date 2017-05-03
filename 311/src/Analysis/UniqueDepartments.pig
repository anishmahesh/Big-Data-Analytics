/*Pig Script to generate Unique departments which handled 311 requests
  These department type and type of complaints were then used to assign
  a severity score to a complaint - USING Severity Map Reduce Job
*/

data = LOAD 'input2' USING PigStorage(',');
grouped_data = GROUP a BY ($3,$4);
dept_desc = FOREACH grouped_data GENERATE FLATTEN(group) as (Agency,Agency_Name);
STORE dept_desc AS 'depts';
