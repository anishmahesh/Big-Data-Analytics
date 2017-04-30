data = LOAD 'input2' USING PigStorage(',');
grouped_data = GROUP a BY ($3,$4);
dept_desc = FOREACH grouped_data GENERATE FLATTEN(group) as (Agency,Agency_Name);
STORE dept_desc AS 'depts';
