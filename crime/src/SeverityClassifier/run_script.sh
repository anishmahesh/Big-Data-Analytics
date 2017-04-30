#!/bin/bash

javac -classpath `yarn classpath`:. -d . SeverityClassifier.java
jar cvf severityClassifier.jar *.class
hdfs dfs -rm -r -f  project/output/Zipcode2
hadoop jar severityClassifier.jar SeverityClassifier project/input/NEW_FINAL_NYPD_Complaint_Data_Historic.csv project/output/Zipcode2
