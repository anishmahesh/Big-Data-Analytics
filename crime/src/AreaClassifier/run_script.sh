#!/bin/bash

javac -classpath `yarn classpath`:. -d . AreaClassifier.java
jar cvf areaClassifier.jar *.class
hdfs dfs -rm -r -f  project/output/Zipcode1
hadoop jar areaClassifier.jar AreaClassifier project/input/FINAL_REQUIRED.csv project/output/Zipcode1
