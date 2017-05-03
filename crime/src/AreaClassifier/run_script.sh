#!/bin/bash

javac -classpath `yarn classpath`:. -d . AreaClassifier.java
jar cvf areaClassifier.jar *.class
hdfs dfs -rm -r -f  project/output/Top5_311
hadoop jar areaClassifier.jar AreaClassifier project/input/Top5_K-Mean.csv project/output/Top5_311
