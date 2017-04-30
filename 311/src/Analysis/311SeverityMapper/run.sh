hadoop fs -rm -r output5
javac -classpath `yarn classpath`:. -d . *.java
jar cvf RateIt.jar *.class
hadoop jar RateIt.jar Severity input4 output5
hadoop fs -getmerge output5 op.csv
