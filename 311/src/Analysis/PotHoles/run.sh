hadoop fs -rm -r output4
javac -classpath `yarn classpath`:. -d . *.java
jar cvf PotHoles.jar *.class
hadoop jar PotHoles.jar PotHole input2 output4
hadoop fs -getmerge output4 op.csv
