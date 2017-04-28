hadoop fs -rm -r output
javac -classpath `yarn classpath`:. -d . *.java
jar cvf Cleaning.jar *.class
hadoop jar Cleaning.jar CleanData input2 output
#hadoop fs -cat output/*
