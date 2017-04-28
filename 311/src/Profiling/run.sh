hadoop fs -rm -r output
javac -classpath `yarn classpath`:. -d . *.java
jar cvf Profiling.jar *.class
hadoop jar Profiling.jar Profiling input2 output
hadoop fs -cat output/*
