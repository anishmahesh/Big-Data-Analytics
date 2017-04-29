!/bin/bash

javac -classpath `yarn classpath`:. -d . DataCleaner.java
jar cvf dataCleaner.jar *.class
rm -rf /user/nk2239/Proj/CleanData
hadoop jar dataCleaner.jar DataCleaner /user/nk2239/Proj/Input.csv /user/nk2239/Proj/CleanData
