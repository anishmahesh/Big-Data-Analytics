!/bin/bash

javac -classpath `yarn classpath`:. -d . AreaClassifier.java
jar cvf areaClassifier.jar *.class
rm -rf  project/output/Zipcode1
hadoop jar areaClassifier.jar AreaClassifier project/input/NEW_FINAL_NYPD_Complaint_Data_Historic.csv project/output/Zipcode1
