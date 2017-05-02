from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession

spark = SparkSession\
    .builder\
    .appName("LinearRegression")\
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

training = spark.read.format("libsvm").load('spark_ex_data/input/spark_reg_input.txt')
train, test = training.randomSplit([0.98, 0.02])

lr = LinearRegression(maxIter=10, regParam=0.01, elasticNetParam=0.0)
lrModel = lr.fit(train)
trainingSummary = lrModel.summary
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("Coefficients: %s" % str(lrModel.coefficients))

prediction = lrModel.transform(test)
prediction.describe().show()

rmse = prediction.rdd.map(lambda r: ((r[0] - r[2])**2)).mean()**.5

print("Test RMSE: %f\n" % rmse)

pr = prediction.rdd.map(lambda r: (r[0], r[2])).collect()
diff_count = 0
border_count = 0
for p in pr:
    label_grade = 'A' if p[0] < 14 else ('B' if p[0] < 28 else 'C')
    pred_grade = 'A' if p[1] < 14 else ('B' if p[1] < 28 else 'C')
    diff_count += 1 if not label_grade == pred_grade else 0
    if not label_grade == pred_grade:
        if (26 <= label_grade <= 30 and abs(label_grade - pred_grade) <= 4) or (12 <= label_grade <= 16 and abs(label_grade - pred_grade <= 4)):
            border_count += 1
    print(p[0],p[1],'\t',label_grade,pred_grade)

print('\nNo. of incorrect grade predictions: %d' % diff_count)
print('No. of border cases: %d' % border_count)