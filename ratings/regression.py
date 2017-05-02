from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession

spark = SparkSession\
    .builder\
    .appName("LinearRegression")\
    .getOrCreate()

training = spark.read.format("libsvm").load('spark_ex_data/input/rbda_data.txt')
train, test = training.randomSplit([0.9, 0.1])

lr = LinearRegression(maxIter=10, regParam=0.01, elasticNetParam=0.0)
lrModel = lr.fit(train)
trainingSummary = lrModel.summary
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("Coefficients: %s" % str(lrModel.coefficients))

prediction = lrModel.transform(test)
prediction.describe().show()

rmse = prediction.rdd.map(lambda r: ((r[0] - r[2])**2)).mean()**.5

print("Test RMSE: %f" % rmse)

# pr = prediction.rdd.map(lambda r: (r[0], r[2])).collect()
# for p in pr:
#     print(p[0],p[2])