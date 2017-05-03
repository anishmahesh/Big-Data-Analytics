from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col

spark = SparkSession\
    .builder\
    .appName("LinearRegression")\
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

label_col = 'score'
cols_to_keep = ['american','asian','continental','indian','italian','mexican','middle_eastern','vio_facilities','vio_food_handlin','vio_hygiene','vio_vermin','total_violations','critical_violations','sev1','sev2','sev3','sev4','severity1','severity2','attempted','completed','violation','misdemeanor','felony']

df = spark.read.csv('spark_ex_data/input/merged_with_headers.csv', header=True)
df = df.select(col(label_col).alias('label'), *cols_to_keep)
df = df.select(*(col(c).cast("float").alias(c) for c in df.columns))

assembler = VectorAssembler(inputCols=cols_to_keep, outputCol='features')

training = assembler.transform(df).select('label', 'features')

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
        if (26 <= p[0] <= 30 and abs(p[0] - p[1]) <= 4) or (12 <= p[0] <= 16 and abs(p[0] - p[1] <= 4)):
            border_count += 1
    print((str(p[0]) + ' ' + str(p[1]) + '\t' + label_grade + ' ' + pred_grade).expandtabs(5))

print('\nNo. of incorrect grade predictions: %d' % diff_count)
print('No. of border cases: %d' % border_count)