from pyspark.ml import Pipeline
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.context import SparkContext
from pyspark.mllib.tree import DecisionTree


spark = SparkContext('local')
from pyspark.sql import SQLContext
from pyspark.sql.types import *
sqlContext = SQLContext(spark)

customSchema = StructType([ \
    StructField("zip", IntegerType(), True), \
    StructField("Rating1_311", IntegerType(), True), \
    StructField("Rating2_311", IntegerType(), True), \
    StructField("Rating3_311", IntegerType(), True), \
    StructField("Rating4_311", IntegerType(), True)])

# Load the data stored in LIBSVM format as a DataFrame.
#data = sqlContext.csvFile("output5")
#data = sqlContext.load(source="com.databricks.spark.csv", header="false", path = "output5")
data = sqlContext.read \
    .format('com.databricks.spark.csv') \
    .options(header='false') \
    .load('output5', schema = customSchema)
'''
# Automatically identify categorical features, and index them.
# We specify maxCategories so features with > 4 distinct values are treated as continuous.
featureIndexer =\
    VectorIndexer(inputCol="Rating2_311", outputCol="zip").fit(data)

print(featureIndexer)
'''
print("\n\n\nTEST\n\n\n")
# Split the data into training and test sets (30% held out for testing)
(trainingData, testData) = data.randomSplit([0.7, 0.3])
print(testData)

model = DecisionTree.trainRegressor(trainingData, categoricalFeaturesInfo={},
                                    impurity='variance', maxDepth=5, maxBins=32)
print(model.toString())
#exclude = ['zip']
# Evaluate model on test instances and compute test error
#predictions = model.predict(testData.map(lambda x: x.features not in exclude))
#labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
#testMSE = labelsAndPredictions.map(lambda (v, p): (v - p) * (v - p)).sum() /\
#    float(testData.count())
#print('Test Mean Squared Error = ' + str(testMSE))
#print('Learned regression tree model:')
#print(model.toDebugString())

'''
from pyspark.ml.feature import VectorAssembler

ignore = ['zip']
assembler = VectorAssembler(
    inputCols=[x for x in data.columns if x not in ignore],
    outputCol='features')

assembler.transform(data)
print(type(assembler))
print(assembler)
# Train a DecisionTree model.
#dt = DecisionTreeRegressor(featuresCol='features')
#model = dt.fit(trainingData)
#print(model)

# Chain indexer and tree in a Pipeline
pipeline = Pipeline(stages=[['Rating1_311'], dt])

# Train model.  This also runs the indexer.
model = pipeline.fit(trainingData)

# Make predictions.
predictions = model.transform(testData)

# Select example rows to display.
predictions.select("prediction", "label", "features").show(5)

# Select (prediction, true label) and compute test error
evaluator = RegressionEvaluator(
    labelCol="label", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

treeModel = model.stages[1]
# summary only
print(treeModel)
'''
