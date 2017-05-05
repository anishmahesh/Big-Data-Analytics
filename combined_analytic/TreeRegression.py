from pyspark.ml import Pipeline
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.context import SparkContext
from pyspark.mllib.tree import DecisionTree
from pyspark.mllib.regression import LabeledPoint

from pyspark.sql import SQLContext
from pyspark.sql.types import *

from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.mllib.linalg import Vectors


__author__ = "Sanchit Mehta"

customSchema = StructType([ \
    StructField("zip", IntegerType(), True), \
    StructField("Rating1_311", IntegerType(), True), \
    StructField("Rating2_311", IntegerType(), True), \
    StructField("Rating3_311", IntegerType(), True), \
    StructField("Rating4_311", IntegerType(), True)])

'''
EL NUEVO AMANECER RESTAURANT,10002,1,0,0,0,0,0,0,6,2,7,5,20,12,22,0,1,0,10002,346,1666,5170,1197,10002,27351.0,13265.0,652.0,39964.0,4833.0,24512.0,11271.0
'''
# Load and parse the data
def parsePoint(line):
    values = [int(x) for x in line.split(',')[2:15]]
    values.extend([int(x) for x in line.split(',')[20:23]])
    values.extend([int(x) for x in line.split(',')[25:31]])
    values.insert(0,line.split(',')[15])
    return LabeledPoint(values[0], values[1:])

sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)
data = sc.textFile("merged/merged.csv")
print("\n\n\nTESTTTTTTTTT\n\n\n")
parsedData = data.map(parsePoint)

(trainingData, testData) = data.randomSplit([0.7, 0.3])
print(testData)

model = DecisionTree.trainRegressor(parsedData, categoricalFeaturesInfo={},
                                    impurity='variance', maxDepth=5, maxBins=32)
print(model)

# Make predictions.
predictions = model.transform(testData)

# Select example rows to display.
predictions.select("prediction", "label", "features").show(5)
