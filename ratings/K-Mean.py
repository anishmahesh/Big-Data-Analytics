from __future__ import print_function
from pyspark.ml.clustering import KMeans
from pyspark.sql import SparkSession

spark = SparkSession\
    .builder\
    .appName("KMeansExample")\
    .getOrCreate()


spark.sparkContext.setLogLevel('ERROR')

dataset = spark.read.format("libsvm").load("project/input/list1.txt")

kmeans = KMeans().setK(5).setSeed(1)
model = kmeans.fit(dataset)

wssse = model.computeCost(dataset)
print("Within Set Sum of Squared Errors = " + str(wssse))

centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)
