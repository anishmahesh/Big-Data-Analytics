from __future__ import print_function
from pyspark.ml.clustering import KMeans
from pyspark.sql import SparkSession

spark = SparkSession\
    .builder\
    .appName("KMeansExample")\
    .getOrCreate()


spark.sparkContext.setLogLevel('ERROR')

dataset = spark.read.format("libsvm").load("project/input/list1.txt")

kmeans = KMeans().setK(40).setSeed(1)
model = kmeans.fit(dataset)

wssse = model.computeCost(dataset)
print("Within Set Sum of Squared Errors = " + str(wssse))

centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)

print(model)

center_dict = {}
def createDict(point):
    center = centers[model.predict(point)]
    if not center in center_dict:
        center_dict[center] = []
    center_dict[center].append(point)


dataset.rdd.map(lambda point: createDict(point))

count = 0
for k in sorted(center_dict, key=lambda k: len(center_dict[k]), reverse=True):
        if not count == 5:
            print(k +  '\t' + center_dict[k])
        else:
            break
        count += 1