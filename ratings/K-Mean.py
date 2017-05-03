from __future__ import print_function
from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark.sql import SparkSession
import numpy as np
from math import sqrt

spark = SparkSession\
    .builder\
    .appName("KMeansExample")\
    .getOrCreate()


spark.sparkContext.setLogLevel('ERROR')

data = spark.sparkContext.textFile("project/input/lat_long_list.txt")

parsedData = data.map(lambda line: np.array([float(x) for x in line.split(',')]))


clusters = KMeans.train(parsedData, 40, maxIterations=50, initializationMode="random")

def error(point):
    center = clusters.centers[clusters.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))

WSSSE = parsedData.map(lambda point: error(point)).reduce(lambda x, y: x + y)
print("Within Set Sum of Squared Error = " + str(WSSSE))

center_dict = {}
def createDict(point):
    center = clusters.centers[clusters.predict(point)]
    if not center in center_dict:
        center_dict[center] = []
    center_dict[center].append(point)

parsedData.map(lambda point: createDict(point))

print(center_dict)

count = 0
for k in sorted(center_dict, key=lambda k: len(center_dict[k]), reverse=True):
        if not count == 5:
            print(k +  '\t' + center_dict[k])
        else:
            break
        count += 1