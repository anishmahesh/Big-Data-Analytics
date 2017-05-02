from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("RandomForestRegressorExample") \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    # Load and parse the data file, converting it to a DataFrame.
    data = spark.read.format("libsvm").load("spark_ex_data/input/spark_reg_input.txt")

    # Automatically identify categorical features, and index them.
    # Set maxCategories so features with > 4 distinct values are treated as continuous.
    featureIndexer = \
        VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(data)

    # Split the data into training and test sets (30% held out for testing)
    (trainingData, testData) = data.randomSplit([0.98, 0.02])

    # Train a RandomForest model.
    rf = RandomForestRegressor(featuresCol="indexedFeatures")

    # Chain indexer and forest in a Pipeline
    pipeline = Pipeline(stages=[featureIndexer, rf])

    # Train model.  This also runs the indexer.
    model = pipeline.fit(trainingData)

    # Make predictions.
    prediction = model.transform(testData)

    # Select (prediction, true label) and compute test error
    evaluator = RegressionEvaluator(
        labelCol="label", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(prediction)
    print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

    prediction.describe().show()

    pr = prediction.rdd.map(lambda r: (r[1], r[0])).collect()
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
    print('No. of border cases: %d\n' % border_count)

    rfModel = model.stages[1]
    print(rfModel)  # summary only