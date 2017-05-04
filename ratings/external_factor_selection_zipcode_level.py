from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorIndexer
from pyspark.sql.functions import col

spark = SparkSession \
    .builder \
    .appName("Feature Selection") \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

model_to_use = 'linear'
# model_to_use = 'random_forest'

label_cols = ['grade_A_pct', 'grade_B_pct', 'grade_C_pct']
cols_to_keep = ['crime_sev1','crime_sev2','crime_attempted','crime_completed','violation','misdemeanor','felony','311_sev1','311_sev2','311_sev3','311_sev4','avg_cost','avg_tip','passenger_count']
pre_label_cols = ['grade_A_count','grade_B_count','grade_C_count']

cols_to_select = cols_to_keep + pre_label_cols

df = spark.read.csv('spark_ex_data/input/MergedRestZip_with_headers.csv', header=True)
df = df.select(*cols_to_select)
df = df.select(*(col(c).cast("double").alias(c) for c in df.columns))
df = df.withColumn('grade_A_pct', df.grade_A_count / (df.grade_A_count + df.grade_B_count + df.grade_C_count))
df = df.withColumn('grade_B_pct', df.grade_B_count / (df.grade_A_count + df.grade_B_count + df.grade_C_count))
df = df.withColumn('grade_C_pct', df.grade_C_count / (df.grade_A_count + df.grade_B_count + df.grade_C_count))

assembler = VectorAssembler(inputCols=cols_to_keep, outputCol='features')

transformed = assembler.transform(df)

for label_col in label_cols:
    training = transformed.select(col(label_col).alias('label'), 'features')

    print('\n\n==========='+label_col+'=============\n')

    # Running linear regression with elastic net
    print('\n-----Elastic Net-------\n')

    lr = LinearRegression(maxIter=10, regParam=0.01, elasticNetParam=0.5)
    lrModel = lr.fit(training)
    trainingSummary = lrModel.summary
    print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
    coeffs = lrModel.coefficients.toArray()

    features_selected = [(z[0], z[1]) for z in zip(cols_to_keep, coeffs) if not z[1] == 0]
    features_selected = sorted(features_selected, key=lambda x: abs(x[1]))[::-1]

    print('Features selected:\n')
    for (feature, coeff) in features_selected:
        print(feature, coeff)


    # Running random forest
    print('\n-----Random Forest-------\n')

    featureIndexer = VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(training)
    rf = RandomForestRegressor(featuresCol="indexedFeatures")

    # Chain indexer and forest in a Pipeline
    pipeline = Pipeline(stages=[featureIndexer, rf])

    # Train model.  This also runs the indexer.
    model = pipeline.fit(training)
    rfModel = model.stages[1]
    rf_coeffs = rfModel.featureImportances.toArray()
    rf_features_selected = [(z[0], z[1]) for z in zip(cols_to_keep, rf_coeffs)]
    rf_features_selected = sorted(rf_features_selected, key=lambda x: x[1])[::-1]

    print('Features selected:\n')
    for f in rf_features_selected[:len(features_selected)]:
        print(f)
