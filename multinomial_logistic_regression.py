from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# Load training data
data = spark \
    .read \
    .format("libsvm") \
    .load("/srv/spark-2.1.1/data/mllib/sample_multiclass_classification_data.txt")

# Show first two row features
sample=data.select("features").take(2)

# Split the data into training and test sets (30% held out for testing)
(trainingData, testData) = data.randomSplit([0.7, 0.3])

# Define Model
lr=LogisticRegression()

# Define Grid
paramGrid = ParamGridBuilder() \
    .addGrid(lr.maxIter, [10, 50, 100]) \
    .addGrid(lr.regParam, [0.01, 0.1, 1,10]) \
    .addGrid(lr.elasticNetParam, [0.01,0.1, 0.5, 1]) \
    .build()

# Define Crossvalidation
crossval = CrossValidator(estimator=lr,
                          estimatorParamMaps=paramGrid,
                          evaluator=MulticlassClassificationEvaluator(),
                          numFolds=3)

# Run cross-validation, and choose the best set of parameters.
cvModel = crossval.fit(trainingData)

# Get the accuracy for each model
cvModel.avgMetrics

# Get the best model
bestModel = cvModel.bestModel

# Show the best model parameters
bestModel._java_obj.getRegParam()
bestModel._java_obj.getElasticNetParam()
bestModel._java_obj.getMaxIter()

# Make predictions with the bestModel
predictions = bestModel.transform(testData)

# Select example rows to display.
predictions.select("label","prediction", "features").show(20)

# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(labelCol="label",
predictionCol="prediction",
metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Accuracy = %g " % (accuracy))
