import numpy as np
import sys
import os
import psutil
import time
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, col
from pyspark.sql.types import FloatType
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from sklearn.metrics import classification_report

OPTIMIZED = True if sys.argv[1] == "True" else False
time_start = time.time()


SparkContext.getOrCreate(SparkConf().setMaster('spark://spark-master:7077')).setLogLevel("INFO")
spark = SparkSession.builder.master("spark://spark-master:7077").appName("practice").getOrCreate()


data = spark.read.format("csv").option("header", "true").option('inferSchema', 'true').load("hdfs://namenode:9000/data/train.csv")
if OPTIMIZED:
    data.cache()
    data = data.repartition(5)


for col in ['Gender', 'Customer Type', 'Type of Travel']:
    indexer = StringIndexer(inputCol=col, outputCol= col + "_index")
    data = indexer.fit(data).transform(data)
    data = data.drop(col)
    data = data.withColumnRenamed(col + "_index", col)
    one_hot_encoder = OneHotEncoder(inputCol=col,
                                    outputCol=col + "_one_hot")
    one_hot_encoder = one_hot_encoder.fit(data)
    data = one_hot_encoder.transform(data)
    data = data.drop(col)
    data = data.withColumnRenamed(col + "_one_hot", col)



numeric_cols = ['Gender', 'Customer Type', 'Age', 'Type of Travel',
       'Flight Distance', 'Inflight wifi service',
       'Departure/Arrival time convenient', 'Ease of Online booking',
       'Gate location', 'Food and drink', 'Online boarding', 'Seat comfort',
       'Inflight entertainment', 'On-board service', 'Leg room service',
       'Baggage handling', 'Checkin service', 'Inflight service',
       'Cleanliness', 'Departure Delay in Minutes', 'Arrival Delay in Minutes']
assembler = VectorAssembler(inputCols=numeric_cols, outputCol='vectorized_data')
data = assembler.transform(data)

data_train, data_test = data.randomSplit([0.75, 0.25])
if OPTIMIZED:
    data_train.cache()
    data_train = data_train.repartition(5)
    data_test.cache()
    data_test = data_test.repartition(5)


model = LogisticRegression(featuresCol="vectorized_data", labelCol="satisfaction", maxBins=700)
model = model.fit(data_train)


pred_test = model.transform(data_test)

pred_test = pred_test.toPandas()
y_true = pred_test["satisfaction"].values
y_pred = pred_test["prediction"].values
labels = [1, 0]
print(classification_report(y_true, y_pred, zero_division=0, target_names=labels))

time_res = time.time() - time_start
RAM_res = psutil.Process(os.getpid()).memory_info().rss / (float(1024)**2)

spark.stop()

with open('/log.txt', 'a') as f:
    f.write("Time: " + str(time_res) + " seconds, RAM: " + str(RAM_res) + " Mb.\n")