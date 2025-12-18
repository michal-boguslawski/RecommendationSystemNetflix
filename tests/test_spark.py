from pyspark.sql import SparkSession

spark = SparkSession.builder.master("spark://spark-master:7077").appName("TestSpark").getOrCreate()
data = spark.range(1, 101)
print("Sum:", data.groupBy().sum().collect())
spark.stop()
