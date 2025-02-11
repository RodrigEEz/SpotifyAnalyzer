import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder\
   .master("local[*]")\
   .appName('test')\
   .getOrCreate()

data = [("Airflow", 1), ("Spark", 2), ("Connection", 3)]
df = spark.createDataFrame(data, ["word", "count"])
df.show()
spark.stop()