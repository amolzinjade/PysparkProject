from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('company').getOrCreate()
sqlContext = SQLContext(spark)
data = spark.read.csv("emp_data.csv")
data1 = data.write.parquet("output.parquet")
data2 = spark.read.parquet("output.parquet")
data2.show()

