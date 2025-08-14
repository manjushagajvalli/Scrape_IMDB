from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

spark = SparkSession.builder.appName("SampleCode").getOrCreate()

data = [("A", 1), ("B", 2), ("A", 3), ("B", 4)]
columns = ["Category", "Value"]
df = spark.createDataFrame(data, columns)

grouped_df = df.groupBy("Category").agg(sum("Value").alias("TotalValue")) 

grouped_df.show()
