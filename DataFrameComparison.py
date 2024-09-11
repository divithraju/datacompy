# Importing necessary modules
from pyspark.sql import SparkSession
from datacompy.spark import SparkSQLCompare

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("DataFrameComparison") \
    .getOrCreate()

# Example data for comparison
data_1 = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
data_2 = [(1, "Alice"), (2, "Bob"), (4, "David")]

df1 = spark.createDataFrame(data_1, ["ID", "Name"])
df2 = spark.createDataFrame(data_2, ["ID", "Name"])

# Using SparkSQLCompare for DataFrame comparison
compare = SparkSQLCompare(df1, df2, join_columns=["ID"])
comparison_result = compare.report()

# Output result
print(comparison_result)

# Stop Spark session
spark.stop()


