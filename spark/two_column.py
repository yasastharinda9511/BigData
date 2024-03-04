from pyspark.sql import SparkSession
from pyspark.sql import Row

result_data = [(1, 1.52), (2, 1.34), (3, 1.77), (4, 1.98), (5, 2.27)]

spark = SparkSession.builder.appName("AirlinesDelayAnalysis").getOrCreate()
# Create a DataFrame from the list of tuples
result_df = spark.createDataFrame(result_data, ["Iteration", "ExecutionTime"])

result_df = result_df.coalesce(1) 
# Specify the output CSV file path
csv_output_path = "./myOutputFolder/result.csv"

# Write the DataFrame to a CSV file in S3
result_df.write.csv(csv_output_path, header=True, mode="overwrite")

print(f"Result DataFrame written to CSV: {csv_output_path}")