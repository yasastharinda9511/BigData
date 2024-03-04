from pyspark.sql import SparkSession
from pyspark.sql import Row

# Assuming you have a SparkSession named 'spark'
# If not, you can create one as follows:
# spark = SparkSession.builder.appName("YourAppName").getOrCreate()

# Your existing code...

# Create a list of Rows to represent your data
spark = SparkSession.builder.appName("AirlinesDelayAnalysis").getOrCreate()

result_string = "12345678 , 2 , 3 , 5"
result_rows = [Row(result=result_string)]


# Create a DataFrame from the list of Rows
result_df = spark.createDataFrame(result_rows)

# Specify the output CSV file path
csv_output_path = "./myOutputFolder/result.csv"

# Write the DataFrame to a CSV file in S3
result_df.write.csv(csv_output_path, header=True, mode="overwrite")

print(f"Result DataFrame written to CSV: {csv_output_path}")
