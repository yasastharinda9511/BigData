
from pyspark.sql import SparkSession
from pyspark.sql import Row
import time

def run_and_save_query(spark, query,output_path):
    # Run the SQL query
    start_time = time.time()
    result =  spark.sql(query)
    end_time = time.time()

    result.write.mode("overwrite").csv(output_path, header=True)

    return end_time - start_time


spark = SparkSession.builder.appName("AirlinesDelayAnalysis").getOrCreate()

# Load the Airlines Dataset into a DataFrame
input_path = 's3://yasashadoopbucket/AirlineAssignmentData/Dataset/DelayedFlights-updated.csv'
#input_path = './DelayedFlights-updated.csv'
df = spark.read.csv(input_path, header=True, inferSchema=True)

# Register the DataFrame as a temporary SQL table
df.createOrReplaceTempView("delay_flights")

# Define the output path on Amazon S3
output_base_path = 's3://yasashadoopbucket/myOutputFolder'
#output_base_path = './myOutputFolder'

# Run and save each query
queries = {
    "YearlyCarrierDelay": "SELECT Year, avg((CarrierDelay/ArrDelay) * 100) AS avg_carrier_delay_percentage FROM delay_flights GROUP BY Year",
    "YearlyNASDelay": "SELECT Year, AVG((NASDelay/ArrDelay) *100) AS avg_nas_delay FROM delay_flights WHERE Year BETWEEN 2003 AND 2010 GROUP BY Year",
    "YearlyWeatherDelay": "SELECT Year, AVG((WeatherDelay/ArrDelay)*100) AS avg_weather_delay FROM delay_flights WHERE Year BETWEEN 2003 AND 2010 GROUP BY Year",
    "YearlyLateAircraftDelay": "SELECT Year, AVG((LateAircraftDelay/ArrDelay)*100) AS avg_late_aircraft_delay FROM delay_flights WHERE Year BETWEEN 2003 AND 2010 GROUP BY Year",
    "YearlySecurityDelay": "SELECT Year, AVG((SecurityDelay/ArrDelay)*100) AS avg_security_delay FROM delay_flights WHERE Year BETWEEN 2003 AND 2010 GROUP BY Year"
}

result_string = ""

result_data = []

for query_name, query in queries.items():
    output_path = output_base_path +"/"+ query_name + ".csv"
    query_execution_time = run_and_save_query(spark, query,output_path)
    result_data.append((query_name, query_execution_time))


# Create a DataFrame from the list of Rows
result_df = spark.createDataFrame(result_data, ["query_name", "ExecutionTime"])

result_df = result_df.coalesce(1) 
# Specify the output CSV file path
csv_output_path = "s3://yasashadoopbucket/myOutputFolder/query_execution_result.csv"
#csv_output_path = "./myOutputFolder/query_execution_result.csv"

# Write the DataFrame to a CSV file in S3
result_df.write.csv(csv_output_path, header=True, mode="overwrite")

print(f"Result DataFrame written to CSV: {csv_output_path}")


