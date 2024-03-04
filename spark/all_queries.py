
from pyspark.sql import SparkSession
from pyspark.sql import Row
import time

def run_and_save_query(spark, query, output_path):
    # Run the SQL query
    result = spark.sql(query)
    
    # Save the result as a CSV file on Amazon S3
    result.write.mode("overwrite").csv(output_path, header=True)


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
    "YearlyNASDelay": "SELECT Year, AVG(NASDelay) AS avg_nas_delay FROM delay_flights WHERE Year BETWEEN 2003 AND 2010 GROUP BY Year",
    "YearlyWeatherDelay": "SELECT Year, AVG(WeatherDelay) AS avg_weather_delay FROM delay_flights WHERE Year BETWEEN 2003 AND 2010 GROUP BY Year",
    "YearlyLateAircraftDelay": "SELECT Year, AVG(LateAircraftDelay) AS avg_late_aircraft_delay FROM delay_flights WHERE Year BETWEEN 2003 AND 2010 GROUP BY Year",
    "YearlySecurityDelay": "SELECT Year, AVG(SecurityDelay) AS avg_security_delay FROM delay_flights WHERE Year BETWEEN 2003 AND 2010 GROUP BY Year"
}

iterations = [1, 1, 1, 1, 1]
iterations.reverse()

output_file_path = 's3://yasashadoopbucket/myOutputFolder/spark_iteration_results.txt'  # Replace with your desired file path
#output_file_path = './spark_iteration_results.txt' 

result_string = ""

result_data = []

while(len(iterations) !=0) :

    iteration_count = iterations.pop()
    start_time = time.time()
    for _ in range(iteration_count):
        for query_name, query in queries.items():
            output_path = output_base_path +"/"+ query_name + ".csv"
            run_and_save_query(spark, query, output_path)
    end_time = time.time()
    result_string +=f"{iteration_count}: {end_time - start_time:.2f}\n"
    result_data.append((iteration_count, end_time - start_time ))
    print(result_string)


# Create a DataFrame from the list of Rows
result_df = spark.createDataFrame(result_data, ["Iteration", "ExecutionTime"])

result_df = result_df.coalesce(1) 
# Specify the output CSV file path
csv_output_path = "s3://yasashadoopbucket/myOutputFolder/result.csv"

# Write the DataFrame to a CSV file in S3
result_df.write.csv(csv_output_path, header=True, mode="overwrite")

print(f"Result DataFrame written to CSV: {csv_output_path}")


