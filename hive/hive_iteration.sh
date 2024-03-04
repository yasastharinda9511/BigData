#!/bin/bash

# Number of iterations
num_iterations=5

# Hive queries
queries=("SELECT Year, avg((CarrierDelay/ArrDelay) * 100) AS avg_carrier_delay_percentage FROM delay_flights GROUP BY Year;"
        "SELECT Year, avg((NASDelay/ArrDelay) * 100) AS avg_carrier_delay_percentage FROM delay_flights GROUP BY Year;" 
        "SELECT Year, avg((WeatherDelay/ArrDelay) * 100) AS avg_carrier_delay_percentage FROM delay_flights GROUP BY Year;" 
        "SELECT Year, avg((LateAircraftDelay/ArrDelay) * 100) AS avg_carrier_delay_percentage FROM delay_flights GROUP BY Year;" 
        "SELECT Year, avg((SecurityDelay/ArrDelay) * 100) AS avg_carrier_delay_percentage FROM delay_flights GROUP BY Year;" 
         # Add more queries as needed
        )

start_time=$(date +%s.%N)
# Enter the Hive shell
hive <<EOF

${queries[0]}
${queries[1]}
${queries[2]}
${queries[3]}
${queries[4]}

EOF

end_time=$(date +%s.%N)

# Calculate and print the execution time
execution_time=$(echo "$end_time - $start_time" | bc)
echo "Script execution time: $execution_time seconds"
