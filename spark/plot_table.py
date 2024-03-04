#hive

# YearlyCarrierDelay : 32.444
# YearlyNASDelay:35.0
# YearlyWeatherDelay: 31.601
# YearlyLateAircraftDelay:27.225
# YearlySecurityDelay:30.656



#Spark

# YearlyCarrierDelay,0.269057035446167
# YearlyNASDelay,0.07881426811218262
# YearlyWeatherDelay,0.05375838279724121
# YearlyLateAircraftDelay,0.04573678970336914
# YearlySecurityDelay,0.04559755325317383

import matplotlib.pyplot as plt
import numpy as np

queries = np.array(["YearlyCarrierDelay", "YearlyNASDelay", "YearlyWeatherDelay", "YearlyLateAircraftDelay", "YearlySecurityDelay"])
hive_delays = np.array([32.444, 35.0, 31.601, 27.225, 30.656])
spark_delays = np.array([0.269, 0.0788, 0.053, 0.045, 0.045])

# Create a figure and axis
fig, ax = plt.subplots()

# Create a table with data
table_data = np.vstack((queries, hive_delays, spark_delays)).T
table = plt.table(cellText=table_data, colLabels=["Queries", "Hive Delays", "Spark Delays"],
                  cellLoc='center', loc='center', colColours=['#f5f5f5']*3, cellColours=[['#f5f5f5']*3]*len(queries))

# Hide the axes
ax.axis('off')

# Adjust the table layout
table.auto_set_font_size(False)
table.set_fontsize(10)
table.scale(1.2, 1.2)

# Show the plot
plt.show()

