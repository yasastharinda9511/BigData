import matplotlib.pyplot as plt
import numpy as np

queries = np.array(["YearlyCarrierDelay", "YearlyNASDelay", "YearlyWeatherDelay", "YearlyLateAircraftDelay", "YearlySecurityDelay"])
hive_delays = np.array([32.444, 35.0, 31.601, 27.225, 30.656])
spark_delays = np.array([0.269, 0.0788, 0.053, 0.045, 0.045])

# Set up the figure and axis
fig, ax = plt.subplots()

# Bar width
bar_width = 0.35
bar_positions_hive = np.arange(len(queries))
bar_positions_spark = bar_positions_hive + bar_width

# Create bar charts for hive and spark delays
ax.bar(bar_positions_hive, hive_delays, width=bar_width, label='Hive Delays')
ax.bar(bar_positions_spark, spark_delays, width=bar_width, label='Spark Delays')

# Set axis labels and title
ax.set_xticks(bar_positions_hive + bar_width / 2)
ax.set_xticklabels(queries, rotation=45, ha='right')
ax.set_xlabel('Queries')
ax.set_ylabel('Delays')
ax.set_title('Comparison of Delays between Hive and Spark')

# Add legend
ax.legend()

# Show the plot
plt.tight_layout()
plt.show()