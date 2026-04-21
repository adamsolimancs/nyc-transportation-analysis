import pandas as pd
import matplotlib.pyplot as plt

'''
to run the this file for data visualization
pip install pandas matplotlib or use pip3
python visualize.py
'''

# taxi vs citibike trips by temperature
df = pd.read_csv('taxi_bike_temp.csv')
df = df.dropna(subset=['temp_bucket'])  # remove rows with missing temp

taxi = df[df['is_taxi'] == 1].sort_values('temp_bucket')
bike = df[df['is_taxi'] == 0].sort_values('temp_bucket')

plt.figure(figsize=(10, 6))
plt.scatter(taxi['temp_bucket'], taxi['count'], color='gold', label='Taxi', alpha=0.7, s=60)
plt.scatter(bike['temp_bucket'], bike['count'], color='dodgerblue', label='Citibike', alpha=0.7, s=60)
plt.xlabel('Temperature (°F)')
plt.ylabel('Number of Trips')
plt.title('Taxi vs Citibike Trips by Temperature (NYC 2019)')
plt.legend()
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig('taxi_vs_citibike_trips.jpg')
plt.show()

# avg trip duration by temperature
df2 = pd.read_csv('weather_duration.csv')
df2 = df2.dropna(subset=['temp_bucket'])

taxi2 = df2[df2['is_taxi'] == 1].sort_values('temp_bucket')
bike2 = df2[df2['is_taxi'] == 0].sort_values('temp_bucket')

plt.figure(figsize=(10, 6))
plt.plot(taxi2['temp_bucket'], taxi2['avg_duration'], color='gold', marker='o', label='Taxi', linewidth=2)
plt.plot(bike2['temp_bucket'], bike2['avg_duration'], color='dodgerblue', marker='o', label='Citibike', linewidth=2)
plt.xlabel('Temperature (°F)')
plt.ylabel('Avg Trip Duration (seconds)')
plt.title('Weather Impact on Trip Duration (NYC 2019)')
plt.legend()
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig('weather_trip_duration.jpg')
plt.show()