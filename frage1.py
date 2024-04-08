import pandas as pd

# Read data from CSV file
data_file = '/Users/jessica/dev/projects/tankstellenkoenig/data/2024-04-07-stations.csv'
df = pd.read_csv(data_file)

# Filter out rows with missing latitude or longitude values and latitude equal to 0.0
df = df.dropna(subset=['latitude', 'longitude'])
df = df[df['latitude'] != 0.0]

# Find the row with the minimum latitude value
most_southern_station = df[df['latitude'] == df['latitude'].min()]

# Print the name, city, and latitude of the most southern petrol station
print("Most southern petrol station:")
print(most_southern_station[['name', 'city', 'latitude']])
