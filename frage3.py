import pandas as pd

# Daten aus den CSV-Dateien lesen
stations = pd.read_csv('/Users/jessica/dev/projects/tankstellenkoenig/data/2024-04-07-stations.csv')
prices = pd.read_csv('/Users/jessica/dev/projects/tankstellenkoenig/data/2024-04-07-prices.csv')

# Filter out rows with missing latitude or longitude values and latitude equal to 0.0
prices = prices.dropna(subset=['diesel'])
prices = prices[prices['diesel'] != 0.0]

# Filtere die Zeilen im DataFrame `prices`, um nur die Zeilen mit dem günstigsten Dieselpreis zu behalten
min_diesel_price = prices[prices['diesel'] == prices['diesel'].min()]

# Füge die Informationen aus dem DataFrame `stations` hinzu
result = min_diesel_price.merge(stations, left_on='station_uuid', right_on='uuid', how='left')

# Wähle die relevanten Spalten aus
result = result[['name', 'city', 'diesel', 'date']]

# Drucke die Ergebnisse
print("Name der Tankstelle mit dem günstigsten Diesel, Ort, Preis und Uhrzeit:")
print(result)
