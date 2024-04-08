import os
import pandas as pd

# Verzeichnis, in dem sich die CSV-Dateien befinden
base_dir = '/Users/jessica/dev/projects/tankstellenkoenig/data/2022'

# Liste zum Speichern der DataFrame-Objekte für jede CSV-Datei
dfs = []

# Durchlaufen der Unterordner und Dateien
for subdir, _, files in os.walk(base_dir):
    for file in files:
        # Überprüfen, ob die Datei eine CSV-Datei ist
        if file.endswith('.csv'):
            # Pfad zur CSV-Datei erstellen
            file_path = os.path.join(subdir, file)
            # CSV-Datei einlesen und DataFrame erstellen
            df = pd.read_csv(file_path)
            # DataFrame zur Liste hinzufügen
            dfs.append(df)

# Alle DataFrames in der Liste zu einem einzigen DataFrame zusammenführen
combined_df = pd.concat(dfs, ignore_index=True)

# Den höchsten Dieselwert und die zugehörige Zeile im DataFrame finden
max_diesel_row = combined_df.loc[combined_df['diesel'].idxmax()]

# Den Preis, Tag und die Uhrzeit des höchsten Dieselwerts extrahieren
highest_diesel_price = max_diesel_row['diesel']
highest_diesel_date = max_diesel_row['date']

print(f"Der höchste Dieselwert war {highest_diesel_price} am {highest_diesel_date}.")