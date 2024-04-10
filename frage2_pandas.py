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

# Den höchsten E10-Wert und die zugehörige Zeile im DataFrame finden
max_e10_row = combined_df.loc[combined_df['e10'].idxmax()]

# Den Preis, Tag und die Uhrzeit des höchsten E10-Werts extrahieren
highest_e10_price = max_e10_row['e10']
highest_e10_date = max_e10_row['date']

print(f"Der höchste E10-Wert war {highest_e10_price} am {highest_e10_date}.")