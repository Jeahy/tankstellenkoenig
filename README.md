# Big-Data-Traineeprogramm Challenge

## Fragen

Unter https://creativecommons.tankerkoenig.de/ kannst du sämtliche Preise der deutschen Tankstellen abfragen.
Diese Daten stehen öffentlich zur Verfügung.

1. Welches ist die südlichste Tankstelle Deutschlands?
2. Wie hoch war 2022 der höchste Preis für E10?
3. Wo gab es vorgestern den günstigsten Diesel?
4. Überlege Dir welche Analysen man mit den Daten noch alles machen könnte? Nenne mindestens zwei Möglichkeiten

## Vorüberlegungen

Es gib drei Möglichkeiten um über https://creativecommons.tankerkoenig.de/ an die benötigten Daten zu kommen:
-    API-Abfragen:
     - Umkreissuche
     - Preisabfrage für einzelne Tankstellen
     - Detailabfrage für einzelne Tankstellen
     - P: keine historischen Daten für Frage 2
     - P: für Frage 1 und 3 müsste man so lange API-Abfragen machen, bis man die südlichste Tankstelle bzw den günstigsten Diesel gefunden hat. Laut Webseite soll man aber maximal alle 5min eine API-Abfrage machen und unnötige Belastung vermeiden.
- Postgres Dump für aktuelle und historische Daten
     - P: ziemlich viele Daten, die ich nicht brauche, weil Pakete mehrere Jahre enthalten (z.B.2020-2023)
     - P: Link hat leider gestern nicht funktioniert
- CSV Daten auf Azure Plattform

Pandas, PySpark oder hochladen in PostgreSQL-Datenbank?
- Pandas: ausreichend für die Datenmenge, aber etwas langsam
- Pyspark: schneller
- PostgreSQL: gut, falls weitere Analysen der Daten gewollt

## Setup
Ich habe ein Github Repository erstellt und es auf meinem Rechner dupliziert
```
git clone https://github.com/Jeahy/tankstellenkoenig.git
```
habe eine Virtuelle Umgebung für Python erstellt und sie aktiviert
```
virtualenv tkvenv
source tkvenv/bin/activate
```
die Pandas bibliothek installiert
```
pip install pandas
```
Manuelles Herunterladen der CSVs:
- 2024-04-07-prices.csv   
- 2024-04-07-stations.csv    
- alle CSVs für 2022

## Frage 1: Welches ist die südlichste Tankstelle Deutschlands?

```
import pandas as pd

# Daten aus CSV-Datei lesen
data_file = '/Users/jessica/dev/projects/tankstellenkoenig/data/2024-04-07-stations.csv'
df = pd.read_csv(data_file)

# Zeilen mit fehlenden Breiten- oder Längengradwerten und Breitengraden, die gleich 0.0 sind, herausfiltern
df = df.dropna(subset=['latitude', 'longitude'])
df = df[df['latitude'] != 0.0]

# Zeile mit dem minimalen Breitengradwert finden
most_southern_station = df[df['latitude'] == df['latitude'].min()]

# Name, Stadt und Breitengrad der südlichsten Tankstelle ausgeben
print("Südlichste Tankstelle:")
print(most_southern_station[['name', 'city', 'latitude']])

```

Zunächst bekam ich dieses Ergebnis:
```
Südlichste Tankstelle:
                                           name                            city  latitude
7900             please delete - bitte loeschen  please delete - bitte loeschen       0.0
13809                                  Eurotank                         Schwelm       0.0
14191                                   01_test                             NaN       0.0
15031          Raiffeisen Hohe Mark Hamaland eG                           Velen       0.0
15334                   ZIEGLMEIER GmbH & Co.KG                      Ingolstadt       0.0
15937                   Autohaus Rickmeier GmbH                      Lauenförde       0.0
16066                         Hh Admi-Testkasse               Hh Admi-Testkasse       0.0
16113                 Autohaus Franz Saaler OHG                    Herrischried       0.0
16236                      Tankstelle Zajelsnik                        Freiburg       0.0
16237                         Tankhof Kenzingen                       Kenzingen       0.0
16798  Schrepfer Mineralöle und Brennstoffe Gmb                     Lichtenfels       0.0
16799  Schrepfer Mineralöle und Brennstoffe Gmb                       Ebensfeld       0.0
17197                           Aral Tankstelle                      Grafenberg       0.0
17198                           Aral Tankstelle                    Schutterwald       0.0
17206                           Aral Tankstelle                         Könnern       0.0
17237                           Aral Tankstelle                        Dortmund       0.0
17240                           Aral Tankstelle                    Bad Bentheim       0.0
17241                           Aral Tankstelle                       Isselburg       0.0
17251                           Aral Tankstelle                           Melle       0.0
17252                           Aral Tankstelle                           Melle       0.0
17254                           Aral Tankstelle                     Bremerhaven       0.0
17255                           Aral Tankstelle                       Egelsbach       0.0
17256                           Aral Tankstelle                       Lohfelden       0.0
17257                           Aral Tankstelle                   Bad Krozingen       0.0
17258                           Aral Tankstelle                           Essen       0.0
17259                           Aral Tankstelle                   Heiligenstadt       0.0
```

Dann habe ich mein Skript um diese Zeile ergänzt:
```
df = df[df['latitude'] != 0.0]
```
Ergebnis:
```
Südlichste Tankstelle:
                                   name        city  latitude
14758  Shell Mittenwald Am Brunnstein 2  Mittenwald  47.39957
```
Plausibilitätsprüfung anhand von höchstem Wert und Durchschnittswert.
## Frage 2: Wie hoch war 2022 der höchste Preis für E10?  


Mein Python Skript:
```
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
```
Ergebnis nach ein paar Minuten: 
```
Der höchste Dieselwert war 4.999 am 2022-05-31 13:41:07+02.
```

## Frage 3: Wo gab es vorgestern den günstigsten Diesel?

Mein Python Skript: 
```
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
```
und hier das Ergebnis:
```
Name der Tankstelle mit dem günstigsten Diesel, Ort, Preis und Uhrzeit:
                       name            city  diesel                    date
0  Greenline Wutha-Farnroda  Wutha-Farnroda   1.588  2024-04-07 21:23:53+02
1  Greenline Wutha-Farnroda  Wutha-Farnroda   1.588  2024-04-07 21:41:16+02
2  Greenline Wutha-Farnroda  Wutha-Farnroda   1.588  2024-04-07 21:57:35+02
3  Greenline Wutha-Farnroda  Wutha-Farnroda   1.588  2024-04-07 22:13:54+02
```
Plausibilitätsprüfung anhand von höchstem Wert und Durchschnittswert.

## Überlege Dir welche Analysen man mit den Daten noch alles machen könnte? Nenne mindestens zwei Möglichkeiten
Vorhersage Benzinpreise für 2024
Verhältnis Lage (Norden, Süden, Osten, Westen) und Preise
Verhältnis Öffnungszeiten und Preise

