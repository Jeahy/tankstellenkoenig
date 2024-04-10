## Fragen

Unter https://creativecommons.tankerkoenig.de/ kannst du sämtliche Preise der deutschen Tankstellen abfragen.
Diese Daten stehen öffentlich zur Verfügung.

1. Welches ist die südlichste Tankstelle Deutschlands?
2. Wie hoch war 2022 der höchste Preis für E10?
3. Wo gab es vorgestern den günstigsten Diesel?
4. Überlege Dir welche Analysen man mit den Daten noch alles machen könnte? Nenne mindestens zwei Möglichkeiten

## Vorüberlegungen
### Extrahieren
Es gib drei Möglichkeiten über https://creativecommons.tankerkoenig.de/ an die benötigten Daten zu kommen:
-    API-Abfragen:
     - Umkreissuche
     - Preisabfrage für einzelne Tankstellen
     - Detailabfrage für einzelne Tankstellen
     - P: keine historischen Daten für Frage 2 und 3
     - P: für Frage 1 müsste man so lange API-Abfragen machen, bis man die südlichste Tankstelle bzw. gefunden hat. Laut Webseite soll man aber maximal alle 5min eine API-Abfrage machen und unnötige Belastung vermeiden.
- Postgres Dump für aktuelle und historische Daten
     - P: ziemlich viele Daten, die ich nicht brauche, weil Pakete mehrere Jahre enthalten (z.B.2020-2023)
     - P: Link hat leider gestern nicht funktioniert
- CSV Daten auf Azure Cloud

### Transformation
Pandas oder PySpark?
- Pandas: schnelles Setup
- PySpark: schnellere Verarbeitung

### Laden
Das Hochladen in eine PostgreSQL-Datenbank wäre sinnvoll, wenn weitere Analysen der Daten gewollt wären

## Setup
Ich habe ein Github Repository erstellt und es auf meinem Rechner dupliziert
```
git clone https://github.com/Jeahy/tankstellenkoenig.git
```
eine Virtuelle Umgebung für Python erstellt und sie aktiviert
```
virtualenv tkvenv
source tkvenv/bin/activate
```
Pandas und PySpark installiert
```
pip install pandas
pip install pyspark
```
die benötigten CSVs manuell von der Azure Cloud heruntergeladen:
- 2024-04-07-prices.csv   
- 2024-04-07-stations.csv    
- alle CSVs für 2022

## Frage 1: Welches ist die südlichste Tankstelle Deutschlands?
Mein Python-Skript
```
import pandas as pd

# Daten aus CSV-Datei lesen
data_file = '/Users/jessica/dev/projects/tankstellenkoenig/data/2024-04-07-stations.csv'
df = pd.read_csv(data_file)

# Zeilen mit fehlenden Breiten- oder Längengradwerten und Breitengraden, die gleich 0.0 sind, herausfiltern
df = df.dropna(subset=['latitude', 'longitude'])
df = df[df['latitude'] != 0.0]

# Zeile mit dem niedrigsten Breitengradwert finden
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
Sinnvoll wären Plausibilitätsprüfungen anhand von höchstem Wert und Durchschnittswert.

## Frage 2: Wie hoch war 2022 der höchste Preis für E10?  
### Python Skript mit Pandas
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

# Den höchsten E10-Wert und die zugehörige Zeile im DataFrame finden
max_e10_row = combined_df.loc[combined_df['e10'].idxmax()]

# Den Preis, Tag und die Uhrzeit des höchsten E10-Werts extrahieren
highest_e10_price = max_e10_row['e10']
highest_e10_date = max_e10_row['date']

print(f"Der höchste E10-Wert war {highest_e10_price} am {highest_e10_date}.")
```
Ergebnis nach ein paar Minuten: 
```
Der höchste E10-Wert war 4.999 am 2022-03-15 07:41:06+01.
```
### Python Skript mit PySpark
```
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
import os

# Initialisiere die Spark-Sitzung
spark = SparkSession.builder \
    .appName("CSV mit PySpark lesen") \
    .getOrCreate()

# Definiere das Schema für die CSV-Dateien
schema = StructType([
    StructField("date", TimestampType(), True),
    StructField("station_uuid", StringType(), True),
    StructField("diesel", FloatType(), True),
    StructField("e5", FloatType(), True),
    StructField("e10", FloatType(), True),
    StructField("dieselchange", StringType(), True),
    StructField("e5change", StringType(), True),
    StructField("e10change", StringType(), True)
])

# Pfad zum Verzeichnis mit den CSV-Dateien
base_dir = '/Users/jessica/dev/projects/tankstellenkoenig/data/2022'

# Liste zum Speichern der DataFrame-Objekte für jede CSV-Datei
dfs = []

# Iteriere durch Unterverzeichnisse und Dateien
for subdir, _, files in os.walk(base_dir):
    for file in files:
        # Überprüfe, ob die Datei eine CSV-Datei ist
        if file.endswith('.csv'):
            # Erstelle den Pfad zur CSV-Datei
            file_path = os.path.join(subdir, file)
            # Lese die CSV-Datei und erstelle DataFrame
            df = spark.read.option("header", "true").schema(schema).csv(file_path)
            # Füge den DataFrame zur Liste hinzu
            dfs.append(df)

# Kombiniere alle DataFrames in der Liste zu einem einzigen DataFrame
combined_df = dfs[0]
for df in dfs[1:]:
    combined_df = combined_df.union(df)

# Finde die Zeile mit dem höchsten E10-Wert
max_e10_row = combined_df.orderBy(combined_df['e10'].desc()).first()

# Extrahiere den Preis, das Datum und die Uhrzeit des höchsten E10-Werts
highest_e10_price = max_e10_row['e10']
highest_e10_date = max_e10_row['date']

print(f"Der höchste E10-Preis war {highest_e10_price} am {highest_e10_date}.")
````
Ergebnis hier:
```
Der höchste E10-Preis war 4.999000072479248 am 2022-03-15 12:06:05.
```
Die Verarbeitung mit PySpark war allerdings viel langsamer als mit Pandas, da ich Spark nur im lokalen Mudus mit einer Master und einer Worker Node ausgeführt habe, ohne von der verteilten Verarbeitung zu profitieren.    

Sinnvoll wären Plausibilitätsprüfungen anhand von niedrigstem Wert und Durchschnittswert.
## Frage 3: Wo gab es vorgestern den günstigsten Diesel?

Mein Python Skript: 
```
import pandas as pd

# Daten aus den CSV-Dateien lesen
stations = pd.read_csv('/Users/jessica/dev/projects/tankstellenkoenig/data/2024-04-07-stations.csv')
prices = pd.read_csv('/Users/jessica/dev/projects/tankstellenkoenig/data/2024-04-07-prices.csv')

# Filtere die Zeilen mit fehlenden Werten in der 'diesel' Spalte oder mit 0.0 als Wert
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
Sinnvoll wären Plausibilitätsprüfungen anhand von höchstem Wert und Durchschnittswert.

## Frage 4: Überlege Dir welche Analysen man mit den Daten noch alles machen könnte? Nenne mindestens zwei Möglichkeiten
- Vorhersage Benzinpreise für 2024 -  lineare Regression
- Welche Wochentage und Uhrzeiten sind gut zum Tanken - Pandas, Histogram mit Matplotlib, 
- Niedrigste, höchste und durchschnittliche Preise im Nordwesten, Nordosten, Südosten und Südwesten Deutschlands

