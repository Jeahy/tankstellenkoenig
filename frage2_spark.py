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
