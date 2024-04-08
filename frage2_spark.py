from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Read CSV with PySpark") \
    .getOrCreate()

# Define the schema for the CSV files
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

# Path to the directory containing the CSV files
base_dir = '/Users/jessica/dev/projects/tankstellenkoenig/data/2022'

# List to store DataFrame objects for each CSV file
dfs = []

# Iterate through subdirectories and files
for subdir, _, files in os.walk(base_dir):
    for file in files:
        # Check if the file is a CSV file
        if file.endswith('.csv'):
            # Create path to the CSV file
            file_path = os.path.join(subdir, file)
            # Read the CSV file and create DataFrame
            df = spark.read.option("header", "true").schema(schema).csv(file_path)
            # Append DataFrame to the list
            dfs.append(df)

# Combine all DataFrames in the list into a single DataFrame
combined_df = dfs[0]
for df in dfs[1:]:
    combined_df = combined_df.union(df)

# Find the row with the highest diesel value
max_diesel_row = combined_df.orderBy(combined_df['diesel'].desc()).first()

# Extract the price, date, and time of the highest diesel value
highest_diesel_price = max_diesel_row['diesel']
highest_diesel_date = max_diesel_row['date']

print(f"Der höchste Dieselwert war {highest_diesel_price} am {highest_diesel_date}.")
