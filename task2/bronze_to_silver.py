from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower
from pyspark.sql.types import StringType
import os

# Create SparkSession
spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

# clean text data
def clean_text(df):
    for column in df.columns:
        if df.schema[column].dataType == StringType():
            df = df.withColumn(column, trim(lower(col(column))))
    return df

for data in ["athlete_bio", "athlete_event_results"]:
    df = spark.read.parquet(f"./kari/temporary/bronze/{data}")

    df = clean_text(df)
    df = df.dropDuplicates()
   
    output_path = f"./kari/temporary/silver/{data}"
    os.makedirs(output_path, exist_ok=True)
    df.write.mode("overwrite").parquet(output_path)

    
    print(f"Data saved")
    df = spark.read.parquet(output_path)
    df.show(truncate=False)

# Close Spark
spark.stop()