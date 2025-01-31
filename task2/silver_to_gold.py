from pyspark.sql import SparkSession 
from pyspark.sql.functions import avg, current_timestamp 
import os

# Create SparkSession
spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

# Read 'silver' data from "athlete_bio", "athlete_event_results"
athlete_bio_df = spark.read.parquet("./kari/temporary/silver/athlete_bio") 
athlete_event_results_df = spark.read.parquet("./kari/temporary/silver/athlete_event_results") 

# avoid columns with the same name
athlete_bio_df = athlete_bio_df.withColumnRenamed("country_noc", "bio_country_noc")

# join data
joined_df = athlete_event_results_df.join(athlete_bio_df, "athlete_id")

# Data aggregation
aggregated_df = joined_df.groupBy("sport", "medal", "sex", "country_noc").agg(
    avg("height").alias("avg_height"),  
    avg("weight").alias("avg_weight"),  
    current_timestamp().alias("timestamp"),  
)

# Save data as Parquet
output_path = "./kari/temporary/gold/average_statistic"
os.makedirs(output_path, exist_ok=True)

aggregated_df.write.mode("overwrite").parquet(output_path)

print(f"Data saved")

df = spark.read.parquet(output_path)
df.show(truncate=False)

# Close Spark
spark.stop()