from pyspark.sql import SparkSession
import os
import requests

# load data from FTP
def download_data(file):
  
    BASE_URL = "https://ftp.goit.study/neoversity/"
    url = BASE_URL + file + ".csv" 
    
    response = requests.get(url)

    if response.status_code == 200:
        with open(file + ".csv", "wb") as file:
            file.write(response.content)
        print(f"File downloaded successfully")
    else:
        exit(f"Failed to download the file. Status code: {response.status_code}")

# create SparkSession
spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()

for data in ["athlete_bio", "athlete_event_results"]:
    local_path = f"{data}.csv"
    download_data(data)

    df = spark.read.csv(local_path, header=True, inferSchema=True)

    output_path = f"./kari/temporary/bronze/{data}"
    os.makedirs(output_path, exist_ok=True)
    df.write.mode("overwrite").parquet(output_path)

    print(f"Data saved")
    df = spark.read.parquet(output_path)
    df.show(truncate=False)

# stop Spark
spark.stop()