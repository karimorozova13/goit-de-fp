from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),  
}

# Define the DAG
dag = DAG(
    "kari_data_lake_dag",  
    default_args=default_args,
    schedule_interval=None,  
    tags=["kari"],
)

# 1. landing_to_bronze.py
landing_to_bronze = SparkSubmitOperator(
    task_id="landing_to_bronze",  
    application="/opt/airflow/dags/kari/landing_to_bronze.py",  
    conn_id="spark-default", 
    verbose=1,  
    dag=dag,
)

# 2. bronze_to_silver.py
bronze_to_silver = SparkSubmitOperator(
    task_id="bronze_to_silver",
    application="/opt/airflow/dags/kari/bronze_to_silver.py", 
    conn_id="spark-default", 
    verbose=1,  
    dag=dag,
)

# 3. silver_to_gold.py
silver_to_gold = SparkSubmitOperator(
    task_id="silver_to_gold", 
    application="/opt/airflow/dags/kari/silver_to_gold.py",  
    conn_id="spark-default", 
    verbose=1,  
    dag=dag,
)

# Task execution order
landing_to_bronze >> bronze_to_silver >> silver_to_gold
