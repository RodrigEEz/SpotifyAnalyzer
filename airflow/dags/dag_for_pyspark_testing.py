from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import datetime

dag_spark = DAG(
   dag_id="spark_operator",
   schedule_interval='@once',
   start_date=datetime.datetime.today()
   )

with dag_spark:
   test_task = SparkSubmitOperator(
      task_id='testing',
      conn_id='spark_connection',
      application='/opt/pyspark_scripts/pyspark_testing.py'
   )

   test_task