from turtle import down
from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.mongo.sensors.mongo import MongoHook
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
from pandas import DataFrame
import csv
import requests
import json


default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
   
}


def aggregationmongo(ds, **kwargs):
    execdate=kwargs['execution_date']
    conn = MongoHook(conn_id="local_mongo")
    coll = conn.get_collection("pollutiondata","citydata")
    pipeline = [
    {"$addFields": { "date": { "$toDate": { "$toLong": { "$multiply": ['$weatherdata.dt', 1000 ] } } } } },

    {"$match": {"date": {
        "$gte": execdate,
        "$lt": (execdate+timedelta(hours=24)),
        } }},
    {"$out": execdate.strftime("%d_%m_%y")}]
    aggregate = coll.aggregate(pipeline)

def faggregationexport(ds, **kwargs):
    execdate=kwargs['execution_date']
    collectionname = execdate.strftime("%d_%m_%y")
    print(collectionname)
    conn = MongoHook(conn_id="local_mongo")
    coll = conn.get_collection(collectionname,"citydata")
    data = coll.find({})
    list_data= list(data)
    df = DataFrame(list_data)
    df.to_csv(f"/opt/airflow/dags/filess/{collectionname}.csv")
    

with DAG("pipeline_project", start_date=datetime(2022, 9, 9), schedule_interval="@daily", default_args=default_args, catchup=True, max_active_runs=1) as dag:

    #Aggregate the Data within the MongoDB over 24 hours, by creating a new date field and output to new collection
    aggregationquery = PythonOperator(
    task_id="aggregationquery",
    python_callable=aggregationmongo,
    depends_on_past=True,
    wait_for_downstream=True
    )
    #Export the collection to a csv file
    aggregationexport= PythonOperator(
    task_id="aggregationexport",
    python_callable=faggregationexport,
    )
    # Transfer the file to Hadoop HDFS
    saving_export = BashOperator(
        task_id="saving_export",
        bash_command= """
            hdfs dfs -mkdir -p /pipeline && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/filess/{{execution_date.strftime('%d_%m_%y')}}.csv /pipeline
        """
    )
    #Delete the leftover Local File
    deleting_local = BashOperator(
        task_id="deleting_local",
        bash_command= """
        rm -f $AIRFLOW_HOME/dags/filess/*
        """
    )
    # Create a new Table within Hive, so that we can properly query a relational database
    creating_citydata_table = HiveOperator(
        task_id="creating_citydata_table",
        hive_cli_conn_id="hive_conn",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS weather_data(
                id STRING,
                city STRING,
                batchid INT,
                date_created timestamp,
                lon DOUBLE,
                lat DOUBLE,
                weather_main STRING,
                weather_description STRING,
                temp DOUBLE,
                temp_min DOUBLE,
                temp_max DOUBLE,
                pressure DOUBLE,
                humidity DOUBLE,
                visibility DOUBLE,
                wind_speed DOUBLE,
                wind_deg DOUBLE,
                sunrise DOUBLE,
                sunset DOUBLE,
                aqi DOUBLE,
                air_co DOUBLE,
                air_no DOUBLE,
                air_no2 DOUBLE,
                air_o3 DOUBLE,
                air_so2 DOUBLE,
                air_pm2_5 DOUBLE,
                air_pm10 DOUBLE,
                air_nh3 DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """
    )
    # Use spark to transform and insert the data into the newly created hive table
    weather_processing = SparkSubmitOperator(
        task_id="weather_processing",
        application="/opt/airflow/dags/scripts/weather_processing.py",
        conn_id="spark_conn",
        verbose=False
        )
    # Delete the leftover file within the HDFS file system
    deleting_export = BashOperator(
        task_id="deleting_export",
        bash_command= """
            hdfs dfs -rm -R /pipeline/*
        """
    )

aggregationquery >> aggregationexport >> saving_export >> deleting_local >> creating_citydata_table >> weather_processing >> deleting_export








    

    

    
