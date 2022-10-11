
from os.path import expanduser, join, abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

warehouse_location = abspath('spark-warehouse')

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("Data Processing") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# Read the pipeline csv
df = spark.read.option("header",True).csv('hdfs://namenode:9000/pipeline/*.csv')
df.printSchema()


weatherschema = StructType([
    StructField("coord", StructType([
        StructField("lon", DoubleType(), True),
        StructField("lat", DoubleType(), True)
    ])),
    StructField("weather",ArrayType(
        StructType([
            StructField("main", StringType(), True),
            StructField("description", StringType(), True),
    ]))),
    StructField("main", StructType([
        StructField("temp", DoubleType(), True),
        StructField("temp_min", DoubleType(), True),
        StructField("temp_max", DoubleType(), True),
        StructField("pressure", DoubleType(), True),
        StructField("humidity", DoubleType(), True)
    ])),
    StructField("visibility", DoubleType(), True),
    StructField("wind", StructType([
        StructField("speed", DoubleType(), True),
        StructField("deg", DoubleType(), True)
    ])),
    StructField("sys", StructType([
        StructField("sunrise", IntegerType(), True),
        StructField("sunset", IntegerType(), True)
    ])),
    


])
pollutionschema = StructType([
    StructField("list",ArrayType(
        StructType([
            StructField("main", StructType([
                StructField("aqi", DoubleType(), True)
            ]), True),
            StructField("components", StructType([
                StructField("co", DoubleType(), True),
                StructField("no", DoubleType(), True),
                StructField("no2", DoubleType(), True),
                StructField("o3", DoubleType(), True),
                StructField("so2", DoubleType(), True),
                StructField("pm2_5", DoubleType(), True),
                StructField("pm10", DoubleType(), True),
                StructField("nh3", DoubleType(), True),
            ]), True),
    ]))),
])


df2 = df.select(col("_id").alias("id"),"cityname", "batchid", col("date").cast("timestamp"),from_json(col("weatherdata"), schema=weatherschema).alias("data"), from_json(col("polutiondata"), schema=pollutionschema).alias("data1")).select("id","cityname", "batchid", "date", "data.*", "data1.*")
df3 = df2.select(df2.id, df2.cityname, df2.batchid, df2.date, df2.coord.lon, df2.coord.lat, df2.weather.main[0], df2.weather.description[0], df2.main.temp, df2.main.temp_min,df2.main.temp_max,df2.main.pressure, df2.main.humidity, df2.visibility, df2.wind.speed, df2.wind.deg, df2.sys.sunrise, df2.sys.sunset, df2.list.main.aqi[0], df2.list.components.co[0], df2.list.components.no[0], df2.list.components.no2[0], df2.list.components.o3[0], df2.list.components.so2[0], df2.list.components.pm2_5[0], df2.list.components.pm10[0], df2.list.components.nh3[0])
df3.write.mode("append").insertInto("weather_data")
