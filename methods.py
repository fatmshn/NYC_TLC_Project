import argparse
import os

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
from pyspark.sql.types import IntegerType

def parse_arguments():
    """ parse_arguments function to create arguments help to use in main function"""
    parser = argparse.ArgumentParser(
        description=' ')

    parser.add_argument('--date',
                        type=str,
                        required=True,
                        help='date')
    return parser.parse_args()

def write_to_db(mode: str, df ,table_name : str, username : str, password : str, database : str, host: str, port: str):

    url = "jdbc:postgresql://" + host + ":" + port + "/" + database
    properties = {"user": username, "password": password, "driver": "org.postgresql.Driver"}
    df.write.jdbc(url=url, table=table_name, mode=mode, properties=properties)


def read_data_to_df(spark,header : bool, delimiter : str, data: str, datatype : str):

    if datatype == "csv":
        df = spark.read.options(delimiter=delimiter, header=header).csv(data)
    elif datatype == "parquet" :
        df = spark.read.options(header=header).parquet(data)

    return df

def get_spark_session():

    sparkClassPath = os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.postgresql:postgresql:42.2.1 pyspark-shell'
    sc = SparkContext('local')

    spark = SparkSession(sc) \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.driver.extraClassPath", sparkClassPath) \
        .getOrCreate()

    return spark

def generate_shape_file(spark):

    df = read_data_to_df(spark, True, ",", "/Users/fatmik/PycharmProjects/NYC_Taxi_Project/data/taxi_zones.csv", "csv")
    df = df.withColumnRenamed("OBJECTID,N,9,0", "OBJECTID") \
        .withColumnRenamed("Shape_Leng,N,19,11", "Shape_Leng") \
        .withColumnRenamed("Shape_Area,N,19,11", "Shape_Area") \
        .withColumnRenamed("zone,C,254", "Zone") \
        .withColumnRenamed("LocationID,N,4,0", "LocationID") \
        .withColumnRenamed("borough,C,254", "Borough")

    return df

def generate_fhv_df(spark,date: str):
    """ Document says: 'Indicates if the trip was a part of a shared ride chain offered by a High Volume FHV company
    (e.g. Uber Pool, Lyft Line).
    For shared trips, the value is 1. For non-shared rides, this field is null.'

    The SR_Flag column only takes the value 'null' and '1'. If the SR_Flag column contains a value of 1, shared travel is performed.
    I thought that if the SR_Flag field is 1, there are at least 2 passengers, if it is null, there is at least 1 passenger.
    And in this way, I edited the SR_Flag column and created the passenger_count column."""

    df = read_data_to_df(spark,True,"", "/Users/fatmik/PycharmProjects/NYC_Taxi_Project/data/fhv_tripdata_" + date + ".parquet", "parquet")
    df = df.where(col("PUlocationID").isNotNull())
    df = df \
        .select(col("*"),when(df.SR_Flag == 1, 2).when(df.SR_Flag.isNull(), 1).otherwise(df.SR_Flag).alias("passenger_count")) \
        .drop(col("dispatching_base_num")).drop(col("SR_Flag")).drop(col("Affiliated_base_number")) \
        .withColumn("PUlocationID", df.PUlocationID.cast(IntegerType())).withColumnRenamed("PUlocationID","PULocationID") \
        .withColumn("DOlocationID", df.DOlocationID.cast(IntegerType())).withColumnRenamed("DOlocationID","DOLocationID") \
        .withColumn("passenger_count", col("passenger_count").cast(IntegerType()))

    return df

def generate_fhvhv_df(spark,date: str):
    """ Document says: 'Indicates if the trip was a part of a shared ride chain offered by a High Volume FHV company
        (e.g. Uber Pool, Lyft Line).
        For shared trips, the value is 1. For non-shared rides, this field is null.'

        The shared_match_flag column only takes the value 'Y' and 'N'. If the shared_match_flag column contains a value of Y, shared travel is performed.
        I thought that if the shared_match_flag field is Y, there are at least 2 passengers, if it is N, there is at least 1 passenger.
        And in this way, I edited the shared_match_flag column and created the passenger_count column."""

    df = read_data_to_df(spark, True, "", "/Users/fatmik/PycharmProjects/NYC_Taxi_Project/data/fhvhv_tripdata_" + date + ".parquet", "parquet")
    df = df.where(col("PULocationID").isNotNull())
    df = df.select(col("pickup_datetime"), col("dropoff_datetime"), col("PULocationID"), col("DOLocationID"),
        when(df.shared_match_flag == "Y", "2").when(df.shared_match_flag == "N", "1") \
        .otherwise(df.shared_match_flag).alias("passenger_count")) \
        .withColumn("PULocationID", df.PULocationID.cast(IntegerType())) \
        .withColumn("DOLocationID", df.DOLocationID.cast(IntegerType())) \
        .withColumn("passenger_count", col("passenger_count").cast(IntegerType()))

    return df

def generate_green_df(spark, date:str):
    df = read_data_to_df(spark, True, "", "/Users/fatmik/PycharmProjects/NYC_Taxi_Project/data/green_tripdata_" + date + ".parquet", "parquet")
    df = df.where(col("PULocationID").isNotNull())
    df = df.select(col("lpep_pickup_datetime"), col("lpep_dropoff_datetime"), col("PULocationID"),col("DOLocationID"), col("passenger_count")) \
        .withColumn("PULocationID", df.PULocationID.cast(IntegerType())) \
        .withColumn("DOLocationID", df.DOLocationID.cast(IntegerType())) \
        .withColumn("passenger_count", col("passenger_count").cast(IntegerType())) \
        .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime") \
        .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")

    return df

def generate_yellow_df(spark, date:str):
    df = read_data_to_df(spark, True, "", "/Users/fatmik/PycharmProjects/NYC_Taxi_Project/data/yellow_tripdata_" + date + ".parquet", "parquet")
    df = df.where(col("PULocationID").isNotNull())
    df = df.select(col("tpep_pickup_datetime"), col("tpep_dropoff_datetime"), col("PULocationID"),col("DOLocationID"), col("passenger_count")) \
        .withColumn("PULocationID", df.PULocationID.cast(IntegerType())) \
        .withColumn("DOLocationID", df.DOLocationID.cast(IntegerType())) \
        .withColumn("passenger_count", df.passenger_count.cast(IntegerType())) \
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
        .withColumnRenamed("tpep_pickup_datetime", "dropoff_datetime")

    return df