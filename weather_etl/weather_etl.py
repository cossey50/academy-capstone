import json
import os
from typing import Collection, Mapping, Union
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark import SparkConf
import pyspark.sql.functions as sf
from pyspark.sql.types import (
    BooleanType,
    TimestampType,
    IntegerType,
    StringType,
    DoubleType,
)


from boto3 import client

# Spark jars already present in the base image provided.
config = {
#     "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.2.0,net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.1,net.snowflake:snowflake-jdbc:3.13.3",
    "fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"

}

S3_BUCKET_OPENAQ = os.environ.get("S3_BUCKET_OPENAQ")
SECRET_ARN = os.environ.get("SECRET_ARN")
SNOWFLAKE_SCHEMA = os.environ.get("SNOWFLAKE_SCHEMA")
SNOWFLAKE_SOURCE_NAME = os.environ.get("SNOWFLAKE_SOURCE_NAME")


def read_data(path):
    conf = SparkConf().setAll(config.items())
    spark = SparkSession.builder.config(conf= conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    return spark.read.json(
        path
    )

def clean_data(frame: DataFrame) -> DataFrame:
    for transformation in (
        unnest_columns,
        drop_redundant_columns,
        correct_datatypes,
        improve_columns_names,
    ):
        frame = transformation(frame)
    return frame
                        

def to_snowflake(frame: DataFrame) -> None:

    aws_client = client("secretsmanager", region_name="eu-west-1")
    snowflake_secret = aws_client.get_secret_value(SecretId=SECRET_ARN)
    snowflake_settings = json.loads(snowflake_secret["SecretString"])
    
    sfOptions = {
        "sfURL": snowflake_settings.get("URL"),
        "sfUser": snowflake_settings.get("USER_NAME"),
        "sfPassword": snowflake_settings.get("PASSWORD"),
        "sfDatabase": snowflake_settings.get("DATABASE"),
        "sfSchema": "WILLEM",
        "sfWarehouse": snowflake_settings.get("WAREHOUSE"),
        "sfRole": snowflake_settings.get("ROLE"),
    }
    
    frame.write.format("net.snowflake.spark.snowflake") \
        .options(**sfOptions) \
        .option("dbtable", "openaq") \
        .mode("overwrite") \
        .save()


def drop_redundant_columns(frame: DataFrame) -> DataFrame:
    to_drop = {
        "date_local",
    }
    return frame.drop(*to_drop)


def unnest_columns(frame: DataFrame) -> DataFrame:
    nested_columns = [
        "coordinates",
        "date",
    ]

    # Select all existing columns except the ones to be unnested
    selected_columns = [sf.col(column) for column in frame.columns if column not in nested_columns]
    
    # Select and alias the unnested columns
    unnested_columns = [sf.col("coordinates.latitude").alias("latitude"),
                        sf.col("coordinates.longitude").alias("longitude"),
                        sf.col("date.local").alias("date_local"),
                        sf.col("date.utc").alias("date_utc")]
    
    # Apply the select operation to unnest the DataFrame
    return frame.select(selected_columns + unnested_columns)

def correct_datatypes(frame: DataFrame) -> DataFrame:
    mapping = {
        IntegerType: {
            "locationId",
        },
        DoubleType: {
            "longitude",
            "latitude",
            "value",
        },
        StringType: {
            "city",
            "country",
            "entity",
            "location",
            "parameter",
            "sensorType",
            "unit",
        },
        BooleanType: {
            "isAnalysis",
            "isMobile",
        },
        TimestampType: {
            "date_utc",
        },
    }

    for datatype, colnames in mapping.items():
        for colname in colnames:
            frame = frame.withColumn(
                colname, sf.col(colname).cast(datatype())
            )

    return frame

def improve_columns_names(frame: DataFrame) -> DataFrame:
    renames = {
        "locationId": "location_id",
        "isAnalysis": "is_analysis",
        "isMobile": "is_mobile",
        "sensorType": "sensor_type",
        "date_local": "date_local",
        "date_utc": "date_utc",
    }
    frame = batch_rename_columns(frame, renames)
    # let's also lowercase all column names, to offer a similar style (and less pressing of SHIFT with the pinky)
    return lowercase_column_names(frame)

def lowercase_column_names(frame: DataFrame) -> DataFrame:
    mapping = {c: c.lower() for c in frame.columns}
    return batch_rename_columns(frame, mapping)


def batch_rename_columns(
    df: DataFrame, mapping: Mapping[str, str]
) -> DataFrame:
    for old_name, new_name in mapping.items():
        df = df.withColumnRenamed(old_name, new_name)
    return df

if __name__ == "__main__":
    resource_path = S3_BUCKET_OPENAQ
    
    # target_path = path_to_exercises / "target"
    # target_dir.mkdir(exist_ok=True)

    # Extract
    frame = read_data(resource_path)
    frame = clean_data(frame)
    to_snowflake(frame)