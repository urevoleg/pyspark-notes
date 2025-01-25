import os
import logging
logging.basicConfig(level=logging.INFO,
                    format="⏰ %(asctime)s - 💎 %(levelname)s - %(filename)s - %(funcName)s - 🧾 %(message)s")
import random
import typing as t

import pendulum
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.types import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType, DateType, TimestampType

from dotenv import load_dotenv
load_dotenv()


def get_spark_session(n: t.Any = 4):
    return SparkSession.builder \
    .master(f"local[{n}]") \
    .appName('Spark-Reddit') \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.default.parallelism", "100")\
    .getOrCreate()

    # .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.0") \
    # .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    # .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    # .config("spark.sql.catalog.local.type", "hadoop") \
    # .config("spark.sql.catalog.local.warehouse", "s3a://ods-public-spark/iceberg_warehouse") \


def setup_s3_with_spark(spark: SparkSession):
    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3.S3FileSystem")
    # hadoopConf.set("fs.s3a.endpoint", "http://192.168.55.102:9002")
    hadoopConf.set("fs.s3a.endpoint", "https://storage.yandexcloud.net")
    hadoopConf.set("fs.s3a.access.key", os.getenv("S3_ACCESS_KEY"))
    hadoopConf.set("fs.s3a.secret.key", os.getenv("S3_ACCESS_SECRET_KEY"))


def get_mock_dataframe(spark: SparkSession):
    # Генерируем данные
    data = []
    for i in range(1000):
        name = f"User_{i + random.randint(1000, 1999)}"
        age = random.randint(18, 65)  # Случайный возраст от 18 до 65
        city = random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'])
        data.append(Row(name=name, age=age, city=city))

    # Создаем DataFrame из списка строк
    df = spark.createDataFrame(data)
    return df


def processing(bucket: str, s3_prefix: str):
    logging.info(f'Content processing....')

    spark = get_spark_session(n=4)
    logging.info(f'Spark version: {spark.version}')
    setup_s3_with_spark(spark=spark)

    started_at = pendulum.now()
    logging.info(f'Started at: {started_at}')

    df = spark.read\
        .parquet(f"s3a://{bucket}/{s3_prefix}/*.parquet")

    # schema =  StructType() \
    #             .add("application_id", IntegerType(), True) \
    #             .add("ios_ifa", StringType(), True) \
    #             .add("ios_ifv", StringType(), True) \
    #             .add("android_id", StringType(), True) \
    #             .add("google_aid", StringType(), True) \
    #             .add("os_name", StringType(), True) \
    #             .add("os_version", IntegerType(), True) \
    #             .add("device_manufacturer", StringType(), True) \
    #             .add("device_model", StringType(), True) \
    #             .add("device_type", StringType(), True) \
    #             .add("device_locale", StringType(), True) \
    #             .add("app_version_name", StringType(), True) \
    #             .add("app_package_name", StringType(), True) \
    #             .add("event_name", StringType(), True) \
    #             .add("event_json", StringType(), True) \
    #             .add("event_date", StringType(), True) \
    #             .add("event_datetime", StringType(), True) \
    #             .add("event_timestamp", StringType(), True) \
    #             .add("event_receive_datetime", StringType(), True) \
    #             .add("event_receive_timestamp", StringType(), True) \
    #             .add("connection_type", StringType(), True) \
    #             .add("operator_name", StringType(), True) \
    #             .add("mcc", StringType(), True) \
    #             .add("mnc", StringType(), True) \
    #             .add("country_iso_code", StringType(), True) \
    #             .add("city", StringType(), True) \
    #             .add("appmetrica_device_id", StringType(), True)

    df.show()

    df_with_date = df.withColumn("created_dt", F.date_format(F.to_timestamp(F.col("created_utc")), "yyyy-MM-dd"))

    dst_bucket = "ods-public-spark"
    dst_s3_prefix = "reddit"
    df_with_date.write \
        .partitionBy("created_dt")\
        .mode("overwrite")\
        .parquet(f"s3a://{dst_bucket}/{dst_s3_prefix}")

    table_name = "local.default.reddit"  # Укажите имя вашей таблицы

    # df_with_date.writeTo(table_name) \
    #     .partitionedBy(F.col("created_dt")) \
    #     .tableProperty("format-version", "2") \
    #     .createOrReplace()  # Создаем новую таблицу
    #
    # # Проверяем данные в таблице
    # spark.table(table_name).show()

    end_at = pendulum.now()
    logging.info(f'Ended at: {pendulum.now()}, duration: {round((end_at - started_at).in_seconds() / 60, 1)} minutes')

    logging.info(f'End ....')


if __name__ == "__main__":

    dm_date = pendulum.today().date()

    s3_bucket = "public-bucket-6"
    s3_prefix = "reddit/2025-01-1{5,6,7,8,9}"
    processing(bucket=s3_bucket, s3_prefix=s3_prefix)
