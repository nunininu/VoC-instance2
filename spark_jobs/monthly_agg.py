import os
import sys

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, LongType
from pyspark.sql.functions import col, explode, split, count, lit, to_timestamp

def main(year, month, consult_path, result_path, output_path):
    """
    Spark를 통해 카테고리 및 키워드 집계
    
    Args:  
        year (int): 분석 년도  
        month (int): 분석 월  
        consult_path (str): 문의 데이터 경로  
        result_path (str): 분석 결과 데이터 경로  
        output_path (str): 저장 경로
    """
    load_dotenv()
    spark = SparkSession.builder \
    .appName("VOC Monthly Aggregation") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.endpoint", os.getenv("AWS_S3_ENDPOINT")) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
    .config("spark.sql.parquet.binaryAsString", "true") \
    .getOrCreate()

    consulting_schema = StructType([
        StructField("consulting_id", StringType(), False),
        StructField("client_id", StringType(), False),
        StructField("category_id", LongType(), False),
        StructField("channel_id", LongType(), False),
        StructField("consulting_datetime", StringType(), False),
        StructField("turns", LongType(), False),
        StructField("content", StringType(), False),
        StructField("analysis_status", StringType(), False),
    ])

    analysis_result_schema = StructType([
        StructField("analysis_result_id", StringType(), False),
        StructField("client_id", StringType(), False),
        StructField("consulting_id", StringType(), False),
        StructField("keywords", StringType(), False),
        StructField("is_negative", BooleanType(), False),
        StructField("negative_point", StringType(), True),
        StructField("created_datetime", LongType(), False),
    ])


    # 데이터 불러오기
    consult_df = spark.read.schema(consulting_schema).parquet(f"{consult_path}/year={year}/month={month}")
    consult_df = consult_df.withColumn(
        "consulting_datetime", to_timestamp("consulting_datetime")
    )
    result_df = spark.read.schema(analysis_result_schema).parquet(f"{result_path}/year={year}/month={month}")

    # 조인 (consulting_id 기준)
    df = result_df.join(
        consult_df.select("consulting_id", "category_id"),
        on="consulting_id",
        how="inner"
    )
    df = df.withColumn("year", lit(year)).withColumn("month", lit(month))

    # 카테고리별 집계
    category_agg = df.groupBy("category_id", "year", "month").agg(
        count("*").alias("count")
    )

    # 키워드별 집계
    df_keywords = df.withColumn("keyword", explode(split(col("keywords"), ",")))
    keyword_agg = df_keywords.groupBy("keyword", "year", "month").agg(
        count("*").alias("count")
    )

    # 저장
    category_agg.write.mode("overwrite").partitionBy("year", "month").parquet(f"{output_path}/category")
    keyword_agg.write.mode("overwrite").partitionBy("year", "month").parquet(f"{output_path}/keyword")

    spark.stop()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True)
    parser.add_argument("--month", required=True)
    parser.add_argument("--consult_path", required=True)
    parser.add_argument("--result_path", required=True)
    parser.add_argument("--output_path", required=True)
    args = parser.parse_args()

    main(args.year, args.month, args.consult_path, args.result_path, args.output_path)
