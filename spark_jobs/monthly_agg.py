import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, avg, count, lit

def main(year, month, consult_path, result_path, output_path):
    spark = SparkSession.builder.appName("VOC Monthly Aggregation").getOrCreate()

    # 1. 데이터 불러오기
    # consult_df = spark.read.parquet(f"{consult_path}/year={year}/month={month}")
    # result_df = spark.read.parquet(f"{result_path}/year={year}/month={month}")

    # 2. 조인 (consulting_id 기준)
    # df = result_df.join(
    #     consult_df.select("consulting_id", "category"),
    #     on="consulting_id",
    #     how="inner"
    # )

    # # year, month 컬럼 추가
    # df = df.withColumn("year", lit(year)).withColumn("month", lit(month))

    # # 3. 카테고리별 집계
    # category_agg = df.groupBy("category", "year", "month").agg(
    #     count("*").alias("count")
    # )

    # # 4. 키워드별 집계
    # df_keywords = df.withColumn("keyword", explode(split(col("keywords"), ",")))
    # keyword_agg = df_keywords.groupBy("keyword", "year", "month").agg(
    #     count("*").alias("count")
    # )

    # # 5. 전체 긍부정 평균
    # sentiment_agg = df.groupBy("year", "month").agg(
    #     avg("positive").alias("avg_positive"),
    #     avg("negative").alias("avg_negative")
    # )

    # # 6. 저장
    # category_agg.write.mode("overwrite").partitionBy("year", "month").parquet(f"{output_path}/category")
    # keyword_agg.write.mode("overwrite").partitionBy("year", "month").parquet(f"{output_path}/keyword")
    # sentiment_agg.write.mode("overwrite").partitionBy("year", "month").parquet(f"{output_path}/sentiment")
    
    # ---- 테스트용 -----
    # result_df = result_df.withColumn("year", lit(year)).withColumn("month", lit(month))
    # df_keywords = result_df.withColumn("keyword", explode(split(col("keywords"), ",")))
    # keyword_agg = df_keywords.groupBy("keyword", "year", "month").agg(
    #     count("*").alias("count")
    # )
    # sentiment_agg = result_df.groupBy("year", "month").agg(
    #     avg("positive").alias("avg_positive"),
    #     avg("negative").alias("avg_negative")
    # )
    # keyword_agg.write.mode("overwrite").partitionBy("year", "month").parquet(f"{output_path}/keyword")
    # sentiment_agg.write.mode("overwrite").partitionBy("year", "month").parquet(f"{output_path}/sentiment")
    print("spark test")
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
