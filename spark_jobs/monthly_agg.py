import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, count, lit

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
    spark = SparkSession.builder.appName("VOC Monthly Aggregation").getOrCreate()

    # 데이터 불러오기
    consult_df = spark.read.parquet(f"{consult_path}/year={year}/month={month}")
    result_df = spark.read.parquet(f"{result_path}/year={year}/month={month}")

    # 조인 (consulting_id 기준)
    df = result_df.join(
        consult_df.select("consulting_id", "category"),
        on="consulting_id",
        how="inner"
    )
    df = df.withColumn("year", lit(year)).withColumn("month", lit(month))

    # 카테고리별 집계
    category_agg = df.groupBy("category", "year", "month").agg(
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
