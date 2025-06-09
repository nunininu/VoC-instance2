import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import ShortCircuitOperator, PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# ----- 로깅 -----
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)
logger = logging.getLogger("monthly_agg")

# ----- 공통 클라이언트 -----
BUCKET = "wh04-voc-bucket"
BASE_PATH = "sample/raw/consulting_api_raw"

# ----- 유틸 함수 -----
def check_files_exist(**context):
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    year = context["logical_date"].year
    month = context["logical_date"].month
    s3_hook = S3Hook(aws_conn_id="aws_s3_conn")
    logging.info(f"S3 연결 완료")
    
    # paths = [
        # f"{BASE_PATH}/consultings/year={year}/month={month}",
    #     f"{BASE_PATH}/analysis_result/year={year}/month={month}"
    # ]

    # for path in paths:
    #     files = gcs_hook.list(bucket_name=BUCKET, prefix=BASE_PATH)
    #     if not files:
    #         return False
    # return True
    files = s3_hook.list_keys(bucket_name=BUCKET, prefix=BASE_PATH)
    if not files:
        return False
    return True

def generate_sql(**context):
    import uuid
    from io import BytesIO
    import pandas as pd
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    year = context["logical_date"].year
    month = context["logical_date"].month
    s3_hook = S3Hook(aws_conn_id="aws_s3_conn")

    # sql = []
    # for kind in ["category", "keyword", "sentiment"]:
    #     path = f"{BASE_PATH}/{kind}/year={year}/month={month}"
    #     files = gcs_hook.list(bucket_name=BUCKET, prefix=path)

    #     for obj in files:
    #         data = gcs_hook.download(bucket_name=BUCKET, object_name=obj)
    #         df = pd.read_parquet(BytesIO(data))

    #         for _, raw in df.iterrows():
    #             if kind == "sentiment":
    #                 sql.append(f"""
    #                 INSERT INTO sentiment_agg (analysis_reasult_id, positive, negative, year, month)
    #                 VALUES ({raw['analysis_result_id']}, {raw['positive']}, {raw['negative']}, {year}, {month});
    #                 """)
    #             else:
    #                 sql.append(f"""
    #                 INSERT INTO {kind}_agg (analysis_result_id, {kind}, count, year, month)
    #                 VALUES ({raw['analysis_result_id']}, '{row[kind]}', {raw['count']}, year, month);
    #                 """)
    # return sql
    return f"""
    INSERT INTO sample (id)
    VALUES ('{str(uuid.uuid4())}');
    """


# ----- DAG -----
with DAG(
    "voc_monthly_agg",
    default_args={
        "depends_on_past": False
    },
    schedule="@monthly",
    start_date=datetime(2020, 1, 1),
    catchup=True,
    tags=["voc", "monthly", "agg"],
    max_active_runs=3,
    max_active_tasks=3
) as dag:
    check_data = ShortCircuitOperator(
        task_id="check_data",
        python_callable=check_files_exist,
        provide_context=True
    )

    spark_agg = SparkSubmitOperator(
        task_id="spark_monthly_agg",
        application="/opt/spark_jobs/monthly_agg.py",
        conn_id="spark_conn",
        application_args=[
            "--year", "{{ logical_date.year }}",
            "--month", "{{ '%02d' % logical_date.month }}",
            "--consult_path", f"{BASE_PATH}/consultings",
            "--result_path", f"{BASE_PATH}/analysis_result",
            "--output_path", f"{BASE_PATH}/aggregated"
        ],
        verbose=True
    )

    prepare_sql = PythonOperator(
        task_id="prepare_sql",
        python_callable=generate_sql,
        provide_context=True
    )

    save_to_rds = PostgresOperator(
        task_id="save_to_rds",
        postgres_conn_id="aws_postgres_conn",
        sql="{{ ti.xcom_pull(task_ids='prepare_sql') }}"
    )

    check_data >> spark_agg >> prepare_sql >> save_to_rds
