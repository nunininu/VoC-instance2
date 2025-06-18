import logging
import os
from datetime import datetime, timedelta

from dotenv import load_dotenv
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
load_dotenv("/opt/airflow/.env")
BUCKET = "wh04-voc-bucket"
BASE_PATH = "meta"
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_S3_ENDPOINT = os.getenv("AWS_S3_ENDPOINT")

# ----- 유틸 함수 -----
def check_files_exist(**context) -> bool:
    """
    S3에 파일이 존재하는지 확인
    
    Args:  
        context (dict): task 내부 내용
    
    Returns:  
        (bool): 파일 존재 여부
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    year = context["logical_date"].year
    month = context["logical_date"].month
    s3_hook = S3Hook(aws_conn_id="aws_s3_conn")
    logging.info(f"S3 연결 완료")

    paths = [
        f"{BASE_PATH}/consultings/year={year}/month={'%02d' % month}/",
        f"{BASE_PATH}/analysis_result/year={year}/month={'%02d' % month}/"
    ]

    for path in paths:
        logging.info(f"경로 체크중... {path}")
        files = s3_hook.list_keys(bucket_name=BUCKET, prefix=path)
        if not files:
            return False
    return True

def generate_sql(**context) -> str:
    """
    XCom으로 넘길 sql 쿼리문 작성
    
    Args:  
        context (dict): task 내부 내용
    
    Returns:
        (str): 쿼리문
    """
    import uuid
    from io import BytesIO
    import pandas as pd
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    year = context["logical_date"].year
    month = context["logical_date"].month
    s3_hook = S3Hook(aws_conn_id="aws_s3_conn")

    sql = []
    for kind in ["category", "keyword"]:
        path = f"{BASE_PATH}/agg/{kind}/year={year}/month={'%02d' % month}/"
        logger.info(f"경로 탐색중: {path}")
        files = s3_hook.list_keys(bucket_name=BUCKET, prefix=path)

        for obj in files:
            s3_obj = s3_hook.get_key(key=obj, bucket_name=BUCKET)
            data = s3_obj.get()["Body"].read()
            df = pd.read_parquet(BytesIO(data))

            for _, raw in df.iterrows():
                col = "category_id" if kind == "category" else "keyword"
                if kind == "category":
                    val_str = str(raw["category_id"])
                    val_sql = val_str
                else:
                    val_str = raw["keyword"].replace("'","''")
                    val_sql = f"'{val_str}'"

                sql.append(f"""
                INSERT INTO {kind}_agg ({kind}_agg_id, {col}, count, year, month)
                VALUES ('{str(uuid.uuid4())}', {val_sql}, {raw['count']}, {year}, {month});
                """)
    return "\n".join(sql)


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
    max_active_runs=1,
    max_active_tasks=1
) as dag:
    # 1번 task: 파일 존재 여부 체크
    check_data = ShortCircuitOperator(
        task_id="check_data",
        python_callable=check_files_exist,
        provide_context=True
    )

    # 2번 task: Spark을 통해 한 달치 데이터 집계
    spark_agg = SparkSubmitOperator(
        task_id="spark_monthly_agg",
        application="/opt/spark_jobs/monthly_agg.py",
        conn_id="spark_conn",
        application_args=[
            "--year", "{{ logical_date.year }}",
            "--month", "{{ '%02d' % logical_date.month }}",
            "--consult_path", f"s3a://{BUCKET}/{BASE_PATH}/consultings",
            "--result_path", f"s3a://{BUCKET}/{BASE_PATH}/analysis_result",
            "--output_path", f"s3a://{BUCKET}/{BASE_PATH}/agg"
        ],
        conf={
            "spark.driver.host": "3.38.223.1",
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.driver.port": "36000",
            "spark.blockManager.port": "36010",
            "spark.sql.parquet.int96RebaseModeInRead": "LEGACY",
            "spark.sql.parquet.datetimeRebaseModeInRead": "LEGACY",
            "spark.sql.parquet.enableVectorizedReader": "false",
            "spark.sql.parquet.filterPushdown": "true",
            "spark.hadoop.fs.s3a.connection.maximum": "100",
            "spark.hadoop.fs.s3a.threads.max": "20",
            "spark.sql.shuffle.partitions": "4",
            "spark.executor.memory": "1g",
            "spark.executor.cores": "1",
            "spark.driver.memory": "1g",
            "spark.driver.cores": "1",
            "spark.hadoop.fs.s3a.access.key": AWS_ACCESS_KEY,
            "spark.hadoop.fs.s3a.secret.key": AWS_SECRET_KEY,
            "spark.hadoop.fs.s3a.endpoint": AWS_S3_ENDPOINT,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.jars": "/opt/spark/jars/hadoop-aws-3.3.2.jar,/opt/spark/jars/aws-java-sdk-bundle-1.11.1026.jar",
        },
        verbose=True
    )

    # 3번 task: RDS에 실행할 쿼리문 작성
    prepare_sql = PythonOperator(
        task_id="prepare_sql",
        python_callable=generate_sql,
        provide_context=True
    )

    # 4번 task: RDS에 쿼리문 실행
    save_to_rds = PostgresOperator(
        task_id="save_to_rds",
        postgres_conn_id="aws_postgres_conn",
        sql="{{ ti.xcom_pull(task_ids='prepare_sql') }}"
    )

    check_data >> spark_agg >> prepare_sql >> save_to_rds
