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
BASE_PATH = "meta"

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
        f"{BASE_PATH}/consultings/year={year}/month={month}",
        f"{BASE_PATH}/analysis_result/year={year}/month={month}"
    ]

    for path in paths:
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
    from io import BytesIO
    import pandas as pd
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    year = context["logical_date"].year
    month = context["logical_date"].month
    s3_hook = S3Hook(aws_conn_id="aws_s3_conn")

    sql = []
    for kind in ["category", "keyword"]:
        path = f"{BASE_PATH}/agg/{kind}/year={year}/month={month}"
        files = s3_hook.list_keys(bucket_name=BUCKET, prefix=path)

        for obj in files:
            data = s3_hook.download(bucket_name=BUCKET, object_name=obj)
            df = pd.read_parquet(BytesIO(data))

            for _, raw in df.iterrows():
                sql.append(f"""
                INSERT INTO {kind}_agg ({kind}_agg_id, {kind}, count, year, month)
                VALUES ('{raw['analysis_result_id']}', '{raw[kind]}', {raw['count']}, {year}, {month});
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
            "--consult_path", f"{BASE_PATH}/consultings",
            "--result_path", f"{BASE_PATH}/analysis_result",
            "--output_path", f"{BASE_PATH}/agg"
        ],
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
