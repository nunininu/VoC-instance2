from datetime import datetime

from airflow.sdk import dag, task

@dag(
    schedule="@monthly",
    start_date=datetime(2020, 1, 1),
    catchup=False,
    tags=["test"],
)
def test():
    @task()
    def extract():
        data = {"a": 0, "b": 1, "c": 2, "d": 3, "e": 4}
        return data

    @task()
    def transform(data: dict):
        total = 0

        for v in data.values():
            total += v

        return {"sum": total}

    @task()
    def load(value: dict):
        print(value["sum"])

    data = extract()
    value = transform(data)
    load(value)

test()
