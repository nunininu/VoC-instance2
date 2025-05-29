FROM apache/airflow:3.0.0

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --prefer-binary -r /requirements.txt
