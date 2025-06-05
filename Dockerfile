FROM apache/airflow:2.10.4

C:q
OPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt
