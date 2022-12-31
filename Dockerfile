FROM apache/airflow
USER airflow
COPY requirements.txt /
RUN pip install -r /requirements.txt
