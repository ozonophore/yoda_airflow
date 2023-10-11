FROM apache/airflow:2.7.1
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" xlrd==1.2.0
