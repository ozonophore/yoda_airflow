```shell
echo -e "AIRFLOW_UID=$(id -u) \nAIRFLOW_CONN_DATABASE=postgresql://user:password@ip:port/db" > .env
```
```shell
echo -e "AIRFLOW_CONN_DATABASE=postgresql://user:password@ip:port/db" > .env
```

## Init database
```shell
docker compose up airflow-init
```

## Start Airflow
```shell
docker compose up -d
```

## Selenium Grid
http://localhost:4444/ui#/sessions

The webserver is available at: http://localhost:8080. The default account has the login 'airflow' and the password 'airflow'.
