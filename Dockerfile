FROM openjdk:17 as builder

WORKDIR /app
COPY ./tools/kzex/gradle /app/gradle
COPY ./tools/kzex/gradlew /app/gradlew
COPY ./tools/kzex/build.gradle.kts /app/build.gradle.kts
COPY ./tools/kzex/settings.gradle.kts /app/settings.gradle.kts
COPY ./tools/kzex/src /app/src

RUN sh ./gradlew build -Dversion=0.0.0

#------------------------------------------------------------

FROM apache/airflow:2.7.1
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" xlrd==1.2.0

USER root

# Установка OpenJDK
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk

#RUN apt-get install -y wget gnupg
#RUN apt-get install -y libappindicator1 fonts-liberation
#RUN wget -O /tmp/chrome.deb https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
##RUN apt install -y gdebi-core
#RUN apt-get install /tmp/chrome.deb -y
#RUN apt-get install -f -y

RUN apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir /opt/airflow/tools && chown -R airflow /opt/airflow/tools
RUN chmod -R 777 /opt/airflow/tools

USER airflow

COPY --from=builder /app/build/libs/kzex.jar /opt/airflow/tools/kzex.jar