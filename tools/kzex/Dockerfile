FROM openjdk:17

WORKDIR /app
COPY ./gradle /app/gradle
COPY ./gradlew /app/gradlew
COPY ./build.gradle.kts /app/build.gradle.kts
COPY ./settings.gradle.kts /app/settings.gradle.kts
COPY ./src /app/src

RUN sh ./gradlew build -Dversion=0.0.0