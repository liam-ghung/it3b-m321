FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /source
COPY pom.xml pom.xml
COPY chat-service/pom.xml chat-service/pom.xml
COPY batch-writer/pom.xml batch-writer/pom.xml
COPY chat-service/src chat-service/src
RUN mvn -B -ntp -pl chat-service -am package -Dmaven.test.skip=true

FROM eclipse-temurin:21-jre-jammy
# curl erlaubt API-Prüfungen innerhalb des Netzes ohne veröffentlichte Ports.
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /source/chat-service/target/chat-service-0.1.0.jar app.jar
USER 10001
ENTRYPOINT ["java", "-jar", "app.jar"]
