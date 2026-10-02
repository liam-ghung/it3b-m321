FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /source
COPY pom.xml pom.xml
COPY chat-service/pom.xml chat-service/pom.xml
COPY batch-writer/pom.xml batch-writer/pom.xml
COPY batch-writer/src batch-writer/src
RUN mvn -B -ntp -pl batch-writer -am package -Dmaven.test.skip=true

FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY --from=build /source/batch-writer/target/batch-writer-0.1.0.jar app.jar
USER 10001
ENTRYPOINT ["java", "-jar", "app.jar"]
