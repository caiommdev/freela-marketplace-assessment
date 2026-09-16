# syntax=docker/dockerfile:1
# Build context must be the repository root (multi-module Maven reactor).

FROM maven:3.9-eclipse-temurin-25 AS build
WORKDIR /workspace

COPY pom.xml .
COPY eureka-server/pom.xml eureka-server/pom.xml
COPY api-gateway/pom.xml api-gateway/pom.xml
COPY contrato-service/pom.xml contrato-service/pom.xml
COPY notificacao-service/pom.xml notificacao-service/pom.xml
COPY reputacao-service/pom.xml reputacao-service/pom.xml
COPY auditoria-service/pom.xml auditoria-service/pom.xml
RUN mvn -q -B -pl notificacao-service dependency:go-offline

COPY notificacao-service/src notificacao-service/src
RUN mvn -q -B -pl notificacao-service clean package -DskipTests

FROM eclipse-temurin:25-jre AS runtime
WORKDIR /app
COPY --from=build /workspace/notificacao-service/target/*.jar app.jar

EXPOSE 8082
ENTRYPOINT ["java", "-jar", "app.jar"]
