FROM eclipse-temurin:17-jre

WORKDIR /app

COPY target/*-jar-with-dependencies.jar app.jar

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
