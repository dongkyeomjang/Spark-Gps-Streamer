# 1단계: 빌드
FROM gradle:8.4.0-jdk17 AS builder
WORKDIR /app
COPY . .
RUN gradle shadowJar --no-daemon

# 2단계: 실행
FROM openjdk:17-jdk-slim
WORKDIR /opt/app
COPY --from=builder /app/build/libs/*-all.jar app.jar

ENTRYPOINT ["java", "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED", "-jar", "app.jar"]

