# Multi-stage Dockerfile for MoveInSync Trip Fan-Out Service
# Stage 1: Build JAR using Eclipse Temurin Java 21 LTS
FROM eclipse-temurin:21-jdk AS builder
WORKDIR /workspace

# Copy Maven wrapper and POM to pre-fetch dependencies
COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
RUN ./mvnw dependency:go-offline -B || true

# Copy source tree and compile package
COPY src/ src/
RUN ./mvnw clean package -DskipTests -B

# Stage 2: Runtime image with lightweight JRE 21 Alpine
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# Non-root user for security best practices
RUN addgroup -S moveinsync && adduser -S appuser -G moveinsync
USER appuser

# Copy executable jar from builder stage
COPY --from=builder /workspace/target/*.jar app.jar

EXPOSE 8080

# Production JVM flags with Generational ZGC for sub-millisecond pauses
ENTRYPOINT ["java", "-XX:+UseZGC", "-XX:+ZGenerational", "-Djava.security.egd=file:/dev/./urandom", "-jar", "app.jar"]

