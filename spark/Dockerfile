FROM python:3.11-slim

# Java is required by PySpark
RUN apt-get update && apt-get install -y \
    default-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
