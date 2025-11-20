#!/bin/bash

echo "Building Flink job..."
mvn clean package -DskipTests

echo "Starting Flink cluster..."
docker-compose up -d

echo "Waiting for cluster to be ready..."
sleep 30

echo "Submitting job..."
JAR_FILE=$(find target -name "*.jar" | head -n 1)
docker-compose exec jobmanager flink run -d -c com.experiment.TaxiRevenueJob /opt/flink/data/$JAR_FILE

echo "Experiment deployed successfully!"