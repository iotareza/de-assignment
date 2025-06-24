#!/bin/bash

# Create spark-jars directory
mkdir -p spark-jars

# Download AWS S3 connector JARs
echo "Downloading AWS S3 connector JARs..."

# Download hadoop-aws JAR
wget -O spark-jars/hadoop-aws-3.3.4.jar \
  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# Download AWS Java SDK bundle
wget -O spark-jars/aws-java-sdk-bundle-1.12.261.jar \
  https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.261/aws-java-sdk-bundle-1.12.261.jar

echo "AWS S3 connector JARs downloaded successfully!"
echo "Files in spark-jars directory:"
ls -la spark-jars/ 