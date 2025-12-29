FROM apache/spark:4.1.0-scala2.13-java21-python3-r-ubuntu

USER root

# Install Python and pip if not already available
RUN apt-get update && \
    apt-get install -y python3-pip curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Add Hadoop AWS and AWS SDK JARs for S3 compatibility with MinIO
RUN cd /opt/spark/jars && \
    curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

WORKDIR /opt/spark

# Set Spark home
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

USER spark
