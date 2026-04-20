FROM apache/spark:3.5.0

USER root

RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/spark/jars

RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    wget https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar && \
    wget https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar

RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

RUN wget https://repo1.maven.org/maven2/com/clickhouse/spark/clickhouse-spark-runtime-3.5_2.12/0.10.0/clickhouse-spark-runtime-3.5_2.12-0.10.0.jar && \
    wget https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.9.5/clickhouse-jdbc-0.9.5.jar

WORKDIR /app
COPY streaming_requirements.txt .
RUN pip install --no-cache-dir -r streaming_requirements.txt

COPY streaming_processor.py .

ENV SPARK_DIST_CLASSPATH="/opt/spark/jars/*"

USER 185