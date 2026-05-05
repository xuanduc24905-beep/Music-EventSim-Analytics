FROM apache/spark:latest

USER root

RUN pip install --no-cache-dir numpy pandas pyarrow

# Spark 4.1.1 dùng Scala 2.13 — pre-bake Kafka connector JARs
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.0/spark-sql-kafka-0-10_2.13-4.0.0.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.0.0/spark-token-provider-kafka-0-10_2.13-4.0.0.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.8.1/kafka-clients-3.8.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar /opt/spark/jars/
