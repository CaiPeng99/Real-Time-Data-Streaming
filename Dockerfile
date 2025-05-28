FROM eclipse-temurin:17-jdk

ENV SCALA_VERSION=2.13.8
ENV SPARK_VERSION=3.5.5

RUN apt-get update && \
    apt-get install -y curl unzip git python3 python3-pip && \
    ln -sf /usr/bin/python3 /usr/bin/python

RUN pip install --no-cache-dir --break-system-packages cassandra-driver

# Set Spark version
ENV SPARK_VERSION=3.5.5

# Download and install Spark with Scala 2.13
RUN curl -O https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3-scala2.13.tgz && \
    tar -xvzf spark-${SPARK_VERSION}-bin-hadoop3-scala2.13.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop3-scala2.13 /opt/spark && \
    ln -s /opt/spark/bin/spark-submit /usr/bin/spark-submit && \
    rm spark-${SPARK_VERSION}-bin-hadoop3-scala2.13.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin