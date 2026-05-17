FROM flink:1.17

# Install python
RUN apt-get update -y && apt-get install -y python3 python3-pip python3-dev && rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/bin/python3 /usr/bin/python

# Install PyFlink and other requirements
COPY requirements.txt /opt/flink/requirements.txt
RUN pip3 install --no-cache-dir -r /opt/flink/requirements.txt

# Download Kafka connector for Flink (Flink 1.17 - Kafka 3.x)
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.1/flink-sql-connector-kafka-1.17.1.jar

WORKDIR /opt/flink/app
COPY fraud_detection_job.py .
