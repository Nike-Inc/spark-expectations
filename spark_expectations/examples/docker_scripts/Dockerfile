FROM ubuntu:latest

# Update and install Java
RUN apt-get update -y \
    && apt-get install -y \
        openjdk-8-jdk \
        curl \
        ca-certificates \
    && apt-get clean

# Add custom CA certificate and ensure the certificate is available in the build context
COPY certs/ /usr/local/share/ca-certificates/
RUN update-ca-certificates

ENV KAFKA_VERSION=3.3.1
ENV KAFKA_HOME=/opt/kafka

# Download and install Kafka securely
RUN curl -SL "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_2.13-${KAFKA_VERSION}.tgz" \
    | tar -xzC /opt \
    && mv "/opt/kafka_2.13-${KAFKA_VERSION}" "${KAFKA_HOME}"

ENV PATH=${PATH}:${KAFKA_HOME}/bin
COPY start-kafka.sh /usr/bin/
RUN chmod +x /usr/bin/start-kafka.sh

# Expose Kafka port
EXPOSE 9092

# Set default command
CMD ["start-kafka.sh"]