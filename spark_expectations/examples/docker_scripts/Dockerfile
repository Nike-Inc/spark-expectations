FROM centos:latest

RUN cd /etc/yum.repos.d/
RUN sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-*
RUN sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-*

RUN yum -y install java

RUN yum update -y \
    && yum install -y java-1.8.0-openjdk-devel \
    && yum clean all

ENV KAFKA_VERSION=3.3.1
ENV KAFKA_HOME=/opt/kafka

# RUN curl -SL https://downloads.apache.org/kafka/${KAFKA_VERSION}/kafka_2.12-${KAFKA_VERSION}.tgz \
#     | tar -xzC /opt \
#     && mv /opt/kafka_2.12-${KAFKA_VERSION} ${KAFKA_HOME}

RUN curl -SL "https://archive.apache.org/dist/kafka/3.0.0/kafka_2.13-3.0.0.tgz" \
    | tar -xzC /opt \
    && mv "/opt/kafka_2.13-3.0.0" "${KAFKA_HOME}"


ENV PATH ${PATH}:${KAFKA_HOME}/bin

COPY start-kafka.sh /usr/bin/
RUN chmod +x /usr/bin/start-kafka.sh

# EXPOSE 9092

CMD ["start-kafka.sh"]