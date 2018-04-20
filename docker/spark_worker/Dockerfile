FROM bde2020/spark-base:2.1.0-hadoop2.7


MAINTAINER edxops

COPY scripts/worker.sh /
ENV SPARK_WORKER_WEBUI_PORT=8081 \
    SPARK_WORKER_LOG=/spark/logs \
    SPARK_MASTER="spark://sparkmaster:7077" \
    SPARK_HOME=/spark

RUN apt-get -y update && apt-get -y install --reinstall python-pkg-resources

EXPOSE 8081

CMD ["/bin/bash", "/worker.sh"]