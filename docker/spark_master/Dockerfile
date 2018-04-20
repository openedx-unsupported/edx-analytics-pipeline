FROM bde2020/spark-base:2.1.0-hadoop2.7


MAINTAINER edxops

COPY scripts/master.sh /
ENV SPARK_MASTER_PORT=7077 \
    SPARK_MASTER_WEBUI_PORT=8080 \
    SPARK_MASTER_LOG=/spark/logs \
    HADOOP_USER_NAME=hadoop \
    SPARK_HOME=/spark \
    PATH=$PATH:/spark/bin

RUN apt-get -y update && apt-get -y install --reinstall python-pkg-resources \
    && cp /spark/conf/spark-defaults.conf.template /spark/conf/spark-defaults.conf \
    && echo 'spark.master  spark://sparkmaster:7077\nspark.eventLog.enabled  true\nspark.eventLog.dir  hdfs://namenode:8020/tmp/spark-events\nspark.history.fs.logDirectory  hdfs://namenode:8020/tmp/spark-events' > /spark/conf/spark-defaults.conf

EXPOSE 8080 7077 6066 18080 4040

CMD ["/bin/bash", "/master.sh"]