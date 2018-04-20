FROM uhopper/hadoop:2.7.2


MAINTAINER edxops
# install mysql 5.6 client
ENV MYSQL_VERSION=5.6 DEBIAN_FRONTEND=noninteractive
WORKDIR /tmp
RUN \
  echo "deb http://ftp.de.debian.org/debian/ stretch main non-free contrib\n" > /etc/apt/sources.list && \
  echo "deb-src http://ftp.de.debian.org/debian/ stretch main non-free contrib\n" >> /etc/apt/sources.list && \
  echo "deb http://security.debian.org/ stretch/updates main contrib non-free\n" >> /etc/apt/sources.list && \
  echo "deb-src http://security.debian.org/ stretch/updates main contrib non-free" >> /etc/apt/sources.list

RUN apt-get -y update
RUN apt-get -yqq install apt-transport-https lsb-release ca-certificates gnupg2
RUN apt-key adv --keyserver ha.pool.sks-keyservers.net --recv-keys A4A9406876FCBD3C456770C88C718D3B5072E1F5
RUN echo "deb http://repo.mysql.com/apt/debian/ stretch mysql-${MYSQL_VERSION}" > /etc/apt/sources.list.d/mysql.list
RUN apt-get update && apt-get install -y mysql-community-client && rm -rf /var/lib/apt/lists/*

WORKDIR /
ADD scripts/nodemanager.sh /run.sh
RUN chmod a+x /run.sh

CMD ["/run.sh"]