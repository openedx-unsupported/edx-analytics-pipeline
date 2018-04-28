#!/usr/bin/env bash

set -e
set -o pipefail
set -x

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

# Bring the databases online.
docker-compose up -d mysql analyticspipeline

# Ensure the MySQL server is online and usable
echo "Waiting for MySQL"
until docker exec -i edx.devstack.analytics_pipeline.mysql mysql -uroot -se "SELECT EXISTS(SELECT 1 FROM mysql.user WHERE user = 'root')" &> /dev/null
do
  printf "."
  sleep 1
done

# In the event of a fresh MySQL container, wait a few seconds for the server to restart
# This can be removed once https://github.com/docker-library/mysql/issues/245 is resolved.
sleep 20

echo -e "MySQL ready"

echo -e "${GREEN}Creating databases and users...${NC}"
docker exec -i edx.devstack.analytics_pipeline.mysql mysql -uroot mysql < ./scripts/provision.sql
sleep 3

# initialize hive metastore
echo -e "${GREEN}Initializing HIVE...${NC}"
docker exec -i edx.devstack.analytics_pipeline bash -c '/edx/app/hadoop/hive/bin/schematool -dbType mysql -initSchema'

# materialize hadoop directory structure
echo -e "${GREEN}Initializing Hadoop directory structure...${NC}"

until curl http://namenode:50070/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus|grep -q 'active'; do
  printf "Waiting for namenode!"
  sleep 5
done
sleep 30 # for datanode & other services to activate
echo -e "${GREEN}Namenode is ready!${NC}"

docker exec -i -u hadoop edx.devstack.analytics_pipeline bash -c 'sudo /edx/app/hadoop/hadoop/bin/hdfs dfs -chown -R hadoop:hadoop hdfs://namenode:8020/; hdfs dfs -mkdir -p hdfs://namenode:8020/edx-analytics-pipeline/{warehouse,marker,manifest,packages} hdfs://namenode:8020/{spark-warehouse,data} hdfs://namenode:8020/tmp/spark-events;hdfs dfs -copyFromLocal -f /edx/app/hadoop/lib/edx-analytics-hadoop-util.jar hdfs://namenode:8020/edx-analytics-pipeline/packages/;'

echo -e "${GREEN}Provisioning complete!${NC}"
