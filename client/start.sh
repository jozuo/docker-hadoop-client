#!/bin/bash

INIT_MARKER="init-completed"

source /etc/profile

# setup hive
if [ `ls /hive | grep ${INIT_MARKER} | wc -l` -eq 0 ]; then
  sudo chown hadoop:hadoop /spark

  hadoop fs -mkdir -p /tmp
  hadoop fs -chmod g+w /tmp

  hadoop fs -mkdir -p /user/hive/warehouse
  hadoop fs -chmod g+w /user/hive/warehouse

  hadoop fs -mkdir -p /user/spark/warehouse
  hadoop fs -chown g+w /user/spark/warehouse 

  schematool -dbType derby -initSchema --verbose
  touch /hive/${INIT_MARKER}
fi

# start hbase
start-hbase.sh && bash
