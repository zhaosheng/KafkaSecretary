#!/bin/bash

for file in ./libs/*.jar
do
    CLASSPATH=$file:$CLASSPATH
done

for file in $HADOOP_HOME/etc/hadoop/*.xml
do
    CLASSPATH=$file:$CLASSPATH
done

CLASSPATH="./KafkaSecretary-1.0-SNAPSHOT.jar:$CLASSPATH"

java -classpath $CLASSPATH \
    -Djava.awt.headless=true \
    -Dlog4j.configuration=file://`pwd`/conf/log4j.properties \
    -Dhadoop.home.dir=$HADOOP_HOME \
    kafka.Kafka2HDFS ./conf/kafka.properties