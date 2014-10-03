#!/bin/bash

for file in /Users/shengzhao/Github/KafkaSecretary/target/libs/*.jar
do
    CLASSPATH=$file:$CLASSPATH
done

CLASSPATH="/Users/shengzhao/Github/KafkaSecretary/target/KafkaSecretary-1.0-SNAPSHOT.jar:$CLASSPATH"

java -classpath $CLASSPATH \
    -Djava.awt.headless=true \
    -Dlog4j.configuration=file:///Users/shengzhao/Github/KafkaSecretary/conf/log4j.properties \
    kafka.Kafka2HDFS /Users/shengzhao/Github/KafkaSecretary/conf/kafka.properties

