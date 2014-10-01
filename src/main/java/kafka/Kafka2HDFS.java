package kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This class is designed to get messages from Kafka stream and write them into HDFS directly.
 * Created by shengzhao on 9/30/14.
 */
public class Kafka2HDFS {

  private static final Logger _logger = LoggerFactory.getLogger(Kafka2HDFS.class);
  private int _threadNum;
  private String _topic;
  private List<KafkaStream<byte[], byte[]>> _kafkaStreams;
  private ExecutorService _executors;
  private ConsumerConnector _consumerConnector;


  public Kafka2HDFS(String configFile) throws IOException {
    Properties properties = new Properties();
    properties.load(new FileInputStream(configFile));
    int partitionID = Integer.parseInt(properties.getProperty(Constants.KAFKA_PARTITION_NUM));
    this._threadNum = partitionID;
    this._topic = properties.getProperty("topic");
    _logger.info("Zookeeper: {}", properties.getProperty(Constants.ZK_CONNECT_STRING));
    _logger.info("Consumer Group: {}", properties.getProperty(Constants.KAFKA_CONSUMER_GROUP_ID));
    _logger.info("Total Partition: {}", partitionID);

    _consumerConnector = Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(_topic, partitionID);
    Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = _consumerConnector.createMessageStreams(topicCountMap);
    _kafkaStreams = messageStreams.get(_topic);
    _logger.info("Got {} Kafka streams for topic {}", _kafkaStreams.size(), _topic);
    _executors = Executors.newFixedThreadPool(partitionID);
  }


  public void run() {
    for (int i = 0; i < _kafkaStreams.size(); i++) {
      _executors.submit(new Kafka2HDFSWorker(i, _kafkaStreams.get(i)));
    }

    try {
      while (true) {
        Thread.sleep(10000);
      }
    } catch (InterruptedException e) {
      _logger.warn("Interrupted Exception! Cleaning up.");
    } finally {
      shutDown();
    }
  }

  private void shutDown() {
    _executors.shutdown();
    _consumerConnector.shutdown();
  }

  public static void main(String[] args) throws IOException {

    Kafka2HDFS kafka2HDFS = new Kafka2HDFS(args[0]);
    kafka2HDFS.run();
  }
}
