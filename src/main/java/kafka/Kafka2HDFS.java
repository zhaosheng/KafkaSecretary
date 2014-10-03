package kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.hadoop.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This class is designed to get messages from Kafka stream and write them into HDFS directly.
 * Created by shengzhao on 9/30/14.
 */
public class Kafka2HDFS{

  private static final Logger _logger = LoggerFactory.getLogger(Kafka2HDFS.class);
  private int _threadNum;
  private String _topic;
  private List<KafkaStream<byte[], byte[]>> _kafkaStreams;
  private ExecutorService _executors;
  private ConsumerConnector _consumerConnector;
  private Properties _properties;


  public Kafka2HDFS(String configFile) throws IOException {
    _properties = new Properties();
    _properties.load(new FileInputStream(configFile));
    int partitionID = Integer.parseInt(_properties.getProperty(Constants.KAFKA_PARTITION_NUM));
    this._threadNum = partitionID;
    this._topic = _properties.getProperty(Constants.TOPIC);
    _logger.info("Zookeeper: {}", _properties.getProperty(Constants.ZK_CONNECT_STRING));
    _logger.info("Consumer Group: {}", _properties.getProperty(Constants.KAFKA_CONSUMER_GROUP_ID));
    _logger.info("Total Partition: {}", partitionID);

    _consumerConnector = Consumer.createJavaConsumerConnector(new ConsumerConfig(_properties));
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(_topic, partitionID);
    Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = _consumerConnector.createMessageStreams(topicCountMap);
    _kafkaStreams = messageStreams.get(_topic);
    _logger.info("Got {} Kafka streams for topic {}", _kafkaStreams.size(), _topic);
    _executors = Executors.newFixedThreadPool(partitionID);

  }


  public void run() {
    ArrayList<Kafka2HDFSWorker> workerList = new ArrayList<Kafka2HDFSWorker>(_kafkaStreams.size());
    for (int i = 0; i < _kafkaStreams.size(); i++) {
      Kafka2HDFSWorker worker = new Kafka2HDFSWorker(i, _kafkaStreams.get(i), _properties);
      workerList.add(worker);
      _executors.submit(worker);
    }

    // Hook the housekeeping thread.
    ShutdownHookManager.get().addShutdownHook(new Kafka2HDFSHouseKeeper(this, workerList), 0);
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

  public void shutDown() {
    _executors.shutdownNow();
    _consumerConnector.shutdown();
  }

  public static void main(String[] args) throws IOException {

    Kafka2HDFS kafka2HDFS = new Kafka2HDFS(args[0]);
    kafka2HDFS.run();
  }
}
