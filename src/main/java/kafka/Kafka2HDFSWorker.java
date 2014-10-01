package kafka;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;

/**
 *
 * Created by shengzhao on 9/30/14.
 */
public class Kafka2HDFSWorker implements Runnable {
  private final Logger _logger = LoggerFactory.getLogger(Kafka2HDFSWorker.class);
  private int _threadNum;
  private KafkaStream _kafkaStream;


  public Kafka2HDFSWorker(int _threadNum, KafkaStream _kafkaStream) {
    this._threadNum = _threadNum;
    this._kafkaStream = _kafkaStream;
  }

  @Override
  public void run() {
    ConsumerIterator<byte[], byte[]> consumerIterator = _kafkaStream.iterator();
    while (consumerIterator.hasNext()) {
      if (Thread.interrupted()) {
        _logger.warn("Interrupted Kafka2HDFSWorker {}", _threadNum);
      }
      MessageAndMetadata<byte[], byte[]> next = consumerIterator.next();
      try {
        _logger.info("Message: [{}] {}", _threadNum, new String(next.message(), "UTF-8"));
      } catch (UnsupportedEncodingException e) {
        _logger.error("Unsupported encoding exception {}", e.getMessage());
      }
    }
  }
}
