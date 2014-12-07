package kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * The housekeeping class will be hooked to the shutdown process to make sure the process
 * can complete gracefully.
 * Created by shengzhao on 10/2/14.
 */
public class Kafka2HDFSHouseKeeper implements Runnable {
  private Logger _logger = LoggerFactory.getLogger(Kafka2HDFSHouseKeeper.class);
  private Kafka2HDFS _kafka2HDFS;
  private List<Kafka2HDFSWorker> _workerList;

  public Kafka2HDFSHouseKeeper(Kafka2HDFS _kafka2HDFS, List<Kafka2HDFSWorker> list) {
    this._kafka2HDFS = _kafka2HDFS;
    this._workerList = list;
  }

  @Override
  public void run() {
    _logger.info("Housekeeping started...");
    _kafka2HDFS.shutDown();
    _logger.info("Shutting down {} workers.", _workerList.size());
    for (Kafka2HDFSWorker kafka2HDFSWorker : _workerList) {
      kafka2HDFSWorker.shutDown();
    }
    _logger.info("Housekeeping finished...");
  }
}
