package kafka;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Multi-thread worker to write data from Kafka Steam into HDFS.
 * Created by shengzhao on 9/30/14.
 */
public class Kafka2HDFSWorker implements Runnable {
  private final Logger _logger = LoggerFactory.getLogger(Kafka2HDFSWorker.class);
  private int _threadNum;
  private KafkaStream _kafkaStream;
  private String _hdfs;
  private OutputStream _hdfsOutputStream;
  private BufferedWriter _bufferedWriter;
  private String _hdfsUser;
  private String _localTmpFolder;
  private String _topic;
  private String _hdfsBaseFolder;
  private String _currentFileName;



  public Kafka2HDFSWorker(int _threadNum, KafkaStream _kafkaStream, Properties properties) {
    this._threadNum = _threadNum;
    this._kafkaStream = _kafkaStream;
    this._hdfs = properties.getProperty(Constants.HDFS_HOST);
    this._hdfsUser = properties.getProperty(Constants.HDFS_USER);
    this._localTmpFolder = properties.getProperty(Constants.LOCAL_TMP_FOLDER);
    this._topic = properties.getProperty(Constants.TOPIC);
    this._hdfsBaseFolder = properties.getProperty(Constants.HDFS_BASE_FOLDER);
  }


  public void shutDown() {
    _logger.info("Shutting down worker {}", _threadNum);
    if (_hdfsOutputStream != null) {
      try {
        _logger.info("Closing output stream for worker {}", _threadNum);
        _bufferedWriter.close();
        storeTmpFileInHDFS(_currentFileName);
        File localFile = new File(_localTmpFolder + "/" + _currentFileName);
        localFile.deleteOnExit();
      } catch (IOException e) {
        _logger.error("Failed to close hdfs output stream while cleaning up... {}", e.getMessage());
      } catch (InterruptedException e) {
        _logger.error("InterruptedException: {}", e.getMessage());
      }
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void run() {
    ConsumerIterator<byte[], byte[]> consumerIterator = _kafkaStream.iterator();
    while (consumerIterator.hasNext()) {
      if (Thread.interrupted()) {
        _logger.warn("Interrupted Kafka2HDFSWorker {}", _threadNum);
        shutDown();
      }
      MessageAndMetadata<byte[], byte[]> next = consumerIterator.next();
      try {
        String message = new String(next.message(), "UTF-8");
        _logger.debug("Message: [{}] {}", _threadNum, message);
        _logger.info("Message [{}]: Length {}", _threadNum, message.length());
        if (_hdfsOutputStream == null) {
          _currentFileName = openLocalOutputStream();
          _bufferedWriter = new BufferedWriter(new OutputStreamWriter(_hdfsOutputStream));
        }
        _bufferedWriter.write(message);
        _bufferedWriter.newLine();
      } catch (UnsupportedEncodingException e) {
        _logger.error("Unsupported encoding exception {}", e.getMessage());
      } catch (IOException e) {
        _logger.error("Unable to create HDFS file... {}", e.getMessage());
      }

    }
  }

  /**
   *
   * @throws IOException
   */
  private String openLocalOutputStream() throws IOException {
    File base = new File(_localTmpFolder);
    if (!base.exists()) {
      base.mkdirs();
    }
    String fileName = _topic + "-" + _threadNum + ".dat." + System.currentTimeMillis();
    _hdfsOutputStream = new FileOutputStream(_localTmpFolder + "/" + fileName);

    return fileName;
  }


  private void storeTmpFileInHDFS(final String localFileName) throws IOException, InterruptedException {

    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(_hdfsUser);
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        FileSystem fileSystem = FileSystem.get(URI.create(_hdfs), new Configuration());
        Path filePath = new Path(_hdfsBaseFolder + "/" + _topic + "-" + _threadNum + ".dat");
        final String preferedFileName = filePath.getName();
        Path parentFilePath = filePath.getParent();
        if (fileSystem.exists(parentFilePath)) {
          FileStatus[] fileStatuses = fileSystem.listStatus(parentFilePath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
              if (path.getName().equals(preferedFileName)) {
                // include the preferred file itself
                return true;
              }
              // include the preferred files with file ID as well
              Pattern pattern = Pattern.compile(preferedFileName + "\\.[0-9]+");
              Matcher matcher = pattern.matcher(path.getName());
              return matcher.find();
            }
          });

          if (fileStatuses.length > 0) {
            _logger.info("Preferred file ({}) existing in HDFS.", preferedFileName);
            int suffixFileID = fileStatuses.length;
            for (FileStatus fileStatuse : fileStatuses) {
              int dotIndex = fileStatuse.getPath().getName().lastIndexOf('.');
              if (dotIndex > 0) {
                try {
                  int fileID = Integer.valueOf(fileStatuse.getPath().getName().substring(dotIndex + 1));
                  if (fileID > suffixFileID) {
                    suffixFileID = fileID;
                  }
                  _logger.debug("SuffixFileID set to {}", suffixFileID);
                } catch (NumberFormatException e) {
                  _logger.warn("NumberFormatException: {}", e.getMessage());
                }
              }
            }
            filePath = new Path(parentFilePath.toString() + "/" + preferedFileName + "." + suffixFileID);
            _logger.info("Preferred file set to {}", filePath);
          }
        } else {
          fileSystem.mkdirs(parentFilePath);
        }
        Path localFile = new Path(_localTmpFolder + "/" + localFileName);
        FileSystem.get(URI.create(_hdfs), new Configuration()).copyFromLocalFile(localFile, filePath);
        return null;
      }
    });


  }
}
