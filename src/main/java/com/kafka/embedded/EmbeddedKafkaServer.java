package com.kafka.embedded;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

/**
 * Embedded KAFKA server.
 */
public class EmbeddedKafkaServer {

	private static final String KAFKA_RELATIVE_WORKDIR = "embedded_kafka";

	private boolean running;
	private Properties zkProperties;
	private EmbeddedZooKeeperServer zooKeeperServer;
	private Thread zooKeeperServerThread;
	private Properties kafkaProperties;
	private KafkaServerStartable kafkaServer;

	public EmbeddedKafkaServer() {
		this(getDefaultKafkaProperties(), getDefaultZookeeperProperties());
	}

	public EmbeddedKafkaServer(Properties kafkaProperties) {
		this(kafkaProperties, getDefaultZookeeperProperties());
	}

	public EmbeddedKafkaServer(Properties kafkaProperties, Properties zkProperties) {
		this.kafkaProperties	= kafkaProperties;
		this.zkProperties		= zkProperties;
	}

	static Properties getDefaultKafkaProperties() {
		// https://docs.confluent.io/platform/current/installation/configuration/broker-configs.html
		// https://github.com/apache/kafka/blob/trunk/config/server.properties
		Properties kafkaProperties = new Properties();
		kafkaProperties.put("broker.id", "0");
		kafkaProperties.put("host.name", "localhost");
		kafkaProperties.put("port", "9092");
		kafkaProperties.put("zookeeper.connect", "localhost:2181");
		kafkaProperties.put("zookeeper.connection.timeout.ms", "6000");
		kafkaProperties.put("advertised.host.name", "localhost");
		kafkaProperties.put("group.initial.rebalance.delay.ms", "0");
		kafkaProperties.put("offsets.topic.replication.factor", "1");
		return kafkaProperties;
	}

	static Properties getDefaultZookeeperProperties() {
		// https://github.com/apache/kafka/blob/trunk/config/zookeeper.properties
		Properties zkProperties = new Properties();
		zkProperties.put("clientPort", "2181");
		zkProperties.put("maxClientCnxns", "0");
		return zkProperties;
	}

	/**
	 * Starts a KAFKA server and automatically a ZooKepper server instance which is
	 * required to run before started KAFKA.
	 *
	 * @throws IOException in case startup is unsuccessful
	 */
	public void startup() throws IOException {
		// Clean KAFKA temporary work directory before running
		File kafkaTmpRootDir = new File(System.getProperty("java.io.tmpdir"), KAFKA_RELATIVE_WORKDIR);
		if (kafkaTmpRootDir.exists()) {
			try {
				cleanDir(kafkaTmpRootDir);
			} catch (IOException e) {
				System.err.println("Embedded Kafka : Unable to clean work directory, maybe Kafka is already running?");
				throw (e);
			}
		} else {
			kafkaTmpRootDir.mkdir();
		}
		// start ZooKeeper before KAFKA
		startupZookeeper();
		// KAFKA temporary directory
		File kafkaTmpDir = new File(
				System.getProperty("java.io.tmpdir"),
				KAFKA_RELATIVE_WORKDIR + "/kafka_log-" + UUID.randomUUID()
		);
		kafkaTmpDir.mkdir();
		String tempPath = kafkaTmpDir.getAbsolutePath();
		System.out.println("Embedded Kafka temporary work directory: " + tempPath);
		// load KAFKA properties
		kafkaProperties.put("log.dirs", tempPath);
		kafkaProperties.put("log.dir", tempPath);
		kafkaProperties.put("kafka.logs.dir", tempPath);
		// set system properties required by KAFKA
		System.setProperty("kafka.logs.dir", tempPath);
		KafkaConfig config = new KafkaConfig(kafkaProperties);
		kafkaServer = new KafkaServerStartable(config);
		kafkaServer.startup();
		running = true;
	}

	/**
	 * Starts a ZooKeeper instance.
	 *
	 * @throws IOException in case startup is unsuccessful
	 */
	private void startupZookeeper() throws IOException {
		// ZooKeeper temporary directory
		File zookeeperTmpDir = new File(
				System.getProperty("java.io.tmpdir"),
				KAFKA_RELATIVE_WORKDIR + "/tmp_zookeeper"
		);
		// create the folder
		zookeeperTmpDir.mkdir();
		// load ZooKeeper properties
		// override the data directory in the loaded ZooKeeper properties
		zkProperties.put("dataDir", zookeeperTmpDir.getAbsolutePath());
		// convert properties to ZooKeeper configuration
		QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
		try {
			quorumConfiguration.parseProperties(zkProperties);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		ServerConfig configuration = new ServerConfig();
		configuration.readFrom(quorumConfiguration);
		// start ZooKeeper in a Thread
		zooKeeperServer = new EmbeddedZooKeeperServer();
		new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					zooKeeperServer.runFromConfig(configuration);
				} catch (IOException e) {
					System.err.println("ZooKeeper failed to start");
					running = false;
					e.printStackTrace(System.err);
				}
			}
		}).start();
	}

	/**
	 * Shutdown and cleanup of KAFKA. Also deletes temporary folders.
	 *
	 * @throws IOException in case shutdown is unsuccessful
	 */
	public void shutdown() throws IOException {
		kafkaServer.shutdown();
		kafkaServer.awaitShutdown();
		zooKeeperServer.shutdown();
		running = false;
		if (zooKeeperServerThread != null && !zooKeeperServerThread.isInterrupted()) {
			zooKeeperServerThread.interrupt();
		}
	}

	/**
	 * Returns whether Kafka is up and running
	 *
	 * @return
	 */
	public boolean isRunning() {
		return running;
	}

	/**
	 * Cleans a directory without deleting it.
	 *
	 * @param directoryToBeCleaned directory to clean
	 * @throws IOException in case cleaning is unsuccessful
	 */
	void cleanDir(File directoryToBeCleaned) throws IOException {
		File[] allContents = directoryToBeCleaned.listFiles();
		if (allContents != null) {
			for (File file : allContents) {
				if (!deleteDirectory(file)) {
					throw new IOException("Failed to delete file " + file);
				}
			}
		}
	}

	boolean deleteDirectory(File directoryToBeDeleted) {
		File[] allContents = directoryToBeDeleted.listFiles();
		if (allContents != null) {
			for (File file : allContents) {
				deleteDirectory(file);
			}
		}
		return directoryToBeDeleted.delete();
	}

}
