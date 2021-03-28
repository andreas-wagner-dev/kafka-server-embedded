package com.kafka.embedded;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Properties;
import java.util.UUID;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

/**
 * Class to run an embedded server instance.
 *
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
		this.kafkaProperties = kafkaProperties;
		this.zkProperties = zkProperties;
	}

	static Properties getDefaultKafkaProperties() {
		Properties kafkaProperties = new Properties();
		kafkaProperties.put("broker.id", "0");
		kafkaProperties.put("host.name", "localhost");
		kafkaProperties.put("port", "9092");
		kafkaProperties.put("zookeeper.connect", "localhost");
		kafkaProperties.put("zookeeper.connection.timeout.ms", "6000");
		kafkaProperties.put("advertised.host.name", "localhost");
		kafkaProperties.put("group.initial.rebalance.delay.ms", "0");
		kafkaProperties.put("offsets.topic.replication.factor", "1");
		return kafkaProperties;
	}

	static Properties getDefaultZookeeperProperties() {
		Properties zkProperties = new Properties();
		zkProperties.put("clientPort", "2181");
		zkProperties.put("maxClientCnxns", "0");
		return zkProperties;
	}

	/**
	 * Starts a Kafka server instance. A ZooKepper server instance is required for
	 * Kafka and gets automatically started before Kafka.
	 *
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public void startup() throws FileNotFoundException, IOException {
		// Clean kafka temporary work directory before running
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
		// Start ZooKeeper before Kafka
		startupZookeeper();
		// Kafka temporary directory
		File kafkaTmpDir = new File(System.getProperty("java.io.tmpdir"),
				KAFKA_RELATIVE_WORKDIR + "/kafka_log-" + UUID.randomUUID());
		kafkaTmpDir.mkdir();
		String tempPath = kafkaTmpDir.getAbsolutePath();
		System.out.println("Embedded Kafka temporary work directory: " + tempPath);
		// Load Kafka Properties
		kafkaProperties.put("log.dirs", tempPath);
		kafkaProperties.put("log.dir", tempPath);
		kafkaProperties.put("kafka.logs.dir", tempPath);
		// Set System Properties required by Kafka
		System.setProperty("kafka.logs.dir", tempPath);
		KafkaConfig config = new KafkaConfig(kafkaProperties);
		kafkaServer = new KafkaServerStartable(config);
		kafkaServer.startup();
		running = true;
	}

	/**
	 * Starts a ZooKeeper instance.
	 *
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private void startupZookeeper() throws FileNotFoundException, IOException {
		// ZooKeeper temporary directory
		File zookeeperTmpDir = new File(System.getProperty("java.io.tmpdir"),
				KAFKA_RELATIVE_WORKDIR + "/tmp_zookeeper");
		// Create the folder
		zookeeperTmpDir.mkdir();
		// Load ZooKeeper Properties
		// Override the data directory in the loaded ZooKeeper Properties
		zkProperties.put("dataDir", zookeeperTmpDir.getAbsolutePath());
		// Convert Properties to ZooKeeper config
		QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
		try {
			quorumConfiguration.parseProperties(zkProperties);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		final ServerConfig configuration = new ServerConfig();
		configuration.readFrom(quorumConfiguration);
		// Start ZooKeeper in a Thread
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
	 * Shutdown and cleanup of Kafka. Also deletes temporary folders.
	 *
	 * @throws IOException
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

	/**
	 * Cleans a directory without deleting it.
	 *
	 * @param directory directory to clean
	 * @throws IOException in case cleaning is unsuccessful
	 */
	public static void cleanDirectory(File directory) throws IOException {
		Files.walkFileTree(directory.toPath(), new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
				Files.delete(dir);
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				Files.delete(file);
				return FileVisitResult.CONTINUE;
			}
		});
	}

}
