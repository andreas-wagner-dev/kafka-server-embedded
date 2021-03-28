package com.kafka.embedded;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

import com.kafka.embedded.main.MainServer;

/**
 * Class to run an embedded server instance.
 */
public class EmbeddedKafkaMain {

	private static EmbeddedKafkaServer embeddedKafkaServer;

	public static void main(String[] args) {
		String clientPort = getArg("-client-port", args);
		if (clientPort != null) {
			Integer port = Integer.valueOf(clientPort);
			if (!MainServer.isPortInUse(port)) {
				MainServer cmdServer = new MainServer();
				try {
					cmdServer.start(port, cmd -> {
						switch (cmd) {
						case "start":
							startupKafkaServer(args);
							break;
						case "stop":
							shutdownKafkaServer();
							// close this server
							cmdServer.close();
							break;
						default:
							break;
						}
					});
				} catch (IOException e) {
					e.printStackTrace();
				}
			} else {
				System.out.println("This server port " + port + " is in use.");
			}
		}
	}

	private static void startupKafkaServer(String[] args) {
		Properties defaultKafkaProperties = EmbeddedKafkaServer.getDefaultKafkaProperties();
		Properties defaultZookeeperProperties = EmbeddedKafkaServer.getDefaultZookeeperProperties();
		try {
			String kafkaPropertiesFile = getArg("-kafka.properties", args);
			if (kafkaPropertiesFile != null) {
				Properties kafkaProperties = loadProperties(kafkaPropertiesFile);
				defaultKafkaProperties.putAll(kafkaProperties);
			}
			String zkPropertiesFile = getArg("-zookeeper.properties", args);
			if (zkPropertiesFile != null) {
				Properties zookeeperProperties = loadProperties(zkPropertiesFile);
				defaultZookeeperProperties.putAll(zookeeperProperties);
			}
			embeddedKafkaServer = new EmbeddedKafkaServer(defaultKafkaProperties, defaultZookeeperProperties);
			embeddedKafkaServer.startup();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void shutdownKafkaServer() {
		if (embeddedKafkaServer != null && embeddedKafkaServer.isRunning()) {
			try {
				embeddedKafkaServer.shutdown();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private static Properties loadProperties(String propertiesFile) throws IOException {
		Properties properties = new Properties();
		FileInputStream inStream = new FileInputStream(propertiesFile);
		properties.load(inStream);
		inStream.close();
		return properties;
	}

	private static String getArg(String name, String... args) {
		if (args != null) {
			for (int i = 0; i < args.length; i++) {
				if (Objects.equals(name, args[i]) && i + 1 < args.length) {
					return args[i + 1];
				}
			}
		}
		return null;
	}
}