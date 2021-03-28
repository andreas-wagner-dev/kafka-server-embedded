package com.kafka.embedded;

import org.apache.zookeeper.server.ZooKeeperServerMain;

/**
 * This class starts and runs a stand alone ZooKeeperServer.
 */
public class EmbeddedZooKeeperServer extends ZooKeeperServerMain {

	@Override
	public void shutdown() {
		super.shutdown();
	}
}
