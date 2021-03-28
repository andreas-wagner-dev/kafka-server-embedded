package com.kafka.embedded.main;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.function.Consumer;

/**
 * Command listener Server.
 */
public class MainServer {

	private boolean running;

	private ServerSocket server;
	private Thread worker;

	public void start(int port, Consumer<String> commandListener) throws IOException {
		server = new ServerSocket(port);
		runServer(server, commandListener);
	}

	public boolean close() {
		running = false;
		return closeServer(server);
	}

	private void runServer(ServerSocket server, Consumer<String> commandListener) throws IOException {
		// running infinite loop for
		while (running) {
			try {
				// starts receiving incoming client requests
				worker = new MainServerWorker(server.accept(), commandListener);
				worker.start();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private boolean closeServer(ServerSocket server) {
		if (!worker.isInterrupted()) {
			worker.interrupt();
		}
		try {
			server.close();
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

	public static boolean isPortInUse(int port) {
		try {
			// ServerSocket try to open a LOCAL port
			new ServerSocket(port).close();
			// local port can be opened, it's available
			return false;
		} catch (Exception e) {
			// local port cannot be opened, it's in use
			return true;
		}
	}

}