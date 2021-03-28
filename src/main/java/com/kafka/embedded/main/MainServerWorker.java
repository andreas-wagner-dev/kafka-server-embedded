package com.kafka.embedded.main;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.function.Consumer;

/**
 * Listener thread of client requests.
 */
class MainServerWorker extends Thread {

	private final Socket socket;
	private Consumer<String> listener;

	// Constructor
	public MainServerWorker(Socket socket, Consumer<String> listener) {
		this.socket = socket;
		this.listener = listener;
	}

	@Override
	public void run() {
		DataInputStream inputStream = null;
		DataOutputStream outputStream = null;
		while (true) {
			try {
				// obtaining input and out streams
				inputStream = new DataInputStream(socket.getInputStream());
				outputStream = new DataOutputStream(socket.getOutputStream());
				// receive the answer from client
				listener.accept(inputStream.readUTF());
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				close(inputStream);
				close(outputStream);
			}
		}
	}

	private void close(Closeable closeable) {
		try {
			if (closeable != null) {
				closeable.close();
			}
		} catch (IOException ec) {
			ec.printStackTrace();
		}
	}

}