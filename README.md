# embedded-kafka-server

Demonstration how to the all components (producer, cunsumer, inculuding server) of Kafka API embedded in a single java progaram.

```
package com.kafka.embedded;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * This class is a demonstration on how to use the EmbeddedKafkaServer class.
 */
public class EmbeddedKafkaServerTest {

	private static final String BOOTSTRAP_SERVERS = "localhost:9092";
	private final static String TOPIC = "my-example-topic";
	private EmbeddedKafkaServer kafkaServer = null;

	public static void main(String[] args) throws Exception {
		new EmbeddedKafkaServerTest().test();
	}

	/**
	 * Sample test
	 *
	 * @throws Exception
	 */
	public void test() throws Exception {
		startupServer();
		// create and publish a message with the producer
		produce(TOPIC, "test message");
		// wait for
		sleep(3);
		// subscribe
		Consumer<String, String> subscriber = subscribe(TOPIC);
		// create and subscribe a message with the consumer
		consume(subscriber);
		sleep(3);
		shutdownServer();
		sleep(3);
		System.out.println("Test execution finished.");
	}

	private void sleep(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
		}
	}

	private static Producer<String, String> createProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return new KafkaProducer<>(props);
	}

	static void produce(String topic, String value) throws Exception {
		final Producer<String, String> producer = createProducer();
		try {
			long size = 5;
			for (int i = 0; i < size; i++) {
				final ProducerRecord<String, String> record = new ProducerRecord<>(topic, String.valueOf(i), value);
				RecordMetadata metadata = producer.send(record).get();
				System.out.printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) time=%d\n",
						record.key(), record.value(), metadata.partition(), metadata.offset(),
						System.currentTimeMillis());
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			producer.flush();
			producer.close();
		}
	}

	static Consumer<String, String> createConsumer() {
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		return new KafkaConsumer<>(props);
	}

	static Consumer<String, String> subscribe(String topic) throws InterruptedException {
		// Create the consumer using props
		Consumer<String, String> consumer = createConsumer();
		// Subscribe to the topic.
		consumer.subscribe(Collections.singletonList(topic));
		return consumer;
	}

	static void consume(Consumer<String, String> consumer) throws InterruptedException {
		final int giveUp = 100;
		int noRecordsCount = 0;
		while (true) {
			ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
			if (consumerRecords.count() == 0) {
				noRecordsCount++;
				if (noRecordsCount > giveUp) {
					break;
				} else {
					continue;
				}
			}

			int recordCount = consumerRecords.count();
			for (ConsumerRecord<String, String> record : consumerRecords) {
				recordCount++;
				System.out.printf("Consumer Record:(%s, %s, %d, %d)\n", record.key(), record.value(),
						record.partition(), record.offset());
			}
			if (recordCount > 0) {
				break;
			}
			consumer.commitSync();
		}
		consumer.close();
		System.out.println("DONE");
	}

	/**
	 * Starts server should be called manually.
	 *
	 * @throws IOException
	 */
	public void startupServer() throws IOException {
		if (kafkaServer == null) {
			System.out.println("Starting Kafka.");
			kafkaServer = new EmbeddedKafkaServer();
			kafkaServer.startup();
			if (kafkaServer.isRunning()) {
				System.out.println("Kafka started.");
			} else {
				throw new IOException("Kafka do not start properly. Please restart.");
			}
		}
	}

	/**
	 * Stops server should be called manually.
	 *
	 * @throws IOException
	 */
	public void shutdownServer() throws IOException {
		if (kafkaServer != null && kafkaServer.isRunning()) {
			System.out.println("Stopping Kafka.");
			kafkaServer.shutdown();
			System.out.println("Kafka stopped.");
		}
	}

}
```
