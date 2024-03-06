# embedded-kafka-server

Demonstration how to use all components of Kafka API (producer, cunsumer, including server) embedded in a single java programm.

```java
package com.kafka.embedded;

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

	private final String LOCALHOST_BOOTSTRAP_SERVERS = "localhost:9092";

	private final String TEST_CLIENT_ID_CONFIG = "KafkaExampleProducer";
	private final String TEST_GROUP_ID_CONFIG = "KafkaExampleConsumer";

	private final String TEST_TOPIC = "test-topic";
	private final String TEST_TOPIC_KEY = "test-key";

	public static void main(String[] args) throws Exception {
		new EmbeddedKafkaServerTest().test();
	}

	/**
	 * Sample test
	 *
	 * @throws Exception
	 */
	public void test() throws Exception {
		System.out.println("Starting Kafka Server.");
		EmbeddedKafkaServer kafkaServer = new EmbeddedKafkaServer();
		kafkaServer.startup();
		System.out.println("Kafka Server is started.");
		// create and publish a message with the producer
		System.out.println("Initialize Kafka Producer.");
		Producer<String, String> producer = createProducer();
		ProducerRecord<String, String> message = new ProducerRecord<>(TEST_TOPIC, TEST_TOPIC_KEY, "test message");
		RecordMetadata metadata = producer.send(message).get();
		System.out.printf("Produce record to: %s:(%s)\n", metadata.topic(), message.value());
		// blocks until all sends are complete
		producer.flush();
		System.out.println("Release Kafka Consumer.");
		producer.close();
		// wait for
		sleep(3);
		System.out.println("Initialize Kafka Consumer.");
		// Create the consumer using props
		Consumer<String, String> consumer = createConsumer();
		System.out.println("Subscribe Kafka Consumer.");
		// subscribe to the topic
		consumer.subscribe(Collections.singletonList(TEST_TOPIC));
		// consume from the topic until record count <= 0
		int recordCount = 0;
		while (recordCount <= 0) {
			// On each poll, consumer will try to use the last consumed offset as the
			// starting offset and fetch sequentially.
			ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
			recordCount = consumerRecords.count();
			for (ConsumerRecord<String, String> record : consumerRecords) {
				System.out.printf(
						"Consume record from %s:(%s, %s, %d, %d)\n",
						record.topic(),
						record.key(),
						record.value(),
						record.partition(),
						record.offset()
				);
			}
			// This is a synchronous commits and will block
			// until either the commit succeeds or an unrecoverable
			// error is encountered (in which case it is thrown to the caller).
			consumer.commitSync();
		}
		System.out.println("Release Kafka Consumer.");
		consumer.close();
		sleep(3);
		// shutdown server();
		System.out.println("Stopping Kafka.");
		kafkaServer.shutdown();
		System.out.println("Kafka stopped.");
		sleep(3);
		System.out.println("Test execution finished.");
	}

	private void sleep(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {}
	}

	private Producer<String, String> createProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, LOCALHOST_BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, TEST_CLIENT_ID_CONFIG);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return new KafkaProducer<>(props);
	}

	private Consumer<String, String> createConsumer() {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, LOCALHOST_BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, TEST_GROUP_ID_CONFIG);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		return new KafkaConsumer<>(props);
	}

}

```
