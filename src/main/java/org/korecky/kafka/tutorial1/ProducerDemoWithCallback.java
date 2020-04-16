package org.korecky.kafka.tutorial1;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallback {
	private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

	public static void main(String[] args) {
		// Create Producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// Create the Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		// Create a producer record
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic",
				"hello world");

		// Send data - asynchronous
		producer.send(record, new Callback() {
			@Override public void onCompletion(RecordMetadata recordMetadata, Exception e) {
				if (e == null) {
					LOGGER.info("Received new metadata.\n"
									+ "Topic: {}\n"
									+ "Partition: {}\n"
									+ "Offset: {}\n"
									+ "Timestamp: {}",
							recordMetadata.topic(),
							recordMetadata.partition(),
							recordMetadata.offset(),
							recordMetadata.timestamp());
				} else {
					LOGGER.error("Error while producing.", e);
				}
			}
		});

		// Flush data
		producer.flush();

		// Close Producer
		producer.close();
	}
}
