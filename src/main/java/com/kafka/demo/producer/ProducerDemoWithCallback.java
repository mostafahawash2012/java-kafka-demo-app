package com.kafka.demo.producer;

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

	public static void main(String[] args) {

		final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
		String bootstrapServers = "127.0.0.1:9092";
		// create producer properties
		Properties props = new Properties();
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

		for (int i = 0; i < 10; i++) {

			// create a producer record
			ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello " + i);

			// send data
			producer.send(record, new Callback() {
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					// executes every time a record is successfully sent or an exception is thrown
					if (exception == null) {
						// the record was successfully sent
						logger.info("Received new metadata \n" + "Topic: " + metadata.topic() + "\n" + "Partition: "
								+ metadata.partition() + "\n" + "Offset: " + metadata.offset() + "\n" + "Timestamp: "
								+ metadata.timestamp());
					} else {
						logger.error("Error while producing: " + exception);
					}
				}
			});
		}
		// flush data
		producer.flush();
		// flush and close
		producer.close();
	}
}
