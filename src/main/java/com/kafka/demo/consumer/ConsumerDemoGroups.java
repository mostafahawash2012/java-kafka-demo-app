package com.kafka.demo.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoGroups {

	public static void main(String[] args) {

		final Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups.class);
		String bootstrapServers = "127.0.0.1:9092";
		String groupId = "my-fourth-application";
		String topic = "first_topic";

		// create consumer properties
		Properties props = new Properties();
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // earliest/latest

		// create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

		// subscribe consumer to our topic(s)
		consumer.subscribe(Arrays.asList(topic));
		// poll new data

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

			for (ConsumerRecord<String, String> record : records) {
				logger.info("key: " + record.key() + ", value: " + record.value());
				logger.info("partition: " + record.partition() + ", offset: " + record.offset() );
			}
		}
		
	}
}
