package com.kafka.demo.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {

	public static void main(String[] args) {

		String bootstrapServers = "127.0.0.1:9092";
		// create producer properties
		Properties props = new Properties();
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

		// create a producer record
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello hawash");

		// send data
		producer.send(record); 
		
		// flush data
		producer.flush();
		// flush and close 
		producer.close();
	}
}
