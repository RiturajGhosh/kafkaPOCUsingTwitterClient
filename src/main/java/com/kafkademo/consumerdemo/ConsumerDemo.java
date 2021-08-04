package com.kafkademo.consumerdemo;

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

/**
 * Consumer
 * 
 * @author Rituraj
 *
 */
public class ConsumerDemo {

	public static void main(String[] args) {

		Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
		String bootStrapServer = "127.0.0.1:9092";
		String topicName = "first_topic";
		String groupId = "my-first-application";
		// create consumer config
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		// create kafka consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

		// subscribe to consumer topics
		consumer.subscribe(Arrays.asList(topicName));

		// poll for new data
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			for (ConsumerRecord<String, String> record : records) {
				logger.info("\n key: " + record.key() + "\n" + "value: " + record.value() + "\n" + "partition: "
						+ record.partition() + "\n offset: " + record.offset());
			}

		}

	}

}
