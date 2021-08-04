package com.kafkademo.producerdemo;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kafkademo.producerdemo.producerconfig.IdempotentProducerConfig;

/**
 * ProducerDemo is a singletone class which can publish data to a kafka topic to
 * the server 127.0.0.1:9092
 * 
 * @author Rituraj
 *
 */
public class ProducerDemo {

	/** ProducerDemo object **/
	private static ProducerDemo producerDemo = null;
	/** Bootstrap server url **/
	private static final String bootStrapServer = "127.0.0.1:9092";
	/** Properties object that will hold config to connect to kafka server **/
	private Properties properties;
	/** Kafka producer **/
	private KafkaProducer<String, String> producer;
	/** Logger for the ProducerDemo class **/
	private static final Logger logger = LoggerFactory.getLogger(ProducerDemo.class);

	/**
	 * Private constructur to fecilitate this class as singletone
	 */
	private ProducerDemo() {
		properties = new IdempotentProducerConfig();
		// setKafkaProducerProperties();
		this.producer = new KafkaProducer<String, String>(properties);

	}

	/**
	 * Method to publish messeges to kafka
	 * 
	 * @param msg
	 */
	public void produceMsg(String msg) {
		try {
			ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", msg);
			this.producer.send(record, new ProducerCallback());
			producer.flush();
		} catch (Exception kafkaException) {
			logger.error("Failed to send messege to kafka");
			logger.error(kafkaException.toString());
			producer.close();
		}
	}

	/**
	 * Static method to get the ProducerDemo object, ProducerDemo class being a
	 * singletone, this method returns only single copy of the object irrespective
	 * of number of times it is called and from any place
	 */
	public static ProducerDemo getKafkaProducer() {
		ProducerDemo producerDemoObj;
		if (producerDemo != null) {
			producerDemoObj = producerDemo;
		} else {
			producerDemoObj = new ProducerDemo();
		}
		return producerDemoObj;
	}
}
