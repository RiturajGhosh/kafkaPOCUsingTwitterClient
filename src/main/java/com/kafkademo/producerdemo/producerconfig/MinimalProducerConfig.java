package com.kafkademo.producerdemo.producerconfig;

import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * MinimalProducerConfig holds the minimum configs to set the kafka producer
 * 
 * @author Rituraj
 *
 */
public final class MinimalProducerConfig extends Properties {

	/** Default serial version UID **/
	private static final long serialVersionUID = 1L;
	/** Bootstrap server url **/
	private static final String bootStrapServer = "127.0.0.1:9092";

	public MinimalProducerConfig() {
		this.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
		this.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		this.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	}

}
