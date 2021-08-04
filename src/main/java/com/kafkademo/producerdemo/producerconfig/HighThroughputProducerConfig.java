package com.kafkademo.producerdemo.producerconfig;

import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Class to hold the properties of high throuput producer configuration, by
 * compressing the messeges/ a batch of messege to the specified algorithm
 * before sending them to kafka broker. Additional settings to set the batch
 * size, and the time to wait to have the messeges to queue in the batch. The
 * batch is sent to kafka broker in one go
 * 
 * @author Rituraj
 *
 */
public final class HighThroughputProducerConfig extends Properties {
	/** Default serial version UID **/
	private static final long serialVersionUID = 1L;
	/** Bootstrap server url **/
	private static final String bootStrapServer = "127.0.0.1:9092";

	public HighThroughputProducerConfig() {
		// Properties of Idempotent producer
		this.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
		this.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		this.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		this.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		this.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		this.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		this.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
		// Additional properties to make that as high throuput producer
		this.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); // specifying on which algorithm it should
																			// compress the msg before sending to kafka
																			// brokerF
		this.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20"); // specifying the max time it should wait to have the
																	// batch of messege
		this.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); // sepcifying the batch
																							// size(the size it supports
																							// for each batch to send
																							// data to kafka broker)
	}
}
