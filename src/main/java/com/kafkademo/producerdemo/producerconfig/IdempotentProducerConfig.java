package com.kafkademo.producerdemo.producerconfig;

import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * IdempotentProducerConfig class holds the properties of a safe producer. For
 * network latency if the ack does not get to the producer from kafka broker but
 * sent data is committed, then when producer again publish the same messge in
 * order to not loose the data, a duplicate data is committed in the kafka
 * broker, to mitigate this problem the idempotence config set to true along
 * with to make it a safe producer acks config is set to true, means from all
 * the ISR brokers ack is needed to the producer, retries config is set to max
 * int so that if producer does not get ack it will try max int number of time
 * to send the data to broker, max in flight request per connection is set to 5
 * 
 * @author Rituraj
 *
 */
public final class IdempotentProducerConfig extends Properties {

	/** Default serial version UID **/
	private static final long serialVersionUID = 1L;
	/** Bootstrap server url **/
	private static final String bootStrapServer = "127.0.0.1:9092";

	public IdempotentProducerConfig() {
		this.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
		this.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		this.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		this.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		this.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		this.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		this.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
	}

}
