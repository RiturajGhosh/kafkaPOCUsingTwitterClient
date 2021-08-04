package com.kafkademo.producerdemo;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ProducerCallBack class implements Callback interface and ovverride the
 * onCompletion method which gets called as producer call back if the object of
 * this class is passed into send method of Kafka Producer
 * 
 * @author Rituraj
 *
 */
public class ProducerCallback implements Callback {

	/** Logger for the ProducerCallBack class **/
	private static final Logger logger = LoggerFactory.getLogger(ProducerCallback.class);

	public ProducerCallback() {

	}

	/**
	 * Method to have functionalities on completion of publishing data to kafka
	 */
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		if (exception == null) {
			logger.info("Received new metadata with \n" + "topic name: " + metadata.topic() + "\n" + "partition name: "
					+ metadata.partition() + "\n" + "offset: " + metadata.offset() + "\n" + "timestamp: "
					+ metadata.timestamp());
		} else {
			logger.error(exception.getStackTrace().toString());
		}

	}

}
