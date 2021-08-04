package com.kafkademo.twitterProject;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.kafkademo.producerdemo.ProducerDemo;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

/**
 * Class to have twitter data extracted from twitter developer's APIs and
 * publish those data to kafka
 * 
 * @author Rituraj
 *
 */
public class TwitterProducer {

	/** Logger for TwitterProducer **/
	Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
	/** consumer key for twitter developer's APIs **/
	public static final String consumerKey = "DRZqAkg5XYiFumjeHWdWQ6mv6";
	/** consumer secret key for twitter developer's APIs **/
	public static final String consumerSecret = "2mBj0mfC7ri6nH5lADjKLEOJvqqyM2VNsTmIIAxxgPSlbdawN4";
	/** token for twitter developer's APIs **/
	public static final String token = "1422434909684060165-fhfUNlHSpUCWYImdgTyzhrYl44Web7";
	/** secret for twitter developer's APIs **/
	public static final String secret = "fvZyyaCBuM0Vujuunn2d2Skno91wFfoiV5ekAujxpTpvK";
	/** ProducerDemo object which published data to kafka **/
	public ProducerDemo producerDemo;
	/** queue to hold received data from twitter client with a capacity of 1000 **/
	private BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

	public TwitterProducer() {
		producerDemo = ProducerDemo.getKafkaProducer();
	}

	/**
	 * Main method to initiate the application
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		new TwitterProducer().run();
	}

	/**
	 * Entry Point to the application's functionality, orchestration of functional
	 * blocks done here
	 */
	public void run() {
		// Connecting to twitter client
		Client client = connectToTwitterClient();
		// Publishing the received data from twitter into kafka
		pubishDataToKafka(client, msgQueue);
		logger.info("Application completed");
	}

	/**
	 * Method to create twitter client, Declare the host you want to connect to, the
	 * endpoint, and authentication
	 * 
	 * @param msgQueue
	 * @return Client object
	 */
	public Client createTwitterClient(BlockingQueue<String> msgQueue) {

		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

		// Optional: set up some followings and track terms
		List<String> terms = Lists.newArrayList("bitcoin");
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);
		ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01") // optional: mainly for the logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));
		Client hosebirdClient = builder.build();
		return hosebirdClient;
	}

	/**
	 * Method to publish tweets into kafka sending through the Producer, after
	 * extracting tweets from Blocking queue
	 * 
	 * @param client
	 * @param msgQueue
	 */
	private void pubishDataToKafka(Client client, BlockingQueue<String> msgQueue) {
		// loop to extract tweets from blockingQueue and send to kafka
		while (!client.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException exception) {
				exception.printStackTrace();
				client.stop();
			}
			if (msg != null) {
				producerDemo.produceMsg(msg);
				logger.info(msg);
			}
		}
	}

	/**
	 * Method to connect to the twitter client
	 * 
	 * @return Client object
	 */
	private Client connectToTwitterClient() {
		Client client = createTwitterClient(this.msgQueue);
		client.connect();
		return client;
	}

}
