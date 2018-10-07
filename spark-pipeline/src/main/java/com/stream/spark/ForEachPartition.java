/**
 * 
 */
package com.stream.spark;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;

/**
 * @author anumishr0
 *
 */
public class ForEachPartition implements Serializable {
	private final Logger log = LoggerFactory.getLogger(getClass());
	private static JsonParser jsonParser = new JsonParser();	
	BulkRequestChild bulkRequest = new BulkRequestChild();

	private Map<String, Object> consumerConfigs() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "spark-group-job");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
		return props;
	}

	public static void main(String args[]) {
		ForEachPartition sparkConsumerService = new ForEachPartition();
		sparkConsumerService.run();
	}

	private static String extractIdFromTweet(String tweetJson) {
		// gson library
		return jsonParser.parse(tweetJson).getAsJsonObject().get("id_str").getAsString();
	}

	public KafkaConsumer<String, String> createConsumer(String topic) {

		// create consumer configs
		Properties properties = new Properties();
		properties.putAll(consumerConfigs());

		// create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		consumer.subscribe(Arrays.asList(topic));
		return consumer;
	}

	private RestHighLevelClient createClient() {

		// https://lakesqs2hn:jrg1pfxmvb@twitter-stream-4407600727.eu-west-1.bonsaisearch.net

		// credential of twitter
		String hostname = "twitter-stream-4407600727.eu-west-1.bonsaisearch.net";
		String username = "lakesqs2hn";
		String password = "jrg1pfxmvb";

		// remove credentialsProvider if you run a local ES
		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

		RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
					@Override
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
					}
				});

		RestHighLevelClient client = new RestHighLevelClient(builder);
		return client;
	}
	

	public void run() {
		log.info("Running Spark Consumer Service..");

		Collection<String> topics = Arrays.asList("twitter_tweets");

		JavaStreamingContext context = new JavaStreamingContext("local[*]", "JavaDirectKafka-1", Durations.seconds(10));

		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(context,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, consumerConfigs()));		
		
		stream.foreachRDD(rdd -> {
			 rdd.foreachPartition(partitionOfRecords -> {
				   BulkRequest bulkRequest = new BulkRequest();
				   KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");
				   RestHighLevelClient restHighLevelClient = createClient();
				   int counter = 0;
				    while (partitionOfRecords.hasNext()) {				    	
				      ConsumerRecord<String, String> consumerRecord = partitionOfRecords.next();
				     
				  	   String id = extractIdFromTweet(consumerRecord.value());
						
						IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id //
						
						).source(consumerRecord.value(), XContentType.JSON);
			
						if(counter <=4) {
						bulkRequest.add(indexRequest);	
						}
						counter++;
				    }
//				    if (recordCount > 0) {
		                BulkResponse bulkItemResponses = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);		               
		                consumer.commitSync();
		               try {
		                    Thread.sleep(1000);
		                } catch (InterruptedException e) {
		                    e.printStackTrace();
		                }
		            //}
				  });
		});


		// Start the computation
		context.start();

		try {
			context.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
