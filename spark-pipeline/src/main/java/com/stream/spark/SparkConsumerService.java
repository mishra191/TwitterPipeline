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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
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
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.ImmutableMap;

import com.google.gson.JsonParser;

public class SparkConsumerService implements Serializable {
	private final Logger log = LoggerFactory.getLogger(getClass());
	private static JsonParser jsonParser = new JsonParser();	
	private String hostname = "twitter-stream-4407600727.eu-west-1.bonsaisearch.net";
	private String username = "lakesqs2hn";
	private String password = "jrg1pfxmvb";
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
		SparkConsumerService sparkConsumerService = new SparkConsumerService();
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
	
//	 public static KafkaConsumer<String, String> createConsumer(String topic){
//
//	        String bootstrapServers = "127.0.0.1:9092";
//	        String groupId = "kafka-demo-elasticsearch";
//
//	        // create consumer configs
//	        Properties properties = new Properties();
//	        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//	        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//	        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//	        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
//	        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//	        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable auto commit of offsets
//	        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100"); // disable auto commit of offsets
//
//	        // create consumer
//	        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
//	        consumer.subscribe(Arrays.asList(topic));
//
//	        return consumer;
//
//	    }
	
//	public static RestHighLevelClient createClient(){
//
//        // replace with your own credentials
//        String hostname = ""; // localhost or bonsai url
//        String username = ""; // needed only for bonsai
//        String password = ""; // needed only for bonsai
//
//        // remove credentialsProvider if you run a local ES
//        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
//        credentialsProvider.setCredentials(AuthScope.ANY,
//                new UsernamePasswordCredentials(username, password));
//
//        RestClientBuilder builder = RestClient.builder(
//                new HttpHost(hostname, 443, "https"))
//                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
//                    @Override
//                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
//                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
//                    }
//                });
//
//        RestHighLevelClient client = new RestHighLevelClient(builder);
//        return client;
//    }

	public void run() {
		log.info("Running Spark Consumer Service..");

		Collection<String> topics = Arrays.asList("twitter_tweets");

		// KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");
		//
		// BulkRequest bulkRequest = new BulkRequest();

		JavaStreamingContext context = new JavaStreamingContext("local[*]", "JavaDirectKafka-1", Durations.seconds(10));

		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(context,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, consumerConfigs()));

		// System.out.println(stream.c);

		// JavaDStream<ConsumerRecord<String, String>> javaDStream = stream.map(new
		// Function<ConsumerRecord<String, String>, ConsumerRecord<String, String>>() {
		//
		// private static final long serialVersionUID = 6272424972267329328L;
		// @Override
		// public ConsumerRecord<String, String> call(ConsumerRecord<String, String>
		// record) {
		// System.out.println("record------------------------------------"+
		// record.value());
		// id_of_document = extractIdFromTweet(record.value());
		// record.partition();
		// return record;
		// }
		// });

		stream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {

			private static final long serialVersionUID = 627242497226732328L;

			@Override
			public void call(JavaRDD<ConsumerRecord<String, String>> javaRDD) throws Exception {
				
				
				
//				JavaEsSpark.saveToEs(javaRDD, "twitter/tweets",
//						 ImmutableMap.of("es.mapping.id", documentId,
//						 "es.net.http.auth.user",username,
//						 "es.net.http.auth.pass",password,
//						 "es.nodes",hostname));
//				javaRDD.foreach(new VoidFunction<ConsumerRecord<String, String>>() {
//
//				
//
//					public void call(ConsumerRecord<String, String> customerRecord) throws Exception {
//						String documentId = extractIdFromTweet(customerRecord.value());						 						
////						JavaEsSpark.save
////					
//						
//					}
//				});
			}

		});

		//

		// stream.map((Function<ConsumerRecord<String, String>, U>) record ->{
		//
		// });

		// javaDStream.

		// javaDStream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String,
		// String>>>() {
		//
		// private static final long serialVersionUID = 627242497226732328L;
		//
		// @Override
		// public void call(JavaRDD<ConsumerRecord<String, String>> javaRDD) throws
		// Exception {
		//
		// System.out.println(javaRDD.count());
		//
		// JavaEsSpark.saveToEs(javaRDD, "twitter/tweets",
		// ImmutableMap.of("es.mapping.id", id_of_document,
		// "es.net.http.auth.user",username,
		// "es.net.http.auth.pass",password,
		// "es.nodes",hostname));
		//
		// }
		//
		// });

		// stream.foreachRDD(rdd -> {
		//
		// rdd.foreach(new VoidFunction<ConsumerRecord<String, String>>() {
		//
		// @Override
		// public void call(ConsumerRecord<String, String> consumerRecord) throws
		// Exception {
		// try {
		//// String id = extractIdFromTweet(consumerRecord.value());
		////
		//// // where we insert data into ElasticSearch
		//// IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id //
		// this is to make our
		//// // consumer idempotent
		//// ).source(consumerRecord.value(), XContentType.JSON);
		////
		//// bulkRequest.add(indexRequest); // we add to our bulk request (takes no
		// time)
		//
		// return consumerRecord.value();
		//
		//
		//
		// } catch (NullPointerException e) {
		// log.warn("skipping bad data: " + consumerRecord.value());
		// }
		//
		// }
		// });
		// });

		// Start the computation
		context.start();

		try {
			context.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
