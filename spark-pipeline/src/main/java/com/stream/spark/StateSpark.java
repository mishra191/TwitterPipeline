/**
 * 
 */
package com.stream.spark;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

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
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;

import scala.Tuple2;

/**
 * @author anumishr0
 *
 */
public class StateSpark implements Serializable{

	private final Logger log = LoggerFactory.getLogger(getClass());
	private static JsonParser jsonParser = new JsonParser();
	private String hostname = "twitter-stream-4407600727.eu-west-1.bonsaisearch.net";
	private String username = "lakesqs2hn";
	private String password = "jrg1pfxmvb";
	BulkRequestChild bulkRequest = new BulkRequestChild();
	private static final Pattern SPACE = Pattern.compile(" ");

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
		StateSpark sparkConsumerService = new StateSpark();
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

	// public static KafkaConsumer<String, String> createConsumer(String topic){
	//
	// String bootstrapServers = "127.0.0.1:9092";
	// String groupId = "kafka-demo-elasticsearch";
	//
	// // create consumer configs
	// Properties properties = new Properties();
	// properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
	// bootstrapServers);
	// properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
	// StringDeserializer.class.getName());
	// properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
	// StringDeserializer.class.getName());
	// properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
	// properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	// properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); //
	// disable auto commit of offsets
	// properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100"); //
	// disable auto commit of offsets
	//
	// // create consumer
	// KafkaConsumer<String, String> consumer = new KafkaConsumer<String,
	// String>(properties);
	// consumer.subscribe(Arrays.asList(topic));
	//
	// return consumer;
	//
	// }

	// public static RestHighLevelClient createClient(){
	//
	// // replace with your own credentials
	// String hostname = ""; // localhost or bonsai url
	// String username = ""; // needed only for bonsai
	// String password = ""; // needed only for bonsai
	//
	// // remove credentialsProvider if you run a local ES
	// final CredentialsProvider credentialsProvider = new
	// BasicCredentialsProvider();
	// credentialsProvider.setCredentials(AuthScope.ANY,
	// new UsernamePasswordCredentials(username, password));
	//
	// RestClientBuilder builder = RestClient.builder(
	// new HttpHost(hostname, 443, "https"))
	// .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback()
	// {
	// @Override
	// public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder
	// httpClientBuilder) {
	// return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
	// }
	// });
	//
	// RestHighLevelClient client = new RestHighLevelClient(builder);
	// return client;
	// }

	public void run() {
		log.info("Running Spark Consumer Service..");

		Collection<String> topics = Arrays.asList("twitter_tweets");

		JavaStreamingContext context = new JavaStreamingContext("local[*]", "JavaDirectKafka-1", Durations.seconds(10));

		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(context,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, consumerConfigs()));
		
		List<Tuple2<String, Integer>> tuples =
		        Arrays.asList(new Tuple2<>("hello", 1), new Tuple2<>("world", 1));
		    JavaPairRDD<String, Integer> initialRDD = context.sparkContext().parallelizePairs(tuples);

		context.checkpoint(".");
		JavaDStream<String> lines = stream.map(new Function<ConsumerRecord<String, String>, String>() {

			private static final long serialVersionUID = 6272424972267329328L;

			@Override
			public String call(ConsumerRecord<String, String> record) {
				return record.value();
			}
		});

		JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());

		JavaPairDStream<String, Integer> wordsDstream = words.mapToPair(s -> new Tuple2<>(s, 1));
		
		// Reduce last 30 seconds of data, every 10 seconds
		JavaPairDStream<String, Integer> windowedWordCounts = wordsDstream.reduceByKeyAndWindow((i1, i2) -> i1 + i2, Durations.seconds(30), Durations.seconds(10));

		Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> mappingFunc = (word, one,
				state) -> {
			int sum = one.orElse(0) + (state.exists() ? state.get() : 0);
			Tuple2<String, Integer> output = new Tuple2<>(word, sum);
			state.update(sum);
			return output;
		};

		// DStream made of get cumulative counts that get updated in every batch
		JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> stateDstream = windowedWordCounts
				.mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD));

		stateDstream.print();

		// Got the java stream with only the text....now find out the total words in
		// this

		// Start the computation
		context.start();

		try {
			context.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
