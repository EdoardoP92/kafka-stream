package kafka.stream;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;

public class StreamK {

	Map<Integer, String> map = new HashMap<>();
	int key = 0;

	private Properties configInputProps(String bootstrapServer) {

		Properties streamsConfiguration = new Properties();

		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-stream");


		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Arrays.asList(bootstrapServer));

		streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, 
				Serdes.String().getClass().getName());

		streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, 
				Serdes.String().getClass().getName());

		streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "C:\\Users\\prima\\Desktop\\state-dir");

		return streamsConfiguration;
	}

	public KafkaStreams streamTopology(String inputTopic, String outputTopic, String bootstrapServer){
		
		/**
		 * DSL (DOMAIN SPECIFIC LANGUAGE)
		 * HIGH LEVEL API
		 */
		StreamsBuilder builder =  new StreamsBuilder();
		KStream<String, String> kafkaInputStream = builder.stream(inputTopic);
		kafkaInputStream.filter(this.conditionToEvaluate()).mapValues(this.mapValue()).to(outputTopic);

		return new KafkaStreams(builder.build() , this.configInputProps(bootstrapServer));
	}

	private Predicate<String,String> conditionToEvaluate(){

		return new Predicate<String,String>() {

			@Override
			public boolean test(String k, String v) {
				if(map.containsValue(v)) {
					return true;
				}
				map.put(key, v);
				key++;
				System.out.println("MAP: "+map);
				return false;
			}
		};
	}

	private ValueMapper<String, String> mapValue(){

		return new ValueMapper<String, String>(){

			@Override
			public String apply(String value) {

				return value+" kafka-stream";
			}
		};
	}

	public KafkaStreams streamJoinTopology(String leftInputTopic, String rightInputTopic, String outputTopic, String bootstrapServer){

		StreamsBuilder builder =  new StreamsBuilder();

		//Use Kstream
		KStream<String, String> leftStream = builder.stream(leftInputTopic, Consumed.with(Serdes.String(), Serdes.String()));
		KStream<String, String> rightStream = builder.stream(rightInputTopic, Consumed.with(Serdes.String(), Serdes.String()));

		/**
		 * This is a sliding window join, meaning that all tuples close to each other 
		 * with regard to time are joined. 
		 * Time here is the difference up to the size of the window.
		 * These joins are always windowed joins because otherwise, 
		 * the size of the internal state store used to perform the join would grow indefinitely.
		 */
		leftStream.join(rightStream, 
				(leftValue, rightValue) -> "richiesta=" + leftValue + ", esito=" + rightValue, 
				JoinWindows.of(Duration.ofMinutes(2)), 
				StreamJoined.with(
						Serdes.String(),   //key
						Serdes.String(),  //left-value
						Serdes.String()  //right-value
						)).to(outputTopic,Produced.with(Serdes.String(), Serdes.String()));//final key-value serde

		return new KafkaStreams(builder.build(), this.configInputProps(bootstrapServer));
	}

	public KafkaStreams tableJoinTopology(String leftInputTopic, String rightInputTopic, String outputTopic, String bootstrapServer) {

		StreamsBuilder builder =  new StreamsBuilder();

		//Use KTable
		KTable<String,String> leftTable = builder.table(leftInputTopic);
		KTable<String,String> rightTable = builder.table(rightInputTopic);

		/**
		 * KTable-KTable joins are designed to be consistent with their counterparts 
		 * in relational databases. They are always non-windowed joins.
		 * The changelog streams of KTables is materialized into local state stores 
		 * that represent the latest snapshot of their tables. 
		 * The join result is a new KTable representing changelog stream of the join operation.
		 */
		leftTable.join(rightTable,
				(leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue,
				Materialized.with(
						Serdes.String(),  
						Serdes.String()  
						))
		.toStream()
		.to(outputTopic);
		

		return new KafkaStreams(builder.build(), this.configInputProps(bootstrapServer));
	}
	
	public KafkaStreams processorApiJoinTopology(String leftInputTopic, String rightInputTopic, String outputTopic, String bootstrapServer) {
		
		/**
		 * PROCESSOR API
		 * LOW LEVEL API
		 */
		Topology topology = new Topology();
		
		topology
			.addSource(Topology.AutoOffsetReset.EARLIEST, "request-source", Serdes.String().deserializer(),
				    Serdes.String().deserializer(), leftInputTopic)
			
			.addProcessor("request-processor", RequestProcessor::new, "request-source")
			
			.addSource(Topology.AutoOffsetReset.EARLIEST, "response-source", Serdes.String().deserializer(),
				    Serdes.String().deserializer(), rightInputTopic)
			
			.addProcessor("response-processor", ResponseProcessor::new, "response-source")
			
			.addSink("final-sink", outputTopic, Serdes.String().serializer(),
				    Serdes.String().serializer(),  "response-processor")
			
			.addStateStore(this.windowedStoreBuilder("requestStateStore"), "request-processor","response-processor");
			
		System.out.println("**************** \r\n"+topology.describe().toString()+"\r\n********************");
		
		return new KafkaStreams(topology, this.configInputProps(bootstrapServer));
	}
	
	private StoreBuilder<WindowStore<String,String>> windowedStoreBuilder(String name){
		StoreBuilder<WindowStore<String,String>> ret = Stores
				.windowStoreBuilder(Stores
						.persistentWindowStore(name, Duration.ofMinutes(2), Duration.ofMinutes(2), true), 
						Serdes.String(), Serdes.String()).withCachingEnabled();
		return ret;
	}
}
