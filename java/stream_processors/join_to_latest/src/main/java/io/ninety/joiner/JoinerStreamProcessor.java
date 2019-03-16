package io.ninety.joiner;

import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

public class JoinerStreamProcessor {

	public static void main(final String[] args) {
		final JoinerProperties props = new JoinerProperties();
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schema-registry:8081");
		props.put(JoinerProperties.LEFT_TOPIC, "x");
		props.put(JoinerProperties.RIGHT_TOPIC, "y");
		props.put(JoinerProperties.JOIN_TOPIC, "xy-join");
		props.put(JoinerProperties.OUT_TOPIC, "xy");
		props.put(JoinerProperties.LEFT_WHERE_FIELD, "key2");
		props.put(JoinerProperties.RIGHT_WHERE_FIELD, "key2");
		props.put(JoinerProperties.LEFT_TIMESTAMP_FIELD, "event_time");
		props.put(JoinerProperties.RIGHT_TIMESTAMP_FIELD, "event_time");
		props.put(JoinerProperties.GROUP_BY_FIELD, "key2_1");
		props.put(JoinerProperties.GROUP_BY_TIMESTAMP_FIELD, "event_time_1");
		props.put(JoinerProperties.AGGREGATE_TIMESTAMP_FIELD, "event_time_2");
		props.put(JoinerProperties.JOIN_WINDOW_SIZE, "PT5S");
		props.put(JoinerProperties.GROUP_WINDOW_SIZE, "PT5S");
		props.put(JoinerProperties.JOIN_WINDOW_RETENTION, "PT60S");
		props.put(JoinerProperties.GROUP_WINDOW_RETENTION, "PT60S");
		props.put(JoinerProperties.LEFT_FIELDS, "event_time as event_time_1, key1 as key1_1, key2 as key2_1, val as val_1");
		props.put(JoinerProperties.RIGHT_FIELDS, "event_time as event_time_2, key1 as key1_2, key2 as key2_2, val as val_2");

		props.loadFromEnvironment("APP_");

		// in order to decouple different instances that process different topics, we ensure the app id is unique
		final String appId = String.format("streams-joiner.%s.%s.%s", props.leftTopic(), props.rightTopic(), props.outTopic());
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
		
		final Topology topology = JoinerTopology.create(props);
		final KafkaStreams streams = new KafkaStreams(topology, props.innerProps());
		final CountDownLatch latch = new CountDownLatch(1);

		// attach shutdown handler to catch control-c
		Runtime.getRuntime().addShutdownHook(new Thread("streams-joiner-shutdown-hook") {
			@Override
			public void run() {
				streams.close();
				latch.countDown();
			}
		});

		try {
			streams.start();
			latch.await();
		} catch (final Throwable e) {
			System.exit(1);
		}
		System.exit(0);
	}
}
