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
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-joiner");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schema-registry:8081");
		props.put(JoinerProperties.LEFT_TOPIC, "x");
		props.put(JoinerProperties.RIGHT_TOPIC, "y");
		props.put(JoinerProperties.OUT_TOPIC, "xy");
		props.put(JoinerProperties.LEFT_TIMESTAMP_FIELD, "event_time");
		props.put(JoinerProperties.RIGHT_TIMESTAMP_FIELD, "event_time");
		props.put(JoinerProperties.GROUP_BY_FIELD, "key1_1");
		props.put(JoinerProperties.JOIN_WINDOW_SIZE, "PT5S");
		props.put(JoinerProperties.GROUP_WINDOW_SIZE, "PT5S");
		props.put(JoinerProperties.JOIN_WINDOW_RETENTION, "PT5S");
		props.put(JoinerProperties.GROUP_WINDOW_RETENTION, "PT5S");
		props.put(JoinerProperties.LEFT_FIELDS, "event_time as event_time_1, key1 as key1_1, key2 as key2_1, val as val_1");
		props.put(JoinerProperties.RIGHT_FIELDS, "event_time as event_time_2, key1 as key1_2, key2 as key2_2, val as val_2");

		props.loadFromEnvironment("APP_");
		
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