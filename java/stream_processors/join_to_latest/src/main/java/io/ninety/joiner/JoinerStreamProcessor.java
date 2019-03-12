package io.ninety.joiner;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

public class JoinerStreamProcessor {

	public static void main(final String[] args) {
		final Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-joiner");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schema-registry:8081");

		final Topology topology = JoinerTopology.createTopology(props);
		final KafkaStreams streams = new KafkaStreams(topology, props);
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
