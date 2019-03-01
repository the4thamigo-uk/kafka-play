package io.ninety;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class App 
{
    public static void main( String[] args )
    {
      final Properties props = new Properties();
        final String leftTopic = System.getenv("JOINER_LEFT_TOPIC");
        final String rightTopic = System.getenv("JOINER_RIGHT_TOPIC");
        final String keySerde = Serdes.String();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-joiner");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, keySerde.getClass().getName());

        // we dont need these
        //props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // create avro serializers/deserializers
        final GenericAvroSerde avroSerde = new GenericAvroSerde();
        final Map<String, String> serdeConfig = ImmutableMap.of(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, props.get(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG).toString());
        avroSerde.configure(serdeConfig, false);

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> leftStream = builder.stream(leftTopic, Consumed.with(keySerde, avroSerde);
        final KStream<String, String> rightStream = builder.stream(rightTopic, Consumed.with(keySerde, avroSerde);

          final KStream<GenericRecord, GenericRecord> joinStream = leftStream.join(rightStream,
            (left, right) -> {
              GenericRecord joined = new GenericData.Record(schema);
              viewRegion.put("user", view.get("user"));
              viewRegion.put("page", view.get("page"));
              viewRegion.put("region", region);
              return viewRegion;            
            },
          JoinWindows.of(TimeUnit.MINUTES.toMillis(5)),
        );

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
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
d
