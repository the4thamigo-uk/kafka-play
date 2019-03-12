/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.ninety;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.TimestampExtractor;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public final class Joiner {

	public static void main(final String[] args) {
		final Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-joiner");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schema-registry:8081");

		// TODO make this configurable
		final Map<String, String> leftMappings = new HashMap<String, String>();
		leftMappings.put("event_time", "event_time_1");
		leftMappings.put("key1", "key1_1");
		leftMappings.put("key2", "key2_1");
		leftMappings.put("val", "val_1");
		final Map<String, String> rightMappings = new HashMap<String, String>();
		rightMappings.put("event_time", "event_time_2");
		rightMappings.put("key1", "key1_2");
		rightMappings.put("key2", "key2_2");
		rightMappings.put("val", "val_2");

		class EventTimeExtractor implements TimestampExtractor {
			@Override
			public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
				System.out.print("processing timestamp");
				System.out.print(record);
				GenericRecord r = (GenericRecord) record.value();
				return (long) r.get("event_time");
			}
		}

		class MergeAllFields implements ValueJoiner<GenericRecord, GenericRecord, GenericRecord> {
			private Schema schema;

			@Override
			public GenericRecord apply(GenericRecord leftValue, GenericRecord rightValue) {
				System.out.print("processing record");
				System.out.print(leftValue.toString());

				if (this.schema == null) {
					final List<Field> leftFields = mapFields(leftValue.getSchema(), leftMappings);
					final List<Field> rightFields = mapFields(rightValue.getSchema(), rightMappings);
					final List<Field> fields = new ArrayList<Field>();
					fields.addAll(leftFields);
					fields.addAll(rightFields);
					this.schema = Schema.createRecord("myschema", "mydoc", "io.ninety", false, fields);
				}
				final GenericRecordBuilder b = new GenericRecordBuilder(this.schema);
				mergeValue(b, leftValue, leftMappings);
				mergeValue(b, rightValue, rightMappings);
				final GenericRecord joinValue = b.build();
				System.out.print(joinValue.toString());
				return joinValue;
			}
		}

		final Map<String, String> propMap = props.entrySet().stream()
				.collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString()));

		final GenericAvroSerde avroSerde = new GenericAvroSerde();
		avroSerde.configure(propMap, false);
		final Consumed<String, GenericRecord> consumed = Consumed.with(Serdes.String(), avroSerde)
				.withTimestampExtractor(new EventTimeExtractor());
		final Joined<String, GenericRecord, GenericRecord> joined = Joined.with(Serdes.String(), avroSerde, avroSerde);

		final StreamsBuilder builder = new StreamsBuilder();
		final KStream<String, GenericRecord> leftStream = builder.stream("x", consumed);
		final KStream<String, GenericRecord> rightStream = builder.stream("y", consumed);

		System.out.print("defining join");
		final KStream<String, GenericRecord> joinStream = leftStream.join(rightStream, new MergeAllFields(),
				JoinWindows.of(Duration.ofSeconds(5)), joined);

		final Produced<String, GenericRecord> produced = Produced.with(Serdes.String(), avroSerde);
		joinStream.to("xy", produced);

		System.out.print("creating topology");
		final Topology topology = builder.build();
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

	private static List<Field> mapFields(Schema s, Map<String, String> m) {
		final List<Field> fs = new ArrayList<Field>();
		s.getFields().forEach((f) -> {
			final String name = f.name();
			final String newName = m.get(name);
			final Field fnew = new Field(newName, f.schema(), f.doc(), f.defaultVal());
			if (fnew != null) {
				fs.add(fnew);
			}
		});
		return fs;
	}

	private static void mergeValue(GenericRecordBuilder b, GenericRecord r, Map<String, String> m) {
		final Schema s = r.getSchema();
		s.getFields().forEach((f) -> {
			final String name = f.name();
			final Object val = r.get(name);
			final String newName = m.get(name);
			b.set(newName, val);
		});
	}

}
