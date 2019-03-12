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
package io.ninety.joiner;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;


public final class JoinerTopology {

	public static Topology createTopology(Properties props) {

		// TODO make this configurable
		final String leftTopic = "x";
		final String rightTopic = "y";
		final String joinTopic = "xy";
		final String leftTsField = "event_time";
		final String rightTsField = "event_time";
		final Duration joinAfter = Duration.ofSeconds(5);
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

		final String groupByKey = "key1_1";

		// make the avro serde
		final Map<String, String> propMap = props.entrySet().stream()
				.collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString()));
		final GenericAvroSerde avroSerde = new GenericAvroSerde();
		avroSerde.configure(propMap, false);
		final Serde<String> strSerde = Serdes.String();

		// timestamp extractors
		final AvroTimestampExtractor leftTsExtractor = AvroTimestampExtractor.create(leftTsField);
		final AvroTimestampExtractor rightTsExtractor = AvroTimestampExtractor.create(rightTsField);
		final AvroTimestampExtractor leftMappedTsExtractor = AvroTimestampExtractor.create(leftMappings.get(leftTsField));
		final AvroTimestampExtractor rightMappedTsExtractor = AvroTimestampExtractor.create(rightMappings.get(rightTsField));
		
		// create the streams from the topics
		final StreamsBuilder builder = new StreamsBuilder();
		final Consumed<String, GenericRecord> leftConsumed = Consumed.with(strSerde, avroSerde).withTimestampExtractor(leftTsExtractor);
		final KStream<String, GenericRecord> leftStream = builder.stream(leftTopic, leftConsumed);
		final Consumed<String, GenericRecord> rightConsumed = Consumed.with(strSerde, avroSerde).withTimestampExtractor(rightTsExtractor);
		final KStream<String, GenericRecord> rightStream = builder.stream(rightTopic, rightConsumed);

		// setup the join
		final Joined<String, GenericRecord, GenericRecord> joined = Joined.with(strSerde, avroSerde, avroSerde);
		final ValueJoiner<GenericRecord, GenericRecord, GenericRecord> joiner = AvroFieldsValueJoiner.create(leftMappings,rightMappings);
		final JoinWindows joinWindow = JoinWindows.of(Duration.ZERO).before(joinAfter);
		final KStream<String, GenericRecord> joinStream = leftStream.join(rightStream, joiner, joinWindow, joined);
		
		// setup the grouping
		final KeyValueMapper<String, GenericRecord, String> groupKeyMapper = AvroKeyValueMapper.create(leftMappedTsExtractor, groupByKey);
		final KGroupedStream<String, GenericRecord> groupStream= joinStream.groupBy(groupKeyMapper, Grouped.with(strSerde,  avroSerde));
		final AvroLastAggregator lastAggregator = AvroLastAggregator.create(rightMappedTsExtractor);
		final KTable<String, GenericRecord> groupTable = groupStream.aggregate(lastAggregator, lastAggregator, Materialized.with(strSerde, avroSerde));

		// write the changelog stream to the topic
		final Produced<String, GenericRecord> produced = Produced.with(strSerde, avroSerde);
		groupTable.toStream(groupKeyMapper).to(joinTopic, produced);

		return builder.build();
	}

}
