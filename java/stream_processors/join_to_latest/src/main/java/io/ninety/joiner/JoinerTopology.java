package io.ninety.joiner;

import java.time.Duration;
import java.util.Objects;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.Windowed;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;

public final class JoinerTopology {

	public static Topology create(JoinerProperties props) {

		// make the avro serde
		final GenericAvroSerde avroSerde = new GenericAvroSerde();
		avroSerde.configure(props.toMap(), false);
		final Serde<String> strSerde = Serdes.String();

		// timestamp extractors
		final AvroTimestampExtractor leftTsExtractor = AvroTimestampExtractor.create(props.leftTimestampField());
		final AvroTimestampExtractor rightTsExtractor = AvroTimestampExtractor.create(props.rightTimestampField());
		final AvroTimestampExtractor groupByTsExtractor = AvroTimestampExtractor.create(props.groupByTimestampField());
		final AvroTimestampExtractor aggregateTsExtractor = AvroTimestampExtractor.create(props.aggregateTimestampField());

		System.out.println(props.toMap());
		
		// create the streams from the topics
		final StreamsBuilder builder = new StreamsBuilder();
		final Consumed<String, GenericRecord> leftConsumed = Consumed.with(strSerde, avroSerde)
				.withTimestampExtractor(leftTsExtractor);
		final KStream<String, GenericRecord> leftStream = builder.stream(props.leftTopic(), leftConsumed);
		final Consumed<String, GenericRecord> rightConsumed = Consumed.with(strSerde, avroSerde)
				.withTimestampExtractor(rightTsExtractor);
		final KStream<String, GenericRecord> rightStream = builder.stream(props.rightTopic(), rightConsumed);

		// setup the join
		final Joined<String, GenericRecord, GenericRecord> joined = Joined.with(strSerde, avroSerde, avroSerde);
		final ValueJoiner<GenericRecord, GenericRecord, GenericRecord> joiner = AvroFieldsValueJoiner
				.create(props.leftFields(), props.rightFields());
		final JoinWindows joinWindow = JoinWindows.of(Duration.ZERO).before(props.joinWindowSize()).grace(props.joinWindowRetention());
		final KStream<String, GenericRecord> joinStream = leftStream.join(rightStream, joiner, joinWindow, joined)
				.filter((k,v) -> Objects.deepEquals(v.get(props.leftMappedWhereField()), v.get(props.rightMappedWhereField())));

		// re-timestamp joined stream on left timestamp
		KafkaTopic.create(props.innerProps(), props.joinTopic(), 1, 1); // TODO shouldn't we use same partitioning
																		// strategy?
		final Produced<String, GenericRecord> produced = Produced.with(strSerde, avroSerde);
		joinStream.to(props.joinTopic(), produced);
		final Consumed<String, GenericRecord> groupByConsumed = Consumed.with(strSerde, avroSerde)
				.withTimestampExtractor(groupByTsExtractor);
		final KStream<String, GenericRecord> joinStream2 = builder.stream(props.joinTopic(), groupByConsumed);

		// setup the grouping
		final KeyValueMapper<String, GenericRecord, String> groupKeyMapper = AvroKeyValueMapper
				.create(groupByTsExtractor, props.groupByField());
		final TimeWindowedKStream<String, GenericRecord> groupStream = joinStream2
				.groupBy(groupKeyMapper, Grouped.with(strSerde, avroSerde))
				.windowedBy(TimeWindows.of(props.groupWindowSize()).grace(props.groupWindowRetention()));
		final AvroLastAggregator lastAggregator = AvroLastAggregator.create(aggregateTsExtractor);
		final KTable<Windowed<String>, GenericRecord> groupTable = groupStream
				.aggregate(lastAggregator, lastAggregator, Materialized.with(strSerde, avroSerde))
				.filter((key, value) -> value != null);

		// write the changelog stream to the topic
		groupTable.toStream((k,v) -> String.valueOf(v.get(props.groupByField()))).to(props.outTopic(), produced);

		return builder.build();
	}

}
