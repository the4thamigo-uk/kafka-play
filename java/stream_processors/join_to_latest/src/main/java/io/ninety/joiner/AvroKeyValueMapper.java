package io.ninety.joiner;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.kstream.KeyValueMapper;

public class AvroKeyValueMapper implements KeyValueMapper<String, GenericRecord, String> {

	public static AvroKeyValueMapper create(AvroTimestampExtractor timestampExtractor, String fieldName) {
		return new AvroKeyValueMapper(timestampExtractor, fieldName);
	}
	private final String fieldName;

	private final AvroTimestampExtractor timestampExtractor;

	private AvroKeyValueMapper(AvroTimestampExtractor timestampExtractor, String fieldName) {
		this.fieldName = fieldName;
		this.timestampExtractor = timestampExtractor;
	}

	@Override
	public String apply(String key, GenericRecord value) {
		final Object field = value.get(this.fieldName);
		final long ts = this.timestampExtractor.extract(value);
		return String.valueOf(ts) + "_" + String.valueOf(field);
	}

}
