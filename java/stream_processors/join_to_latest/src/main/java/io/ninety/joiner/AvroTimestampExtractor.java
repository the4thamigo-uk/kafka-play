package io.ninety.joiner;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class AvroTimestampExtractor implements TimestampExtractor {

	private final String fieldName;

	public static AvroTimestampExtractor create(String fieldName) {
		return new AvroTimestampExtractor(fieldName);
	}

	private AvroTimestampExtractor(String fieldName) {
		this.fieldName = fieldName;
	}

	@Override
	public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
		final GenericRecord r = (GenericRecord) record.value();
		return (long) r.get(this.fieldName);
	}
}
