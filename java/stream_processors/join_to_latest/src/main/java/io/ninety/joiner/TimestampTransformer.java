package io.ninety.joiner;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;

public class TimestampTransformer implements Transformer<String, GenericRecord, KeyValue<String, GenericRecord>>{

	private final String timestampField;
	private ProcessorContext context;
	
	public TimestampTransformer(String timestampField) {
		this.timestampField = timestampField;
	}
	
	@Override
	public void init(ProcessorContext context) {
		this.context = context;
	}

	@Override
	public KeyValue<String, GenericRecord> transform(String key, GenericRecord value) {
		long ts = (Long)value.get(timestampField);
        context.forward(key, value, To.all().withTimestamp(ts));
        return KeyValue.pair(key, value);
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}
}
