package io.ninety.joiner;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;

public class AvroLastAggregator implements Initializer<GenericRecord>, Aggregator<String, GenericRecord, GenericRecord>{

	public static AvroLastAggregator create(AvroTimestampExtractor tsExtractor) {
		return new AvroLastAggregator(tsExtractor);
	}
	
	private final AvroTimestampExtractor tsExtractor;
	
	private AvroLastAggregator(AvroTimestampExtractor tsExtractor) {
		this.tsExtractor = tsExtractor;
	}
	
	@Override
	public GenericRecord apply() {
		return null;
	}

	@Override
	public GenericRecord apply(String key, GenericRecord val, GenericRecord agg) {
		if(agg == null) {
			return val;
		}
		long tsVal = this.tsExtractor.extract(val);
		long tsAgg = this.tsExtractor.extract(agg);
		
		if(tsVal > tsAgg) {
			return val;
		}
		return agg;
	}
}
