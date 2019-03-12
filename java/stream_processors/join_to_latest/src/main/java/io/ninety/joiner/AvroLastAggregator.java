package io.ninety.joiner;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class AvroLastAggregator implements Initializer<GenericRecord>, Aggregator<String, GenericRecord, GenericRecord>{

	private final AvroTimestampExtractor tsExtractor;
	
	public static AvroLastAggregator create(AvroTimestampExtractor tsExtractor) {
		return new AvroLastAggregator(tsExtractor);
	}
	
	private AvroLastAggregator(AvroTimestampExtractor tsExtractor) {
		this.tsExtractor = tsExtractor;
	}
	
	@Override
	public GenericRecord apply() {
		return null;
	}

	@Override
	public GenericRecord apply(String key, GenericRecord val, GenericRecord agg) {
		System.out.println("aggregate");
		System.out.println(String.valueOf(key));
		System.out.println(String.valueOf(val));
		System.out.println(String.valueOf(agg));
		if(agg == null) {
			return val;
		}
		long tsVal = this.tsExtractor.extract(val);
		long tsAgg = this.tsExtractor.extract(agg);
		
		if(tsVal > tsAgg) {
			System.out.println("choosing val");
			System.out.println(String.valueOf(val));
			return val;
		}
		System.out.println("choosing agg");
		System.out.println(String.valueOf(agg));
		return agg;
	}
}
