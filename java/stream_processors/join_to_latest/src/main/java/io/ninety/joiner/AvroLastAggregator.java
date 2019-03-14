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
		System.out.println("initializing");
		return null;
	}

	@Override
	public GenericRecord apply(String key, GenericRecord val, GenericRecord agg) {

		System.out.println("aggregating");
		System.out.println(key);
		System.out.println(val);
		System.out.println(agg);
		
		if(agg == null) {
			return val;
		}
		long tsVal = this.tsExtractor.extract(val);
		long tsAgg = this.tsExtractor.extract(agg);
		System.out.println(tsVal);
		System.out.println(tsAgg);

		if(tsVal >= tsAgg) {
			System.out.println("selecting key");
			System.out.println(val);
			return val;
		}
		System.out.println("selecting agg");
		System.out.println(agg);
		return agg;
	}
}
