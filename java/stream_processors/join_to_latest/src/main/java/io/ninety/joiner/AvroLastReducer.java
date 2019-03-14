package io.ninety.joiner;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.kstream.Reducer;

public class AvroLastReducer implements Reducer<GenericRecord>{

	public static AvroLastReducer create(AvroTimestampExtractor tsExtractor) {
		return new AvroLastReducer(tsExtractor);
	}
	
	private final AvroTimestampExtractor tsExtractor;
	
	private AvroLastReducer(AvroTimestampExtractor tsExtractor) {
		this.tsExtractor = tsExtractor;
	}
	
	@Override
	public GenericRecord apply(GenericRecord val, GenericRecord agg) {

		System.out.println("reducing");
		System.out.println(val);
		System.out.println(agg);

		if(val== null) {
			System.out.println("selecting agg");
			System.out.println(agg);
			return agg;
		}
		
		if(agg == null) {
			System.out.println("selecting val");
			System.out.println(val);
			return val;
		}
		
		long tsVal = this.tsExtractor.extract(val);
		long tsAgg = this.tsExtractor.extract(agg);
		System.out.println(tsVal);
		System.out.println(tsAgg);

		if(tsVal >= tsAgg) {
			System.out.println("selecting val");
			System.out.println(val);
			return val;
		}
		System.out.println("selecting agg");
		System.out.println(agg);
		return agg;
	}
}
