package io.ninety.joiner;

import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Windowed;

public class WindowedKeyValueMapper<K, V, VR> implements KeyValueMapper<Windowed<K>, V, VR>{

	public static <K, V, VR> WindowedKeyValueMapper<K, V, VR> create(KeyValueMapper<K, V, VR> mapper) {
		return new WindowedKeyValueMapper<K,V,VR>(mapper);
	}
	
	private final KeyValueMapper<K, V, VR> mapper;
	
	private WindowedKeyValueMapper(KeyValueMapper<K, V, VR> mapper) {
		this.mapper = mapper;
	}

	@Override
	public VR apply(Windowed<K> key, V value) {
		System.out.println("window key: " + key.toString());
		return this.mapper.apply(key.key(), value);
	}
}
