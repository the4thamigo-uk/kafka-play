package io.ninety.joiner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.streams.kstream.ValueJoiner;

public class AvroFieldsValueJoiner implements ValueJoiner<GenericRecord, GenericRecord, GenericRecord> {
	private Schema schema;
	private final Map<String, String> leftMappings;
	private final Map<String, String> rightMappings;

	public static AvroFieldsValueJoiner create(Map<String, String> leftMappings, Map<String, String> rightMappings) {
		return new AvroFieldsValueJoiner(leftMappings, rightMappings);
	}

	private AvroFieldsValueJoiner(Map<String, String> leftMappings, Map<String, String> rightMappings) {
		this.leftMappings = leftMappings;
		this.rightMappings = rightMappings;
	}

	@Override
	public GenericRecord apply(GenericRecord leftValue, GenericRecord rightValue) {
		System.out.print("processing record");
		System.out.print(leftValue.toString());

		if (this.schema == null) {
			final List<Field> leftFields = mapFields(leftValue.getSchema(), leftMappings);
			final List<Field> rightFields = mapFields(rightValue.getSchema(), rightMappings);
			final List<Field> fields = new ArrayList<Field>();
			fields.addAll(leftFields);
			fields.addAll(rightFields);
			this.schema = Schema.createRecord("myschema", "mydoc", "io.ninety", false, fields);
		}
		final GenericRecordBuilder b = new GenericRecordBuilder(this.schema);
		mergeValue(b, leftValue, leftMappings);
		mergeValue(b, rightValue, rightMappings);
		final GenericRecord joinValue = b.build();
		System.out.print(joinValue.toString());
		return joinValue;
	}

	private static List<Field> mapFields(Schema s, Map<String, String> m) {
		final List<Field> fs = new ArrayList<Field>();
		s.getFields().forEach((f) -> {
			final String name = f.name();
			final String newName = m.get(name);
			final Field fnew = new Field(newName, f.schema(), f.doc(), f.defaultVal());
			if (fnew != null) {
				fs.add(fnew);
			}
		});
		return fs;
	}

	private static void mergeValue(GenericRecordBuilder b, GenericRecord r, Map<String, String> m) {
		final Schema s = r.getSchema();
		s.getFields().forEach((f) -> {
			final String name = f.name();
			final Object val = r.get(name);
			final String newName = m.get(name);
			b.set(newName, val);
		});
	}
}
