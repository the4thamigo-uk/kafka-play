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

	public static AvroFieldsValueJoiner create(Map<String, String> leftMappings, Map<String, String> rightMappings) {
		return new AvroFieldsValueJoiner(leftMappings, rightMappings);
	}
	
	private final Map<String, String> leftMappings;
	private final Map<String, String> rightMappings;
	private final String schemaName;
	private final String schemaDoc;
	private final String schemaNamespace;
	private Schema schema;

	private AvroFieldsValueJoiner(Map<String, String> leftMappings, Map<String, String> rightMappings) {
		this.leftMappings = leftMappings;
		this.rightMappings = rightMappings;

		// TODO: make configurable
		this.schemaName = "myschema";
		this.schemaDoc = "mydoc";
		this.schemaNamespace = "io.ninety";
	}

	@Override
	public GenericRecord apply(GenericRecord leftValue, GenericRecord rightValue) {

		System.out.println("joining");
		System.out.println(leftValue);
		System.out.println(rightValue);
		
		if (this.schema == null) {
			final List<Field> leftFields = mapFields(leftValue.getSchema(), leftMappings);
			final List<Field> rightFields = mapFields(rightValue.getSchema(), rightMappings);
			final List<Field> fields = new ArrayList<Field>();
			fields.addAll(leftFields);
			fields.addAll(rightFields);
			this.schema = Schema.createRecord(this.schemaName, this.schemaDoc, this.schemaNamespace, false, fields);
		}
		final GenericRecordBuilder b = new GenericRecordBuilder(this.schema);
		mergeValue(b, leftValue, leftMappings);
		mergeValue(b, rightValue, rightMappings);
		final GenericRecord joinValue = b.build();
		System.out.println(joinValue);
		return joinValue;
	}
	
	private static List<Field> mapFields(Schema s, Map<String, String> m) {
		final List<Field> fs = new ArrayList<Field>();
		s.getFields().forEach((f) -> {
			final String name = f.name();
			final String newName = m.get(name);
			if (newName != null) {
				final Field fnew = new Field(newName, f.schema(), f.doc(), f.defaultVal());
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
			if (newName != null) {
				b.set(newName, val);
			}
		});
	}
}
