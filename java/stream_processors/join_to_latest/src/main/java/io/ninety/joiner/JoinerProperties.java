package io.ninety.joiner;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class JoinerProperties {

	// supported property names
	public static final String LEFT_TOPIC = "left.topic";
	public static final String RIGHT_TOPIC = "right.topic";
	public static final String OUT_TOPIC = "out.topic";
	public static final String LEFT_FIELDS = "left.fields"; // e.g. field1 as myalias1, field2 as myalias2, ...
	public static final String RIGHT_FIELDS = "right.fields";
	public static final String LEFT_WHERE_FIELD = "left.where.field";
	public static final String RIGHT_WHERE_FIELD = "right.where.field";
	public static final String LEFT_TIMESTAMP_FIELD = "left.timestamp.field";
	public static final String RIGHT_TIMESTAMP_FIELD = "right.timestamp.field";
	public static final String GROUP_BY_FIELD = "group.by.field"; // the field name on which to group after the join
	public static final String GROUP_BY_TIMESTAMP_FIELD = "group.by.timestamp.field"; // the timestamp field on which to group after the join
	public static final String AGGREGATE_TIMESTAMP_FIELD = "aggregate.timestamp.field"; // the timestamp field used to determine the latest record within each group
	public static final String JOIN_WINDOW_SIZE = "join.window.size"; // ISO-8601 duration string
	public static final String GROUP_WINDOW_SIZE = "group.window.size";
	public static final String JOIN_WINDOW_RETENTION = "join.window.retention";
	public static final String GROUP_WINDOW_RETENTION = "group.window.retention";

	// other constants
	private static final String CSV_REGEX = "\\s*,\\s*";
	private static final String AS_REGEX = "(?i)\\s+AS\\s+";

	// data members
	private final Properties props = new Properties();
	private Map<String, String> leftFields = new HashMap<>(); // mapped field to source field e.g. myalias1 -> field1
	private Map<String, String> rightFields = new HashMap<>();
	private Duration joinWindowSize;
	private Duration groupWindowSize;
	private Duration joinWindowRetention;
	private Duration groupWindowRetention;

	public void loadFromProperties(Properties props) {
		props.forEach((k, v) -> put(String.valueOf(k), String.valueOf(v)));
	}

	public void loadFromEnvironment(String envPrefix) {
		final Properties props = new Properties();
		System.getenv().forEach((k, v) -> {
			if (k.startsWith(envPrefix)) {
				final String name = k.replaceFirst(envPrefix, "").toLowerCase().replace('_', '.');
				props.put(name, v);
			}
		});
		loadFromProperties(props);
	}

	public void loadFromFile(String filename) throws IOException {
		final FileInputStream file = new FileInputStream(filename);
		final Properties props = new Properties();
		props.load(file);
		loadFromProperties(props);
	}

	public String leftTopic() {
		return this.props.getProperty(LEFT_TOPIC);
	}

	public String rightTopic() {
		return this.props.getProperty(RIGHT_TOPIC);
	}
	
	public String outTopic() {
		return this.props.getProperty(OUT_TOPIC);
	}

	public Map<String, String> leftFields() {
		return this.leftFields;
	}

	public Map<String, String> rightFields() {
		return this.rightFields;
	}

	public String leftWhereField() {
		return this.props.getProperty(LEFT_WHERE_FIELD);
	}

	public String rightWhereField() {
		return this.props.getProperty(RIGHT_WHERE_FIELD);
	}

	public String leftMappedWhereField() {
		return this.leftFields.get(this.leftWhereField());
	}

	public String rightMappedWhereField() {
		return this.rightFields.get(this.rightWhereField());
	}

	public String leftTimestampField() {
		return this.props.getProperty(LEFT_TIMESTAMP_FIELD);
	}

	public String rightTimestampField() {
		return this.props.getProperty(RIGHT_TIMESTAMP_FIELD);
	}

	public String groupByField() {
		return this.props.getProperty(GROUP_BY_FIELD);
	}

	public String groupByTimestampField() {
		return this.props.getProperty(GROUP_BY_TIMESTAMP_FIELD);
	}

	public String aggregateTimestampField() {
		return this.props.getProperty(AGGREGATE_TIMESTAMP_FIELD);
	}

	public Duration joinWindowSize() {
		return this.joinWindowSize;
	}

	public Duration groupWindowSize() {
		return this.groupWindowSize;
	}

	public Duration joinWindowRetention() {
		return this.joinWindowRetention;
	}

	public Duration groupWindowRetention() {
		return this.groupWindowRetention;
	}

	public Properties innerProps() {
		return this.props;
	}

	public Map<String, String> toMap() {
		return this.props.entrySet().stream()
				.collect(Collectors.toMap(e -> String.valueOf(e.getKey()), e -> String.valueOf(e.getValue())));
	}

	private Map<String, String> parseFields(String fields) {
		final String[] fieldSpecifiers = fields.trim().split(CSV_REGEX);
		return Arrays.stream(fieldSpecifiers).map(s -> {
			final String[] names = s.split(AS_REGEX);
			if (names.length != 2) {
				throw new RuntimeException("Bad field specifier: " + s);
			}
			return names;
		}).collect(Collectors.toMap(names -> names[0], names -> names[1]));
	}

	public void put(String name, String value) {
		this.props.put(name, value);

		// process fields
		switch (name) {
		case LEFT_FIELDS:
			this.leftFields = parseFields(value);
			break;
		case RIGHT_FIELDS:
			this.rightFields = parseFields(value);
			break;
		case JOIN_WINDOW_SIZE:
			this.joinWindowSize = Duration.parse(value);
			break;
		case GROUP_WINDOW_SIZE:
			this.groupWindowSize = Duration.parse(value);
			break;
		case JOIN_WINDOW_RETENTION:
			this.joinWindowRetention = Duration.parse(value);
			break;
		case GROUP_WINDOW_RETENTION:
			this.groupWindowRetention = Duration.parse(value);
			break;
		}
	}
}
