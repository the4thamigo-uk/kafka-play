package io.ninety.joiner;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;

public class JoinerProperties {

	// supported property names
	public static final String LEFT_TOPIC = "left.topic";
	public static final String RIGHT_TOPIC = "right.topic";
	public static final String OUT_TOPIC = "out.topic";
	public static final String LEFT_FIELDS = "left.fields"; // e.g. field1 as myalias1, field2 as myalias2, ...
	public static final String RIGHT_FIELDS = "right.fields";
	public static final String LEFT_TIMESTAMP_FIELD = "left.timestamp.field";
	public static final String RIGHT_TIMESTAMP_FIELD = "right.timestamp.field";
	public static final String GROUP_BY_FIELD = "group.by.field"; // the field name on which to group after the join
	public static final String JOIN_WINDOW_SIZE = "join.window.size"; // ISO-8601 duration string
	public static final String GROUP_WINDOW_SIZE = "group.window.size";
	public static final String JOIN_WINDOW_RETENTION = "join.window.retention";
	public static final String GROUP_WINDOW_RETENTION = "group.window.retention";

	// other constants
	private static final String CSV_REGEX = "\\s*,\\s*";
	private static final String AS_REGEX = "(?i)\\s+AS\\s+";

	// data members
	private final Properties props = new Properties();
	private Map<String, String> leftFields;
	private Map<String, String> rightFields;
	private Duration joinWindowSize;
	private Duration groupWindowSize;
	private Duration joinWindowRetention;
	private Duration groupWindowRetention;

	public String groupByField() {
		return props.getProperty(GROUP_BY_FIELD);
	}

	public Duration groupWindowRetention() {
		return groupWindowRetention;
	}

	public Duration groupWindowSize() {
		return groupWindowSize;
	}

	public Properties innerProps() {
		return this.props;
	}

	public Duration joinWindowRetention() {
		return joinWindowRetention;
	}

	public Duration joinWindowSize() {
		return joinWindowSize;
	}

	public Map<String, String> leftFields() {
		return leftFields;
	}

	public String leftTimestampField() {
		return props.getProperty(LEFT_TIMESTAMP_FIELD);
	}

	public String leftTopic() {
		return props.getProperty(LEFT_TOPIC);
	}

	public void loadFromEnvironment(String envPrefix) {
		final Properties props = new Properties();
		System.getenv().forEach((k, v) -> {
			if (k.startsWith(envPrefix)) {
				final String name = k.replaceFirst(envPrefix, "").toLowerCase().replace('_', '.');
				props.put(name, v);
			}
		});
		System.out.println(props);
		loadFromProperties(props);
	}

	public void loadFromFile(String filename) throws IOException {
		final FileInputStream file = new FileInputStream(filename);
		final Properties props = new Properties();
		props.load(file);
		loadFromProperties(props);
	}

	public void loadFromProperties(Properties props) {
		props.forEach((k, v) -> {
			put(String.valueOf(k), String.valueOf(v));
		});
	}

	public String outTopic() {
		return props.getProperty(OUT_TOPIC);
	}

	private Map<String, String> parseFields(String fields) {
		final String[] fieldSpecifiers = fields.trim().split(CSV_REGEX);
		return Arrays.stream(fieldSpecifiers).map(s -> {
			final String[] names = s.split(AS_REGEX);
			if (names.length != 2) {
				throw new RuntimeException("Bad field specifier: " + s);
			}
			return names;
		}).collect(Collectors.toMap(names -> {
			return names[0];
		}, names -> {
			return names[1];
		}));
	}

	public void put(String name, String value) {
		props.put(name, value);

		// process fields
		switch (name) {
		case LEFT_FIELDS:
			leftFields = parseFields(value);
			break;
		case RIGHT_FIELDS:
			rightFields = parseFields(value);
			break;
		case JOIN_WINDOW_SIZE:
			joinWindowSize = Duration.parse(value);
			break;
		case GROUP_WINDOW_SIZE:
			groupWindowSize = Duration.parse(value);
			break;
		case JOIN_WINDOW_RETENTION:
			joinWindowRetention = Duration.parse(value);
			break;
		case GROUP_WINDOW_RETENTION:
			groupWindowRetention = Duration.parse(value);
			break;
		}
	}

	public Map<String, String> rightFields() {
		return rightFields;
	}

	public String rightTimestampField() {
		return props.getProperty(RIGHT_TIMESTAMP_FIELD);
	}

	public String rightTopic() {
		return props.getProperty(RIGHT_TOPIC);
	}
	
	public Map<String, String> toMap() {
		return Maps.fromProperties(props);
	}
}
