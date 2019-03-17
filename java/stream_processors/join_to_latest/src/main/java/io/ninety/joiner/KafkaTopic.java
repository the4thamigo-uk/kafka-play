package io.ninety.joiner;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
public class KafkaTopic {

	public static void create(Properties props, String topic, int partitions, int replicas) {

		final AdminClient adminClient = AdminClient.create(props);
		final NewTopic newTopic = new NewTopic(topic, partitions, (short)replicas);

		final List<NewTopic> newTopics = new ArrayList<NewTopic>();
		newTopics.add(newTopic);

		adminClient.createTopics(newTopics);
		adminClient.close();

	}
}
