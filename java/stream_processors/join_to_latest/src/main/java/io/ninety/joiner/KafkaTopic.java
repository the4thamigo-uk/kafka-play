package io.ninety.joiner;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
public class KafkaTopic {

	public static void createTopic(Properties props, String topic, int partitions, short replicas) {

		final AdminClient adminClient = AdminClient.create(props);
		final NewTopic newTopic = new NewTopic(topic, partitions,replicas);

		final List<NewTopic> newTopics = new ArrayList<NewTopic>();
		newTopics.add(newTopic);

		adminClient.createTopics(newTopics);
		adminClient.close();

	}
}
