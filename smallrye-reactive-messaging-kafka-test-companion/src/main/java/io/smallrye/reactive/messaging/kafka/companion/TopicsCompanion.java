package io.smallrye.reactive.messaging.kafka.companion;

import static io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion.toUni;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;

/**
 * Companion for Topics operations on Kafka broker
 */
public class TopicsCompanion {

    private final AdminClient adminClient;
    private final Duration kafkaApiTimeout;

    public TopicsCompanion(AdminClient adminClient, Duration kafkaApiTimeout) {
        this.adminClient = adminClient;
        this.kafkaApiTimeout = kafkaApiTimeout;
    }

    /**
     * @param newTopics the set of {@link NewTopic}s to create
     */
    public void create(Collection<NewTopic> newTopics) {
        toUni(adminClient.createTopics(newTopics).all())
                .await().atMost(kafkaApiTimeout);
    }

    /**
     * @param topicPartitions the map of topic names to partition counts to create
     */
    public void create(Map<String, Integer> topicPartitions) {
        create(topicPartitions.entrySet().stream()
                .map(e -> new NewTopic(e.getKey(), e.getValue(), (short) 1))
                .collect(Collectors.toList()));
    }

    /**
     * Create topic
     *
     * @param topic the topic name
     * @param partition the partition count
     */
    public void create(String topic, int partition) {
        create(Collections.singletonList(new NewTopic(topic, partition, (short) 1)));
    }

    /**
     * Create topic and wait for creation
     *
     * @param topic the topic name
     * @param partition the partition count
     * @return the name of the created topic
     */
    public String createAndWait(String topic, int partition) {
        create(topic, partition);
        waitForTopic(topic);
        return topic;
    }

    private void waitForTopic(String topic) {
        try {
            int maxRetries = 10;
            boolean done = false;
            for (int i = 0; i < maxRetries && !done; i++) {
                done = list().contains(topic);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Timed out waiting for topic");
        }
    }

    /**
     * @return the set of topic names
     */
    public Set<String> list() {
        return toUni(adminClient.listTopics().names()).await().atMost(kafkaApiTimeout);
    }

    /**
     * @return the map of topic names to topic descriptions
     */
    public Map<String, TopicDescription> describeAll() {
        return toUni(adminClient.listTopics().names())
                .onItem().transformToUni(topics -> toUni(adminClient.describeTopics(topics).allTopicNames()))
                .await().atMost(kafkaApiTimeout);
    }

    /**
     * @param topics topics to describe
     * @return the map of topic names to topic descriptions
     */
    public Map<String, TopicDescription> describe(String... topics) {
        if (topics.length == 0) {
            return describeAll();
        }
        return toUni(adminClient.describeTopics(Arrays.asList(topics)).allTopicNames()).await().atMost(kafkaApiTimeout);
    }

    /**
     * @param topics the collection of topic names to delete
     */
    public void delete(Collection<String> topics) {
        toUni(adminClient.deleteTopics(topics).all())
                .await().atMost(kafkaApiTimeout);
    }

    /**
     * @param topics the topic names to delete
     */
    public void delete(String... topics) {
        delete(Arrays.asList(topics));
    }
}
