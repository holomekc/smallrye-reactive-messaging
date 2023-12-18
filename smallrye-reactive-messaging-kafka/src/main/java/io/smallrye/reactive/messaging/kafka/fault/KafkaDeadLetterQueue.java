package io.smallrye.reactive.messaging.kafka.fault;

import static io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler.DESERIALIZATION_FAILURE_DLQ;
import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;
import static org.apache.kafka.clients.CommonClientConfigs.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.eclipse.microprofile.reactive.messaging.spi.ConnectorFactory.INCOMING_PREFIX;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaCDIEvents;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorOutgoingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaConsumer;
import io.smallrye.reactive.messaging.kafka.KafkaProducer;
import io.smallrye.reactive.messaging.kafka.SerializationFailureHandler;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.impl.ReactiveKafkaProducer;
import io.smallrye.reactive.messaging.providers.impl.ConnectorConfig;
import io.smallrye.reactive.messaging.providers.impl.OverrideConnectorConfig;
import io.vertx.mutiny.core.Vertx;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class KafkaDeadLetterQueue implements KafkaFailureHandler {

    public static final String DEAD_LETTER_EXCEPTION_CLASS_NAME = "dead-letter-exception-class-name";
    public static final String DEAD_LETTER_CAUSE_CLASS_NAME = "dead-letter-cause-class-name";

    public static final String DEAD_LETTER_REASON = "dead-letter-reason";
    public static final String DEAD_LETTER_CAUSE = "dead-letter-cause";
    public static final String DEAD_LETTER_TOPIC = "dead-letter-topic";
    public static final String DEAD_LETTER_OFFSET = "dead-letter-offset";
    public static final String DEAD_LETTER_PARTITION = "dead-letter-partition";

    private final String channel;
    private final KafkaProducer producer;
    private final String topic;
    private final BiConsumer<Throwable, Boolean> reportFailure;

    public KafkaDeadLetterQueue(String channel, String topic, KafkaProducer producer,
            BiConsumer<Throwable, Boolean> reportFailure) {
        this.channel = channel;
        this.topic = topic;
        this.producer = producer;
        this.reportFailure = reportFailure;
    }

    @ApplicationScoped
    @Identifier(Strategy.DEAD_LETTER_QUEUE)
    public static class Factory implements KafkaFailureHandler.Factory {

        @Inject
        KafkaCDIEvents kafkaCDIEvents;

        @Inject
        @Any
        Instance<SerializationFailureHandler<?>> serializationFailureHandlers;

        @Inject
        @Any
        Instance<ProducerInterceptor<?, ?>> producerInterceptors;

        @Inject
        Instance<Config> rootConfig;

        @Override
        public KafkaFailureHandler create(KafkaConnectorIncomingConfiguration config,
                Vertx vertx,
                KafkaConsumer<?, ?> consumer,
                BiConsumer<Throwable, Boolean> reportFailure) {
            Map<String, Object> deadQueueProducerConfig = new HashMap<>(consumer.configuration());
            String keyDeserializer = (String) deadQueueProducerConfig.remove(KEY_DESERIALIZER_CLASS_CONFIG);
            String valueDeserializer = (String) deadQueueProducerConfig.remove(VALUE_DESERIALIZER_CLASS_CONFIG);

            String consumerClientId = (String) consumer.configuration().get(CLIENT_ID_CONFIG);
            ConnectorConfig connectorConfig = new OverrideConnectorConfig(INCOMING_PREFIX, rootConfig.get(),
                    config.getChannel(), "dead-letter-queue", Map.of(
                            KEY_SERIALIZER_CLASS_CONFIG, c -> getMirrorSerializer(keyDeserializer),
                            VALUE_SERIALIZER_CLASS_CONFIG, c -> getMirrorSerializer(valueDeserializer),
                            CLIENT_ID_CONFIG, c -> config.getDeadLetterQueueProducerClientId()
                                    .orElse("kafka-dead-letter-topic-producer-" + consumerClientId),
                            "topic", c -> "dead-letter-topic-" + config.getChannel(),
                            "key-serialization-failure-handler", c -> "dlq-serialization",
                            "value-serialization-failure-handler", c -> "dlq-serialization",
                            INTERCEPTOR_CLASSES_CONFIG, c -> ""));
            KafkaConnectorOutgoingConfiguration producerConfig = new KafkaConnectorOutgoingConfiguration(connectorConfig);

            String deadQueueTopic = config.getDeadLetterQueueTopic().orElse("dead-letter-topic-" + config.getChannel());

            log.deadLetterConfig(producerConfig.getTopic().orElse(null), producerConfig.getKeySerializer(),
                    producerConfig.getValueSerializer());

            // fire producer event (e.g. bind metrics)
            ReactiveKafkaProducer<Object, Object> producer = new ReactiveKafkaProducer<>(producerConfig,
                    serializationFailureHandlers, producerInterceptors, null, (p, c) -> kafkaCDIEvents.producer().fire(p));

            return new KafkaDeadLetterQueue(config.getChannel(), deadQueueTopic, producer, reportFailure);
        }
    }

    private static String getMirrorSerializer(String deserializer) {
        if (deserializer == null) {
            return StringSerializer.class.getName();
        } else {
            return deserializer.replace("Deserializer", "Serializer");
        }
    }

    private String getThrowableMessage(Throwable throwable) {
        String text = throwable.getMessage();
        if (text == null) {
            text = throwable.toString();
        }
        return text;
    }

    @Override
    public <K, V> Uni<Void> handle(IncomingKafkaRecord<K, V> record, Throwable reason, Metadata metadata) {

        OutgoingKafkaRecordMetadata<K> outgoing = metadata != null
                ? metadata.get(OutgoingKafkaRecordMetadata.class).orElse(null)
                : null;

        String topic = this.topic;
        if (outgoing != null && outgoing.getTopic() != null) {
            topic = outgoing.getTopic();
        }

        K key = record.getKey();
        if (outgoing != null && outgoing.getKey() != null) {
            key = outgoing.getKey();
        }

        Integer partition = null;
        if (outgoing != null && outgoing.getPartition() >= 0) {
            partition = outgoing.getPartition();
        }

        ProducerRecord<K, V> dead = new ProducerRecord<>(topic, partition, key, record.getPayload());

        addHeader(dead, DEAD_LETTER_EXCEPTION_CLASS_NAME, reason.getClass().getName());
        addHeader(dead, DEAD_LETTER_REASON, getThrowableMessage(reason));
        if (reason.getCause() != null) {
            addHeader(dead, DEAD_LETTER_CAUSE_CLASS_NAME, reason.getCause().getClass().getName());
            addHeader(dead, DEAD_LETTER_CAUSE, getThrowableMessage(reason.getCause()));
        }
        addHeader(dead, DEAD_LETTER_TOPIC, record.getTopic());
        addHeader(dead, DEAD_LETTER_PARTITION, Integer.toString(record.getPartition()));
        addHeader(dead, DEAD_LETTER_OFFSET, Long.toString(record.getOffset()));
        record.getHeaders().forEach(header -> dead.headers().add(header));
        if (outgoing != null && outgoing.getHeaders() != null) {
            outgoing.getHeaders().forEach(header -> dead.headers().add(header));
        }
        // remove DESERIALIZATION_FAILURE_DLQ header to prevent unconditional DQL in next consume
        dead.headers().remove(DESERIALIZATION_FAILURE_DLQ);
        log.messageNackedDeadLetter(channel, topic);
        return producer.send(dead)
                .onFailure().invoke(t -> reportFailure.accept((Throwable) t, true))
                .onItem().ignore().andContinueWithNull()
                .chain(() -> Uni.createFrom().completionStage(record.ack()))
                .emitOn(record::runOnMessageContext);
    }

    void addHeader(ProducerRecord<?, ?> record, String key, String value) {
        record.headers().add(key, value.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void terminate() {
        producer.close();
    }
}
