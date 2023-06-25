package io.smallrye.reactive.messaging.aws.sqs;

import static io.smallrye.reactive.messaging.aws.sqs.i18n.SqsLogging.log;

import java.util.concurrent.Flow;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;

import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.smallrye.reactive.messaging.connector.OutboundConnector;
import io.smallrye.reactive.messaging.json.JsonMapping;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.BeforeDestroyed;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.Reception;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

@ApplicationScoped
@Connector(SqsConnector.CONNECTOR_NAME)

@ConnectorAttribute(name = "queue", type = "string", direction = Direction.INCOMING_AND_OUTGOING,
        description = "The consumed / populated SQS queue.", mandatory = true)
@ConnectorAttribute(name = "health-enabled", type = "boolean", direction = Direction.INCOMING_AND_OUTGOING,
        description = "Whether health reporting is enabled (default) or disabled", defaultValue = "true")
@ConnectorAttribute(name = "tracing-enabled", type = "boolean", direction = Direction.INCOMING_AND_OUTGOING,
        description = "Whether tracing is enabled (default) or disabled", defaultValue = "true")

@ConnectorAttribute(name = "batch.send-message.enabled", type = "boolean", direction = Direction.OUTGOING,
        description = "Whether batching for send-message is enabled (default) or disabled", defaultValue = "true")
@ConnectorAttribute(name = "batch.send-message.max-size", type = "int", direction = Direction.OUTGOING,
        description = "Max size of a batch. Default is 10.", defaultValue = "10")
@ConnectorAttribute(name = "batch.send-message.max-window", type = "long", direction = Direction.OUTGOING,
        description = "Max batching window of a batch in milliseconds. Default is 3000.", defaultValue = "3000")

@ConnectorAttribute(name = "batch.delete-message.enabled", type = "boolean", direction = Direction.INCOMING,
        description = "Whether batching for delete-message is enabled (default) or disabled", defaultValue = "true")
@ConnectorAttribute(name = "batch.delete-message.max-size", type = "int", direction = Direction.INCOMING,
        description = "Max size of a batch. Default is 10.", defaultValue = "10")
@ConnectorAttribute(name = "batch.delete-message.max-window", type = "long", direction = Direction.INCOMING,
        description = "Max batching window of a batch in milliseconds. Default is 3000.", defaultValue = "3000")
public class SqsConnector implements InboundConnector, OutboundConnector {

    /**
     * The name of the connector: {@code smallrye-sqs}
     */
    public static final String CONNECTOR_NAME = "smallrye-sqs";

    @Inject
    Instance<JsonMapping> jsonMapper;

    private JsonMapping jsonMapping;

    @PostConstruct
    public void init() {
        if (jsonMapper.isUnsatisfied()) {
            log.warn(
                    "Please add one of the additional mapping modules (-jsonb or -jackson) to be able to (de)serialize JSON messages.");
        } else if (jsonMapper.isAmbiguous()) {
            log.warn(
                    "Please select only one of the additional mapping modules (-jsonb or -jackson) to be able to (de)serialize JSON messages.");
            this.jsonMapping = jsonMapper.stream().findFirst()
                    .orElseThrow(() -> new RuntimeException("Unable to find JSON Mapper"));
        } else {
            this.jsonMapping = jsonMapper.get();
        }
    }

    public void terminate(
            @Observes(notifyObserver = Reception.IF_EXISTS)
            @Priority(50)
            @BeforeDestroyed(ApplicationScoped.class)
            Object event) {
        // TODO: close stuff and destroy things like clear cache.
        //  Is this where graceful shutdown is a thing in smallrye messaging? I like the quarkus approach more :(
    }

    @Override
    public Flow.Publisher<? extends Message<?>> getPublisher(final Config config) {
        final SqsPublisher sqsPublisher = new SqsPublisher(new SqsConnectorIncomingConfiguration(config), jsonMapping);
        return sqsPublisher.getPublisher();
    }

    @Override
    public Flow.Subscriber<? extends Message<?>> getSubscriber(final Config config) {
        final SqsSubscriber sqsSubscriber =
                new SqsSubscriber(new SqsConnectorOutgoingConfiguration(config), jsonMapping);
        return sqsSubscriber.getSubscriber();
    }
}
