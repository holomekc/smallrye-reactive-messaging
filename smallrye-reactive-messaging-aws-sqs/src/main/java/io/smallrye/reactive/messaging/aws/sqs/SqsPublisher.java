package io.smallrye.reactive.messaging.aws.sqs;

import java.util.concurrent.Flow;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.aws.sqs.action.GetQueueUrlAction;
import io.smallrye.reactive.messaging.aws.sqs.action.ReceiveMessageAction;
import io.smallrye.reactive.messaging.json.JsonMapping;

public class SqsPublisher {

    private final SqsConnectorIncomingConfiguration config;
    private final ReceiveMessageAction receiveMessageAction;
    private final GetQueueUrlAction getQueueUrlAction;

    public SqsPublisher(final SqsConnectorIncomingConfiguration config, final JsonMapping jsonMapping) {
        this.config = config;
        this.receiveMessageAction = new ReceiveMessageAction();
        this.getQueueUrlAction = new GetQueueUrlAction();
    }

    public Flow.Publisher<? extends Message<?>> getPublisher() {
        return getQueueUrlAction.getQueueUrl(config.getQueue())
                .onItem().transformToMulti(url -> this.receiveMessageAction.createPublisher());
    }
}
