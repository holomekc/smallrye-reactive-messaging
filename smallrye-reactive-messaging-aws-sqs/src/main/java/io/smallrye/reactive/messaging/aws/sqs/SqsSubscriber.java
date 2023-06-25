package io.smallrye.reactive.messaging.aws.sqs;

import java.time.Duration;
import java.util.concurrent.Flow;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.aws.sqs.action.SendBatchMessageAction;
import io.smallrye.reactive.messaging.aws.sqs.action.SendMessageAction;
import io.smallrye.reactive.messaging.json.JsonMapping;
import io.smallrye.reactive.messaging.providers.helpers.MultiUtils;

public class SqsSubscriber {

    private final SqsConnectorOutgoingConfiguration config;
    private final SendMessageAction sendMessageAction;
    private final SendBatchMessageAction sendBatchMessageAction;

    public SqsSubscriber(final SqsConnectorOutgoingConfiguration config, final JsonMapping jsonMapping) {
        this.config = config;
        this.sendMessageAction = new SendMessageAction();
        this.sendBatchMessageAction = new SendBatchMessageAction();
    }

    public Flow.Subscriber<? extends Message<?>> getSubscriber() {
        final boolean batchingEnabled = config.getBatchSendMessageEnabled();

        return MultiUtils.via(m -> {
            if (batchingEnabled) {
                return m.group().intoLists().of(config.getBatchSendMessageMaxSize(),
                                Duration.ofMillis(config.getBatchSendMessageMaxWindow()))
                        .onItem().call(sendBatchMessageAction::sendMessage)
                        .onItem().transform(ignored -> "");
            } else {
                return m
                        .onItem().call(sendMessageAction::sendMessage)
                        .onItem().transform(ignored -> "");
            }
        });
    }
}
