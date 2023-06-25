package io.smallrye.reactive.messaging.aws.sqs.action;

import java.util.Set;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

public class ReceiveMessageAction extends Action {

    public Multi<? extends Message<?>> createPublisher() {
        return Multi.createBy().repeating().uni(this::poll)
                .until(a -> true)
                // TODO: message counting
                .skip().where(response -> !response.hasMessages())

                // TODO: merge with configurable concurrency
                .onItem().transformToMulti(this::createMultiOfMessages).merge();
    }

    private Multi<? extends Message<?>> createMultiOfMessages(final ReceiveMessageResponse response) {

        return Multi.createFrom().iterable(response.messages())

                .onItem().invoke(message -> {

                    message.messageAttributes();
                    message.attributes();
                    message.messageId();
                    message.receiptHandle();

                })

                // TODO: Maybe skip messages failed during deserialization.
                .onItem().transform(ignore -> null);

        // TODO: confirm / delete messages via delete / delete batch
    }

    private Uni<ReceiveMessageResponse> poll() {
        // TODO: delay, graceful shutdown, error handling, health?
        return Uni.createFrom().completionStage(() -> client.receiveMessage(createRequest()));
    }

    private ReceiveMessageRequest createRequest() {
        // TODO: config, queue url or generic target

        return ReceiveMessageRequest.builder().queueUrl("")
                .maxNumberOfMessages(2)
                .waitTimeSeconds(2)
                .visibilityTimeout(2)
                .attributeNames(Set.of())
                .messageAttributeNames(Set.of())
                .build();
    }
}
