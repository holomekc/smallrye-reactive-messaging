package io.smallrye.reactive.messaging.aws.sqs.action;

import java.util.Map;

import io.smallrye.mutiny.Uni;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

public class SendMessageAction extends Action {

    public Uni<Void> sendMessage() {

        final SendMessageRequest request = SendMessageRequest.builder()
                .queueUrl("")
                .messageAttributes(Map.of())
                .messageGroupId("")
                .messageBody("")
                .build();

        return Uni.createFrom().completionStage(() -> client.sendMessage(request))
                .onItem().invoke(response -> {
                    response.messageId();
                    response.sequenceNumber();
                    response.md5OfMessageBody();
                    response.md5OfMessageAttributes();
                })
                .replaceWithVoid();
    }
}
