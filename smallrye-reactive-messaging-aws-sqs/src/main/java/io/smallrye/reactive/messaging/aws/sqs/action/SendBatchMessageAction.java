package io.smallrye.reactive.messaging.aws.sqs.action;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.aws.sqs.message.OutgoingSqsMessageMetadata;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;

public class SendBatchMessageAction extends Action {
    public Uni<Void> sendMessage(final List<Message<?>> messages) {

        final List<SendMessageBatchRequestEntry> entries = messages.stream().map(msg -> {
            final SendMessageBatchRequestEntry.Builder builder = SendMessageBatchRequestEntry.builder()
                    .id(UUID.randomUUID().toString())
                    .messageBody(jsonMapping.toJson(msg.getPayload()));

            final Optional<OutgoingSqsMessageMetadata> om = msg.getMetadata(OutgoingSqsMessageMetadata.class);

            om.ifPresent(metadata -> builder
                    .messageAttributes(metadata.getMessageAttributes())
                    .messageSystemAttributesWithStrings(metadata.getMessageSystemAttributes())
                    .messageGroupId(metadata.getGroupId())
                    .id(metadata.getId())
            );

            return builder.build();
        }).collect(Collectors.toList());

        final SendMessageBatchRequest request =
                SendMessageBatchRequest.builder().queueUrl(outgoingConfig.getQueue()).entries(entries).build();

        return Uni.createFrom().completionStage(() -> client.sendMessageBatch(request))
                .onItem().invoke(response -> {
                    response.hasSuccessful();

                })
                .replaceWithVoid();
    }
}
