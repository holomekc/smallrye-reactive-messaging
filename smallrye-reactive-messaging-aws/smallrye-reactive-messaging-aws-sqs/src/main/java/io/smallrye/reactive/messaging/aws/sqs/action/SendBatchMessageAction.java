package io.smallrye.reactive.messaging.aws.sqs.action;

import static io.smallrye.reactive.messaging.aws.sqs.i18n.SqsExceptions.ex;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.OutgoingMessageMetadata;
import io.smallrye.reactive.messaging.aws.sqs.SqsConnectorOutgoingConfiguration;
import io.smallrye.reactive.messaging.aws.sqs.Target;
import io.smallrye.reactive.messaging.aws.sqs.client.SqsClientHolder;
import io.smallrye.reactive.messaging.aws.sqs.message.SqsOutgoingMessage;
import io.smallrye.reactive.messaging.aws.sqs.util.Helper;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;

public class SendBatchMessageAction {

    public static Uni<Void> sendMessage(
            SqsClientHolder<SqsConnectorOutgoingConfiguration> clientHolder,
            Target target, List<? extends SqsOutgoingMessage<?>> messages) {
        // We need to remember the correlation between messageId and outgoing message.
        Map<String, SqsOutgoingMessage<?>> messageMap = new HashMap<>();

        SendMessageBatchRequest request = createRequest(clientHolder, target, messages, messageMap);

        // TODO: logging
        Uni<Void> uni = Uni.createFrom().completionStage(clientHolder.getClient().sendMessageBatch(request))
                .onItem().transformToUni(response -> handleResponse(response, messageMap));

        // TODO: configurable retry. Retry complete batch in case the complete HTTP request fails
        //  but what to do in case just specific messages fails? In theory most efficient would be to retry
        //  them by adding them back to the stream. This seems very difficult. Another option is to do the configured
        //  retries for the failed once immediately until all are successful or max retries reached.
        if (true) {
            uni = uni.onFailure().retry()
                    .withBackOff(Duration.ofMillis(0), Duration.ofMillis(0))
                    .atMost(3);
        }

        // TODO: micrometer? Failure and Success?

        return uni;
    }

    private static SendMessageBatchRequest createRequest(
            SqsClientHolder<SqsConnectorOutgoingConfiguration> clientHolder,
            Target target, List<? extends SqsOutgoingMessage<?>> messages,
            Map<String, SqsOutgoingMessage<?>> messageMap) {
        Map<String, SendMessageBatchRequestEntry> entryMap = new HashMap<>();

        messages.forEach(msg -> {
            String payload = Helper.serialize(msg, clientHolder.getJsonMapping());
            String id = UUID.randomUUID().toString();

            final SendMessageBatchRequestEntry entry = SendMessageBatchRequestEntry.builder()
                    // in batching we need to generate the id of a message for every entry.
                    .id(id)
                    .messageAttributes(null)
                    .messageGroupId(null)
                    .messageBody(payload)

                    .messageDeduplicationId(null)
                    .delaySeconds(0)
                    .messageSystemAttributesWithStrings(null)
                    .build();
            messageMap.put(id, msg);
            entryMap.put(id, entry);
        });

        return SendMessageBatchRequest.builder()
                .queueUrl(target.getTargetUrl())
                .entries(entryMap.values())
                .build();
    }

    private static Uni<Void> handleResponse(
            SendMessageBatchResponse response, Map<String, SqsOutgoingMessage<?>> messageMap) {
        Uni<Void> successful = Multi.createFrom().iterable(response.successful())
                .onItem().call(result -> {
                    OutgoingMessageMetadata.setResultOnMessage(messageMap.get(result.id()), result);
                    return Uni.createFrom().completionStage(messageMap.get(result.id()).ack());
                }).collect().last().replaceWithVoid();

        Uni<Void> failed = Multi.createFrom().iterable(response.failed())
                .onItem().call(result -> {
                    OutgoingMessageMetadata.setResultOnMessage(messageMap.get(result.id()), result);
                    return Uni.createFrom().completionStage(messageMap.get(result.id())
                            .nack(ex.illegalStateUnableToBuildClient(
                                    new IllegalStateException(result.code() + ": " + result.message()))));
                }).collect().last().replaceWithVoid();

        return Uni.combine().all().unis(successful, failed).discardItems();
    }
}
