package io.smallrye.reactive.messaging.aws.sqs.action;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.smallrye.mutiny.Uni;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;

public class GetQueueUrlAction extends Action {

    // TODO: Allow to override this and define any other kind of cache, or even the url provider.
    private static final Map<String, String> QUEUE_URL_CACHE = new ConcurrentHashMap<>();

    public Uni<String> getQueueUrl(final String queueName) {
        final String url = QUEUE_URL_CACHE.get(queueName);
        if (url == null) {
            return Uni.createFrom().completionStage(
                            () -> client.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build()))
                    .onItem().invoke(response -> QUEUE_URL_CACHE.put(queueName, response.queueUrl()))
                    .onItem().transform(GetQueueUrlResponse::queueUrl);
        } else {
            return Uni.createFrom().item(url);
        }
    }
}
