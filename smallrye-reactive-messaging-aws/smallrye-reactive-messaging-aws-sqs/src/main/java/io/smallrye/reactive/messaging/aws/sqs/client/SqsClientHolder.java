package io.smallrye.reactive.messaging.aws.sqs.client;

import io.smallrye.reactive.messaging.aws.sqs.SqsConnectorCommonConfiguration;
import io.smallrye.reactive.messaging.aws.sqs.SqsTargetResolver;
import io.smallrye.reactive.messaging.json.JsonMapping;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

public class SqsClientHolder<C extends SqsConnectorCommonConfiguration> {
    private final SqsAsyncClient client;
    private final C config;
    private final JsonMapping jsonMapping;
    private final SqsTargetResolver targetResolver;

    public SqsClientHolder(SqsAsyncClient client, C config, JsonMapping jsonMapping, SqsTargetResolver targetResolver) {
        this.client = client;
        this.config = config;
        this.jsonMapping = jsonMapping;
        this.targetResolver = targetResolver;
    }

    public SqsAsyncClient getClient() {
        return client;
    }

    public C getConfig() {
        return config;
    }

    public JsonMapping getJsonMapping() {
        return jsonMapping;
    }

    public SqsTargetResolver getTargetCache() {
        return targetResolver;
    }
}
