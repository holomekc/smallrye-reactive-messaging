package io.smallrye.reactive.messaging.aws.sqs.message;

import java.util.Map;
import java.util.UUID;

import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeValue;

public class OutgoingSqsMessageMetadata implements SqsMessageMetadata {
    @Override
    public String getQueue() {
        return null;
    }

    @Override
    public String getQueueUrl() {
        return null;
    }

    @Override
    public Map<String, MessageAttributeValue> getMessageAttributes() {
        return null;
    }

    @Override
    public Map<String, MessageSystemAttributeValue> getMessageSystemAttributes() {
        return null;
    }

    public String getGroupId() {
        return null;
    }

    public String getId() {
        return UUID.randomUUID().toString();
    }
}
