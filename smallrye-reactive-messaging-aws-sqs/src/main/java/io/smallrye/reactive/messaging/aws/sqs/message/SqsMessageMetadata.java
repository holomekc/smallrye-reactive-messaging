package io.smallrye.reactive.messaging.aws.sqs.message;

import java.util.Map;

import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeValue;

public interface SqsMessageMetadata {

    String getQueue();

    String getQueueUrl();

    Map<String, MessageAttributeValue> getMessageAttributes();

    Map<String, MessageSystemAttributeValue> getMessageSystemAttributes();

}
