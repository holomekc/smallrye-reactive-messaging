package io.smallrye.reactive.messaging.aws.sqs.message;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;

public interface SqsMessage<T> extends Message<T>, ContextAwareMessage<T> {

    static <T> SqsMessage<T> of(final T value) {
        return new OutgoingSqsMessage<>(value);
    }
}
