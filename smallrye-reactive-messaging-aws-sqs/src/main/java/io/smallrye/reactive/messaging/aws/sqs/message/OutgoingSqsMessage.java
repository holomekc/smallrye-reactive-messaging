package io.smallrye.reactive.messaging.aws.sqs.message;

public class OutgoingSqsMessage<T> implements SqsMessage<T> {

    private final T value;

    public OutgoingSqsMessage(final T value) {
        this.value = value;
    }

    @Override
    public T getPayload() {
        return this.value;
    }
}
