package io.smallrye.reactive.messaging.aws.sqs.message;

public abstract class SqsMessageMetadata {

    private String queue;
    private String queueOwnerAWSAccountId;
    private String conversationId;


    /**
     * Get the name of the queue
     *
     * @return the queue name
     */
    public String getQueue() {
        return queue;
    }

    public SqsMessageMetadata withQueue(String queue) {
        this.queue = queue;
        return this;
    }

    /**
     * During queue name resolving it is possible to overwrite the AWS account id. If not specified the
     * AWS accounts id from the provided client credentials are used
     *
     * @return overwritten AWS account id
     */
    public String getQueueOwnerAWSAccountId() {
        return queueOwnerAWSAccountId;
    }

    public SqsMessageMetadata withQueueOwnerAWSAccountId(String queueOwnerAWSAccountId) {
        this.queueOwnerAWSAccountId = queueOwnerAWSAccountId;
        return this;
    }

    public String getConversationId() {
        return conversationId;
    }

    public SqsMessageMetadata withConversationId(String conversationId) {
        this.conversationId = conversationId;
        return this;
    }
}
