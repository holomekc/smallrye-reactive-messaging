package io.smallrye.reactive.messaging.aws.sqs.action;

import io.smallrye.reactive.messaging.aws.sqs.SqsConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.aws.sqs.SqsConnectorOutgoingConfiguration;
import io.smallrye.reactive.messaging.json.JsonMapping;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

public class Action {

    protected SqsAsyncClient client;
    protected JsonMapping jsonMapping;

    protected SqsConnectorIncomingConfiguration incomingConfig;
    protected SqsConnectorOutgoingConfiguration outgoingConfig;

    protected Action(final Action action) {
        this.client = action.client;
        this.jsonMapping = action.jsonMapping;
        this.incomingConfig = action.incomingConfig;
        this.outgoingConfig = action.outgoingConfig;
    }

    public Action() {
    }

    public static Action of() {
        // TODO: init stuff
        final Action action = new Action();
        return action;
    }
}
