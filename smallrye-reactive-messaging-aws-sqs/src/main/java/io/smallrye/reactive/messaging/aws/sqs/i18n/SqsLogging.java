package io.smallrye.reactive.messaging.aws.sqs.i18n;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.MessageLogger;

@MessageLogger(projectCode = "SRMSG", length = 5)
public interface SqsLogging extends BasicLogger {

    SqsLogging log = Logger.getMessageLogger(SqsLogging.class, "io.smallrye.reactive.messaging.aws.sqs");
}
