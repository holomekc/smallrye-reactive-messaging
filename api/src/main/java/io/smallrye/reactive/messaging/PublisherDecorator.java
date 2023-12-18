package io.smallrye.reactive.messaging;

import java.util.List;

import jakarta.enterprise.inject.spi.Prioritized;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.common.annotation.Experimental;
import io.smallrye.mutiny.Multi;

/**
 * SPI to allow extension of publishers (Multi) included in the final graph.
 *
 * {@code PublisherDecorator}s are invoked higher priority first (from the least value to the greatest).
 *
 * The decorator priority is obtained with the {@link #getPriority()} method.
 * The default priority is {@link #DEFAULT_PRIORITY}.
 */
@Experimental("SmallRye only feature")
public interface PublisherDecorator extends Prioritized {

    /**
     * Default priority
     */
    int DEFAULT_PRIORITY = 1000;

    /**
     * Decorate a Multi
     *
     * @param publisher the multi to decorate
     * @param channelName the name of the channel to which this publisher publishes
     * @param isConnector {@code true} if decorated channel is connector
     * @return the extended multi
     * @deprecated replaced with {@link #decorate(Multi, List, boolean)}
     */
    @Deprecated(since = "4.12.0")
    default Multi<? extends Message<?>> decorate(Multi<? extends Message<?>> publisher, String channelName,
            boolean isConnector) {
        return publisher;
    }

    /**
     * Decorate a Multi
     *
     * @param publisher the multi to decorate
     * @param @param channelName the list of channel names from which this publisher publishes
     * @param isConnector {@code true} if decorated channel is connector
     * @return the extended multi
     */
    default Multi<? extends Message<?>> decorate(Multi<? extends Message<?>> publisher, List<String> channelName,
            boolean isConnector) {
        return decorate(publisher, channelName.isEmpty() ? null : channelName.get(0), isConnector);
    }

    @Override
    default int getPriority() {
        return DEFAULT_PRIORITY;
    }

}
