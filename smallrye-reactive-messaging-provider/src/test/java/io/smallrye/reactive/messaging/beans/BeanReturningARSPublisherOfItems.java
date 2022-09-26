package io.smallrye.reactive.messaging.beans;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Publisher;

@ApplicationScoped
public class BeanReturningARSPublisherOfItems {

    @Outgoing("producer")
    public Publisher<String> create() {
        return ReactiveStreams.of("a", "b", "c").buildRs();
    }

}
