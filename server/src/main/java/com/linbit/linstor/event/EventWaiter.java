package com.linbit.linstor.event;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Duration;

@Singleton
public class EventWaiter
{
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(5L);

    private final Scheduler scheduler;

    @Inject
    public EventWaiter(Scheduler schedulerRef)
    {
        scheduler = schedulerRef;
    }

    /**
     * Wait for a stream to appear with a default timeout and return the stream.
     */
    public <T> Flux<T> waitForStream(
        LinstorEvent<T> event,
        ObjectIdentifier objectIdentifier
    )
    {
        return event.watchForStreams(objectIdentifier)
            .map(ObjectSignal::getSignal)
            // do not use Flux#dematerialize due to https://github.com/reactor/reactor-core/issues/585
            .<T>handle((signal, sink) ->
                {
                    if (signal.isOnComplete())
                    {
                        // The flux completes when the event stream is closed, which is not expected here
                        sink.error(new EventStreamClosedException());
                    }
                    else
                    if (signal.isOnError())
                    {
                        sink.error(signal.getThrowable());
                    }
                    else
                    if (signal.isOnNext())
                    {
                        sink.next(signal.get());
                    }
                }
            )
            .timeout(
                Mono.delay(DEFAULT_TIMEOUT, scheduler),
                ignored -> Mono.never(),
                Flux.error(new EventStreamTimeoutException())
            );
    }
}
