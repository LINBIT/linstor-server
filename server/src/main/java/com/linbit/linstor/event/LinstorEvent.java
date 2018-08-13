package com.linbit.linstor.event;

import reactor.core.publisher.Flux;

public interface LinstorEvent<T>
{
    Flux<ObjectSignal<T>> watchForStreams(ObjectIdentifier ancestor);
}
