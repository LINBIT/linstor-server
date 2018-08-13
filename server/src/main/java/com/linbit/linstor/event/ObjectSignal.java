package com.linbit.linstor.event;

import reactor.core.publisher.Signal;

public class ObjectSignal<T>
{
    private final ObjectIdentifier objectIdentifier;

    private final Signal<T> signal;

    public ObjectSignal(ObjectIdentifier objectIdentifierRef, Signal<T> signalRef)
    {
        objectIdentifier = objectIdentifierRef;
        signal = signalRef;
    }

    public ObjectIdentifier getObjectIdentifier()
    {
        return objectIdentifier;
    }

    public Signal<T> getSignal()
    {
        return signal;
    }
}
