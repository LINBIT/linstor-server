package com.linbit.linstor.event.serializer.protobuf.common;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.event.LinstorEvent;
import com.linbit.linstor.event.WatchableObject;
import com.linbit.linstor.event.common.ConnectionStateEvent;
import com.linbit.linstor.event.serializer.EventSerializer;
import com.linbit.linstor.event.serializer.protobuf.ProtobufEventSerializer;

import javax.inject.Inject;
import javax.inject.Singleton;

@ProtobufEventSerializer(
    eventName = InternalApiConsts.EVENT_CONNECTION_STATE,
    objectType = WatchableObject.CONNECTION
)

@Singleton
public class ConnectionStateEventSerializer implements EventSerializer, EventSerializer.Serializer<String>
{
    private final CommonSerializer commonSerializer;
    private final ConnectionStateEvent connectionStateEvent;

    @Inject
    public ConnectionStateEventSerializer(
        CommonSerializer commonSerializerRef,
        ConnectionStateEvent connectionStateEventRef
    )
    {
        commonSerializer = commonSerializerRef;
        connectionStateEvent = connectionStateEventRef;
    }

    @Override
    public Serializer get()
    {
        return this;
    }

    @Override
    public byte[] writeEventValue(String state)
    {
        return commonSerializer.headerlessBuilder().connectionState(state).build();
    }

    @Override
    public LinstorEvent getEvent()
    {
        return connectionStateEvent.get();
    }
}
