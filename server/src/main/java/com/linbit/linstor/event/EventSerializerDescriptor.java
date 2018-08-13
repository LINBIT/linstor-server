package com.linbit.linstor.event;

import com.linbit.linstor.event.serializer.EventSerializer;
import com.linbit.linstor.event.serializer.protobuf.ProtobufEventSerializer;

public class EventSerializerDescriptor
{
    private final Class<? extends EventSerializer> clazz;
    private final String eventName;
    private final WatchableObject objectType;

    public EventSerializerDescriptor(Class<? extends EventSerializer> clazzRef)
    {
        clazz = clazzRef;
        eventName = clazzRef.getAnnotation(ProtobufEventSerializer.class).eventName();
        objectType = clazzRef.getAnnotation(ProtobufEventSerializer.class).objectType();
    }

    public Class<? extends EventSerializer> getClazz()
    {
        return clazz;
    }

    public String getEventName()
    {
        return eventName;
    }

    public WatchableObject getObjectType()
    {
        return objectType;
    }
}
