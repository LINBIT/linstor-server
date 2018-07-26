package com.linbit.linstor.event;

import com.linbit.linstor.event.writer.EventWriter;
import com.linbit.linstor.event.writer.protobuf.ProtobufEventWriter;

public class EventWriterDescriptor
{
    private final Class<? extends EventWriter> clazz;
    private final String eventName;
    private final WatchableObject objectType;

    public EventWriterDescriptor(Class<? extends EventWriter> clazzRef)
    {
        clazz = clazzRef;
        eventName = clazzRef.getAnnotation(ProtobufEventWriter.class).eventName();
        objectType = clazzRef.getAnnotation(ProtobufEventWriter.class).objectType();
    }

    public Class<? extends EventWriter> getClazz()
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
