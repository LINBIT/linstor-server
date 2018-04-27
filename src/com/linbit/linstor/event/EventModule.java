package com.linbit.linstor.event;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.MapBinder;
import com.linbit.linstor.event.handler.EventHandler;
import com.linbit.linstor.event.handler.protobuf.ProtobufEventHandler;
import com.linbit.linstor.event.writer.EventWriter;

import java.util.List;

public class EventModule extends AbstractModule
{
    private final List<Class<? extends EventWriter>> eventWriters;
    private final List<Class<? extends EventHandler>> eventHandlers;

    public EventModule(
        List<Class<? extends EventWriter>> eventWritersRef,
        List<Class<? extends EventHandler>> eventHandlersRef
    )
    {
        eventWriters = eventWritersRef;
        eventHandlers = eventHandlersRef;
    }

    @Override
    protected void configure()
    {
        bind(WatchStore.class).to(WatchStoreImpl.class);
        bind(EventStreamStore.class).to(EventStreamStoreImpl.class);

        MapBinder<String, EventWriter> eventWriterBinder =
            MapBinder.newMapBinder(binder(), String.class, EventWriter.class);
        MapBinder<String, EventWriterDescriptor> eventWriterDescriptorBinder =
            MapBinder.newMapBinder(binder(), String.class, EventWriterDescriptor.class);
        for (Class<? extends EventWriter> eventWriter : eventWriters)
        {
            EventWriterDescriptor descriptor = new EventWriterDescriptor(eventWriter);
            eventWriterBinder.addBinding(descriptor.getEventName()).to(eventWriter);
            eventWriterDescriptorBinder.addBinding(descriptor.getEventName()).toInstance(descriptor);
        }

        MapBinder<String, EventHandler> eventHandlerBinder =
            MapBinder.newMapBinder(binder(), String.class, EventHandler.class);
        for (Class<? extends EventHandler> eventHandler : eventHandlers)
        {
            eventHandlerBinder.addBinding(eventHandler.getAnnotation(ProtobufEventHandler.class).eventName())
                .to(eventHandler);
        }
    }
}
