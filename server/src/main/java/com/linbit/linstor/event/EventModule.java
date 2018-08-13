package com.linbit.linstor.event;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.MapBinder;
import com.linbit.linstor.event.handler.EventHandler;
import com.linbit.linstor.event.handler.protobuf.ProtobufEventHandler;
import com.linbit.linstor.event.serializer.EventSerializer;

import java.util.List;

public class EventModule extends AbstractModule
{
    private final List<Class<? extends EventSerializer>> eventSerializers;
    private final List<Class<? extends EventHandler>> eventHandlers;

    public EventModule(
        List<Class<? extends EventSerializer>> eventSerializersRef,
        List<Class<? extends EventHandler>> eventHandlersRef
    )
    {
        eventSerializers = eventSerializersRef;
        eventHandlers = eventHandlersRef;
    }

    @Override
    protected void configure()
    {
        bind(WatchStore.class).to(WatchStoreImpl.class);

        MapBinder<String, EventSerializer> eventSerializerBinder =
            MapBinder.newMapBinder(binder(), String.class, EventSerializer.class);
        MapBinder<String, EventSerializerDescriptor> eventSerializerDescriptorBinder =
            MapBinder.newMapBinder(binder(), String.class, EventSerializerDescriptor.class);
        for (Class<? extends EventSerializer> eventSerializer : eventSerializers)
        {
            EventSerializerDescriptor descriptor = new EventSerializerDescriptor(eventSerializer);
            eventSerializerBinder.addBinding(descriptor.getEventName()).to(eventSerializer);
            eventSerializerDescriptorBinder.addBinding(descriptor.getEventName()).toInstance(descriptor);
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
