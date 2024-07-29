package com.linbit.linstor.event.serializer;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.event.LinstorEvent;

/**
 * Serializer for a specific event.
 * The actual implementation is obtained using {@link #get()}.
 * This makes it possible to have a collection of serializers without type-safety difficulties.
 */
public interface EventSerializer
{
    Serializer get();

    interface Serializer<T>
    {
        /**
         * Serialize an event value.
         */
        byte[] writeEventValue(@Nullable T value);

        /**
         * Get the corresponding event source.
         */
        LinstorEvent<T> getEvent();
    }
}
