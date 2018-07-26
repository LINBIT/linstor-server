package com.linbit.linstor.event.writer.protobuf;

import com.linbit.linstor.event.WatchableObject;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Retention(RUNTIME)
@Target(TYPE)
public @interface ProtobufEventWriter
{
    String eventName();

    WatchableObject objectType();
}
