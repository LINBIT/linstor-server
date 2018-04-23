package com.linbit.linstor.event.writer;

import com.linbit.linstor.event.ObjectIdentifier;

public interface EventWriter
{
    byte[] writeEvent(ObjectIdentifier objectIdentifier) throws Exception;
}
