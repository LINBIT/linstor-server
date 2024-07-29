package com.linbit.linstor.event.handler;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.event.EventIdentifier;

import java.io.IOException;
import java.io.InputStream;

public interface EventHandler
{
    void execute(String eventAction, EventIdentifier eventIdentifier, @Nullable InputStream eventDataIn)
        throws IOException;
}
