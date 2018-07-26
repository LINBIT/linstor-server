package com.linbit.linstor.event.handler;

import com.linbit.linstor.event.EventIdentifier;

import java.io.IOException;
import java.io.InputStream;

public interface EventHandler
{
    void execute(String eventAction, EventIdentifier eventIdentifier, InputStream eventDataIn)
        throws IOException;
}
