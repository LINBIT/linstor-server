package com.linbit.linstor.security;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Holds the singleton shutdown protection instance, allowing it to be initialized from the database after
 * dependency injection has been performed.
 */
@Singleton
public class ShutdownProtHolder
{
    private ObjectProtection shutdownProt;

    @Inject
    public ShutdownProtHolder()
    {
    }

    public void setShutdownProt(ObjectProtection shutdownProtRef)
    {
        if (shutdownProt != null)
        {
            throw new IllegalStateException("Object protection already set");
        }
        shutdownProt = shutdownProtRef;
    }

    public ObjectProtection getShutdownProt()
    {
        if (shutdownProt == null)
        {
            throw new IllegalStateException("Object protection not yet set");
        }
        return shutdownProt;
    }
}
