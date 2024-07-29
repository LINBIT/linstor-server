package com.linbit.linstor.security;

import com.linbit.linstor.annotation.Nullable;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Holds the singleton shutdown protection instance, allowing it to be initialized from the database after
 * dependency injection has been performed.
 */
@Singleton
public class ShutdownProtHolder implements ProtectedObject
{
    private @Nullable ObjectProtection shutdownProt;

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

    @Override
    public ObjectProtection getObjProt()
    {
        if (shutdownProt == null)
        {
            throw new IllegalStateException("Object protection not yet set");
        }
        return shutdownProt;
    }
}
