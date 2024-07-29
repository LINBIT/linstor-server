package com.linbit.linstor.systemstarter;

import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessDeniedException;

public interface StartupInitializer
{
    void initialize()
        throws InitializationException,
        AccessDeniedException,
        DatabaseException,
        SystemServiceStartException;

    default void shutdown()
    {
    }

    default void awaitShutdown(long timeout) throws InterruptedException
    {
    }

    default @Nullable SystemService getSystemService()
    {
        return null;
    }
}
