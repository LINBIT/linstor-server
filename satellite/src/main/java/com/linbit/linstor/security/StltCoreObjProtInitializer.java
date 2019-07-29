package com.linbit.linstor.security;

import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.DatabaseException;

import javax.inject.Inject;

public class StltCoreObjProtInitializer
{
    private final AccessContext initCtx;
    private final ShutdownProtHolder shutdownProtHolder;
    private final ObjectProtectionFactory objectProtectionFactory;

    @Inject
    public StltCoreObjProtInitializer(
        @SystemContext AccessContext initCtxRef,
        ShutdownProtHolder shutdownProtHolderRef,
        ObjectProtectionFactory objectProtectionFactoryRef
    )
    {
        initCtx = initCtxRef;
        shutdownProtHolder = shutdownProtHolderRef;
        objectProtectionFactory = objectProtectionFactoryRef;
    }

    public void initialize() throws AccessDeniedException, DatabaseException
    {
        ObjectProtection shutdownProt = objectProtectionFactory.getInstance(
            initCtx,
            ObjectProtection.buildPathController("shutdown"),
            true
        );

        shutdownProtHolder.setShutdownProt(shutdownProt);
    }
}
