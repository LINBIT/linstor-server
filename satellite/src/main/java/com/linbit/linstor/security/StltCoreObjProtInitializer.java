package com.linbit.linstor.security;

import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.api.LinStorScope.ScopeAutoCloseable;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.systemstarter.StartupInitializer;
import com.linbit.linstor.transaction.manager.SatelliteTransactionMgr;
import com.linbit.linstor.transaction.manager.TransactionMgrUtil;

import javax.inject.Inject;

public class StltCoreObjProtInitializer implements StartupInitializer
{
    private final AccessContext initCtx;
    private final LinStorScope linstorScope;
    private final ShutdownProtHolder shutdownProtHolder;
    private final ObjectProtectionFactory objectProtectionFactory;

    @Inject
    public StltCoreObjProtInitializer(
        @SystemContext AccessContext initCtxRef,
        LinStorScope linstorScopeRef,
        ShutdownProtHolder shutdownProtHolderRef,
        ObjectProtectionFactory objectProtectionFactoryRef
    )
    {
        initCtx = initCtxRef;
        linstorScope = linstorScopeRef;
        shutdownProtHolder = shutdownProtHolderRef;
        objectProtectionFactory = objectProtectionFactoryRef;
    }

    @Override
    public void initialize() throws AccessDeniedException, DatabaseException
    {
        SatelliteTransactionMgr transMgr = new SatelliteTransactionMgr();
        try (ScopeAutoCloseable scopeAutoCloseable = linstorScope.enter())
        {
            TransactionMgrUtil.seedTransactionMgr(linstorScope, transMgr);
            ObjectProtection shutdownProt = objectProtectionFactory.getInstance(
                initCtx,
                ObjectProtection.buildPathController("shutdown"),
                true
            );

            shutdownProtHolder.setShutdownProt(shutdownProt);
        }
    }
}
