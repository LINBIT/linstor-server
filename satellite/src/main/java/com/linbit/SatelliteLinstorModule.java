package com.linbit;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

import com.linbit.linstor.FreeSpaceMgr;
import com.linbit.linstor.FreeSpaceMgrName;
import com.linbit.linstor.FreeSpaceMgrProtectionRepository;
import com.linbit.linstor.FreeSpaceMgrRepository;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.sql.SQLException;

public class SatelliteLinstorModule extends AbstractModule
{
    // Name for worker pool for satellite services operations - DeviceManager, etc.
    public static final String STLT_WORKER_POOL_NAME = "StltWorkerPool";

    @Override
    protected void configure()
    {
        bind(FreeSpaceMgrRepository.class).to(FreeSpaceMgrProtectionRepository.class);
    }

    @Provides
    @Singleton
    @Named(STLT_WORKER_POOL_NAME)
    public WorkQueue initializeStltWorkerThreadPool(ErrorReporter errorLog)
    {
        return WorkerPoolInitializer.createDevMgrWorkerThreadPool(
            errorLog,
            null,
            "StltWorkerPool"
        );
    }

    @Provides
    @Singleton
    @Named(LinStor.DISKLESS_FREE_SPACE_MGR_NAME)
    public FreeSpaceMgr initializeDisklessFreeSpaceMgr(
        @SystemContext AccessContext initCtx,
        ObjectProtectionFactory objProtFactory,
        Provider<TransactionMgr> transMgrProvider,
        TransactionObjectFactory transObjFactory
    )
    {
        FreeSpaceMgrName fsmName;
        FreeSpaceMgr freeSpaceMgr;
        try
        {
            fsmName = FreeSpaceMgrName.createReservedName(LinStor.DISKLESS_FREE_SPACE_MGR_NAME);
            freeSpaceMgr = new FreeSpaceMgr(
                initCtx,
                objProtFactory.getInstance(
                    initCtx,
                    ObjectProtection.buildPath(fsmName),
                    true
                ),
                fsmName,
                transMgrProvider,
                transObjFactory
            );
        }
        catch (InvalidNameException | AccessDeniedException | SQLException exc)
        {
            throw new ImplementationError(exc);
        }
        return freeSpaceMgr;
    }
}
