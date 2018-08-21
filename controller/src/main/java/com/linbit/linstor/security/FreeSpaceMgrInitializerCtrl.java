package com.linbit.linstor.security;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.FreeSpaceMgr;
import com.linbit.linstor.FreeSpaceMgrName;
import com.linbit.linstor.FreeSpaceMgrRepository;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.annotation.PublicContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.transaction.ControllerTransactionMgr;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import java.sql.SQLException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

public class FreeSpaceMgrInitializerCtrl
{
    private final AccessContext initCtx;
    private final AccessContext pubCtx;
    private final LinStorScope initScope;
    private final ControllerDatabase dbConnPool;
    private final ReadWriteLock reconfigurationLock;

    private final FreeSpaceMgrRepository freeSpaceMgrRepository;
    private final ObjectProtectionFactory objProtFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final TransactionObjectFactory transObjFactory;

    @Inject
    public FreeSpaceMgrInitializerCtrl(
        @SystemContext AccessContext initCtxRef,
        @PublicContext AccessContext pubCtxRef,
        LinStorScope initScopeRef,
        ControllerDatabase dbConnPoolRef,
        FreeSpaceMgrRepository freeSpaceMgrRepositoryRef,
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        ObjectProtectionFactory objProtFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        initCtx = initCtxRef;
        pubCtx = pubCtxRef;
        initScope = initScopeRef;
        dbConnPool = dbConnPoolRef;
        freeSpaceMgrRepository = freeSpaceMgrRepositoryRef;
        reconfigurationLock = reconfigurationLockRef;
        objProtFactory = objProtFactoryRef;
        transMgrProvider = transMgrProviderRef;
        transObjFactory = transObjFactoryRef;
    }

    public void initialize() throws InitializationException
    {
        TransactionMgr transMgr = null;
        Lock recfgWriteLock = reconfigurationLock.writeLock();
        try
        {
            transMgr = new ControllerTransactionMgr(dbConnPool);
            initScope.enter();
            initScope.seed(TransactionMgr.class, transMgr);

            // Replacing the entire configuration requires locking out all other tasks
            //
            // Since others task that use the configuration must hold the reconfiguration lock
            // in read mode before locking any of the other system objects, locking the maps
            // for nodes, resource definition, storage pool definitions, etc. can be skipped.
            recfgWriteLock.lock();

            initializeDisklessFreeSpaceMgr();
            transMgr.commit();

            initScope.exit();
        }
        catch (SQLException exc)
        {
            throw new InitializationException(
                "Initial load from the database failed",
                exc
            );
        }
        finally
        {
            recfgWriteLock.unlock();
            if (transMgr != null)
            {
                try
                {
                    transMgr.rollback();
                }
                catch (SQLException exc)
                {
                    throw new InitializationException(
                        "Rollback after initial load from the database failed",
                        exc
                    );
                }
                transMgr.returnConnection();
            }
        }
    }

    private void initializeDisklessFreeSpaceMgr()
    {
        try
        {
            FreeSpaceMgrName fsmName = FreeSpaceMgrName.createReservedName(LinStor.DISKLESS_FREE_SPACE_MGR_NAME);
            FreeSpaceMgr freeSpaceMgr = new FreeSpaceMgr(
                initCtx,
                objProtFactory.getInstance(
                    pubCtx,
                    ObjectProtection.buildPath(fsmName),
                    true
                ),
                fsmName,
                transMgrProvider,
                transObjFactory
            );
            freeSpaceMgrRepository.put(initCtx, fsmName, freeSpaceMgr);
        }
        catch (InvalidNameException exc)
        {
            throw new ImplementationError(
                "Invalid hardcoded default diskless free space manager name",
                exc
            );
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(
                "Access denied while creating default diskless free space manager",
                exc
            );
        }
        catch (SQLException exc)
        {
            throw new ImplementationError(
                "An exception occured while creating default diskless free space manager",
                exc
            );
        }
    }
}
