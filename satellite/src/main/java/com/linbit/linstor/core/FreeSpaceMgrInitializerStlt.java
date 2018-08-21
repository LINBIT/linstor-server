package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.FreeSpaceMgr;
import com.linbit.linstor.FreeSpaceMgrName;
import com.linbit.linstor.FreeSpaceMgrProtectionRepository;
import com.linbit.linstor.FreeSpaceMgrRepository;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.transaction.SatelliteTransactionMgr;
import com.linbit.linstor.transaction.TransactionMgr;
import javax.inject.Inject;
import javax.inject.Named;
import java.sql.SQLException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

public class FreeSpaceMgrInitializerStlt
{
    private final AccessContext initCtx;
    private final LinStorScope initScope;
    private final ReadWriteLock reconfigurationLock;

    private final FreeSpaceMgrRepository freeSpaceMgrRepository;
    private final ObjectProtectionFactory objProtFactory;
    private final FreeSpaceMgr disklessFreeSpaceMgr;

    @Inject
    public FreeSpaceMgrInitializerStlt(
        @SystemContext AccessContext initCtxRef,
        LinStorScope initScopeRef,
        FreeSpaceMgrRepository freeSpaceMgrRepositoryRef,
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        ObjectProtectionFactory objProtFactoryRef,
        @Named(LinStor.DISKLESS_FREE_SPACE_MGR_NAME) FreeSpaceMgr disklessFreeSpaceMgrRef
    )
    {
        initCtx = initCtxRef;
        initScope = initScopeRef;
        freeSpaceMgrRepository = freeSpaceMgrRepositoryRef;
        reconfigurationLock = reconfigurationLockRef;
        objProtFactory = objProtFactoryRef;
        disklessFreeSpaceMgr = disklessFreeSpaceMgrRef;
    }

    public void initialize() throws InitializationException
    {
        TransactionMgr transMgr = null;
        Lock recfgWriteLock = reconfigurationLock.writeLock();
        try
        {
            transMgr = new SatelliteTransactionMgr();
            initScope.enter();
            initScope.seed(TransactionMgr.class, transMgr);

            // Replacing the entire configuration requires locking out all other tasks
            //
            // Since others task that use the configuration must hold the reconfiguration lock
            // in read mode before locking any of the other system objects, locking the maps
            // for nodes, resource definition, storage pool definitions, etc. can be skipped.
            recfgWriteLock.lock();

            try
            {
                ((FreeSpaceMgrProtectionRepository) freeSpaceMgrRepository).setObjectProtection(
                    objProtFactory.getInstance(
                        initCtx,
                        ObjectProtection.buildPathSatellite("freeSpaceMgrMap"),
                        true
                    )
                );
            }
            catch (AccessDeniedException | SQLException exc)
            {
                throw new ImplementationError(exc);
            }

System.out.println("objprot initialized");
            initializeDisklessFreeSpaceMgr();
            transMgr.commit();

            initScope.exit();
        }
        catch (SQLException exc)
        {
            throw new ImplementationError(exc);
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
            freeSpaceMgrRepository.put(
                initCtx,
                FreeSpaceMgrName.createReservedName(LinStor.DISKLESS_FREE_SPACE_MGR_NAME),
                disklessFreeSpaceMgr
            );
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
    }
}
