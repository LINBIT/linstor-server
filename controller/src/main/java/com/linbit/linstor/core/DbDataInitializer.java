package com.linbit.linstor.core;

import com.linbit.linstor.InitializationException;
import com.linbit.linstor.annotation.ErrorReporterContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.core.repository.StorPoolDefinitionRepository;
import com.linbit.linstor.dbdrivers.DatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDefinitionDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.systemstarter.StartupInitializer;
import com.linbit.linstor.transaction.TransactionException;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.transaction.manager.TransactionMgrGenerator;
import com.linbit.linstor.transaction.manager.TransactionMgrUtil;

import javax.inject.Inject;
import javax.inject.Named;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import com.google.inject.Key;

public class DbDataInitializer implements StartupInitializer
{
    private final ErrorReporter errorReporter;
    private final AccessContext initCtx;
    private final LinStorScope initScope;
    private final NodeRepository nodeRepository;
    private final ResourceDefinitionRepository resourceDefinitionRepository;
    private final StorPoolDefinitionRepository storPoolDefinitionRepository;
    private final ReadWriteLock reconfigurationLock;
    private final DatabaseDriver databaseDriver;
    private final StorPoolDefinitionDatabaseDriver storPoolDfnDbDriver;
    private final TransactionMgrGenerator transactionMgrGenerator;

    @Inject
    public DbDataInitializer(
        ErrorReporter errorReporterRef,
        @SystemContext AccessContext initCtxRef,
        LinStorScope initScopeRef,
        NodeRepository nodeRepositoryRef,
        ResourceDefinitionRepository resourceDefinitionRepositoryRef,
        StorPoolDefinitionRepository storPoolDefinitionRepositoryRef,
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        DatabaseDriver databaseDriverRef,
        StorPoolDefinitionDatabaseDriver storPoolDfnDbDriverRef,
        TransactionMgrGenerator transactionMgrGeneratorRef
    )
    {
        errorReporter = errorReporterRef;
        initCtx = initCtxRef;
        initScope = initScopeRef;
        nodeRepository = nodeRepositoryRef;
        resourceDefinitionRepository = resourceDefinitionRepositoryRef;
        storPoolDefinitionRepository = storPoolDefinitionRepositoryRef;
        reconfigurationLock = reconfigurationLockRef;
        databaseDriver = databaseDriverRef;
        storPoolDfnDbDriver = storPoolDfnDbDriverRef;
        transactionMgrGenerator = transactionMgrGeneratorRef;
    }

    @Override
    public void initialize()
        throws AccessDeniedException, InitializationException
    {
        TransactionMgr transMgr = null;
        Lock recfgWriteLock = reconfigurationLock.writeLock();
        boolean locked = false;

        InitializationException initExc = null;

        try (LinStorScope.ScopeAutoCloseable close = initScope.enter())
        {
            transMgr = transactionMgrGenerator.startTransaction();
            TransactionMgrUtil.seedTransactionMgr(initScope, transMgr);

            // rebuilding layerData also runs an additional check to verify the used storage pools
            // which needs a peerContext.
            initScope.seed(Key.get(AccessContext.class, PeerContext.class), initCtx);
            initScope.seed(Key.get(AccessContext.class, ErrorReporterContext.class), initCtx);


            // Replacing the entire configuration requires locking out all other tasks
            //
            // Since others task that use the configuration must hold the reconfiguration lock
            // in read mode before locking any of the other system objects, locking the maps
            // for nodes, resource definition, storage pool definitions, etc. can be skipped.
            recfgWriteLock.lock();
            locked = true;

            loadCoreObjects();
            initializeDisklessStorPoolDfn();

            transMgr.commit();
        }
        catch (DatabaseException exc)
        {
            /*
             * we cannot just throw here, as the finally block might also throw an exception.
             * The exception thrown in the finally block would simply override the exception
             * thrown here, in the catch block.
             */
            initExc = new InitializationException(
                "Initial load from the database failed",
                exc
            );
        }
        finally
        {
            if (locked)
            {
                recfgWriteLock.unlock();
            }
            if (transMgr != null)
            {
                try
                {
                    transMgr.rollback();
                }
                catch (TransactionException exc)
                {
                    InitializationException finallyInitExc = new InitializationException(
                        "Rollback after initial load from the database failed",
                        exc
                    );
                    if (initExc != null)
                    {
                        finallyInitExc.addSuppressed(initExc);
                    }
                    initExc = finallyInitExc;
                }
                transMgr.returnConnection();
            }
        }
        if (initExc != null)
        {
            /*
             * If an exception occurred in the try block, but the finally did not try to suppress that exception
             * we still want to throw it.
             */
            throw initExc;
        }
    }

    private void loadCoreObjects()
        throws AccessDeniedException, DatabaseException, InitializationException
    {
        errorReporter.logInfo("Security objects load from database is in progress");

        databaseDriver.loadSecurityObjects();

        errorReporter.logInfo("Security objects load from database completed");
        errorReporter.logInfo("Core objects load from database is in progress");

        nodeRepository.requireAccess(initCtx, AccessType.CONTROL);
        resourceDefinitionRepository.requireAccess(initCtx, AccessType.CONTROL);
        storPoolDefinitionRepository.requireAccess(initCtx, AccessType.CONTROL);

        databaseDriver.loadCoreObjects();

        errorReporter.logInfo("Core objects load from database completed");
    }

    private void initializeDisklessStorPoolDfn()
        throws AccessDeniedException, DatabaseException
    {
        StorPoolDefinition disklessStorPoolDfn = storPoolDfnDbDriver.createDefaultDisklessStorPool();
        storPoolDefinitionRepository.put(initCtx, disklessStorPoolDfn.getName(), disklessStorPoolDfn);
    }
}
