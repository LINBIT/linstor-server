package com.linbit.linstor.core;

import com.linbit.linstor.InitializationException;
import com.linbit.linstor.NodeRepository;
import com.linbit.linstor.ResourceDefinitionRepository;
import com.linbit.linstor.StorPoolDefinitionData;
import com.linbit.linstor.StorPoolDefinitionRepository;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.dbdrivers.DatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDefinitionDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.transaction.TransactionException;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionMgrGenerator;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import com.google.inject.Key;

public class DbDataInitializer
{
    private final ErrorReporter errorReporter;
    private final AccessContext initCtx;
    private final LinStorScope initScope;
    private final Props ctrlConf;
    private final Props stltConf;
    private final NodeRepository nodeRepository;
    private final ResourceDefinitionRepository resourceDefinitionRepository;
    private final StorPoolDefinitionRepository storPoolDefinitionRepository;
    private final ReadWriteLock reconfigurationLock;
    private final DatabaseDriver databaseDriver;
    private final StorPoolDefinitionDataDatabaseDriver storPoolDfnDbDriver;
    private final TransactionMgrGenerator transactionMgrGenerator;

    @Inject
    public DbDataInitializer(
        ErrorReporter errorReporterRef,
        @SystemContext AccessContext initCtxRef,
        LinStorScope initScopeRef,
        @Named(LinStor.CONTROLLER_PROPS) Props ctrlConfRef,
        @Named(LinStor.SATELLITE_PROPS) Props stltConfRef,
        NodeRepository nodeRepositoryRef,
        ResourceDefinitionRepository resourceDefinitionRepositoryRef,
        StorPoolDefinitionRepository storPoolDefinitionRepositoryRef,
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        DatabaseDriver databaseDriverRef,
        StorPoolDefinitionDataDatabaseDriver storPoolDfnDbDriverRef,
        TransactionMgrGenerator transactionMgrGeneratorRef
    )
    {
        errorReporter = errorReporterRef;
        initCtx = initCtxRef;
        initScope = initScopeRef;
        ctrlConf = ctrlConfRef;
        stltConf = stltConfRef;
        nodeRepository = nodeRepositoryRef;
        resourceDefinitionRepository = resourceDefinitionRepositoryRef;
        storPoolDefinitionRepository = storPoolDefinitionRepositoryRef;
        reconfigurationLock = reconfigurationLockRef;
        databaseDriver = databaseDriverRef;
        storPoolDfnDbDriver = storPoolDfnDbDriverRef;
        transactionMgrGenerator = transactionMgrGeneratorRef;
    }

    public void initialize()
        throws AccessDeniedException, InitializationException
    {
        TransactionMgr transMgr = null;
        Lock recfgWriteLock = reconfigurationLock.writeLock();
        try
        {
            transMgr = transactionMgrGenerator.startTransaction();
            initScope.enter();
            initScope.seed(TransactionMgr.class, transMgr);

            // rebuilding layerData also runs an additional check to verify the used storage pools
            // which needs a peerContext.
            initScope.seed(Key.get(AccessContext.class, PeerContext.class), initCtx);


            // Replacing the entire configuration requires locking out all other tasks
            //
            // Since others task that use the configuration must hold the reconfiguration lock
            // in read mode before locking any of the other system objects, locking the maps
            // for nodes, resource definition, storage pool definitions, etc. can be skipped.
            recfgWriteLock.lock();

            loadCoreConf();
            loadCoreObjects();
            initializeDisklessStorPoolDfn();

            transMgr.commit();
            initScope.exit();
        }
        catch (DatabaseException exc)
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
                catch (TransactionException exc)
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

    private void loadCoreConf()
        throws DatabaseException, AccessDeniedException
    {
        ctrlConf.loadAll();
        stltConf.loadAll();
    }

    private void loadCoreObjects()
        throws AccessDeniedException, DatabaseException
    {
        errorReporter.logInfo("Core objects load from database is in progress");

        nodeRepository.requireAccess(initCtx, AccessType.CONTROL);
        resourceDefinitionRepository.requireAccess(initCtx, AccessType.CONTROL);
        storPoolDefinitionRepository.requireAccess(initCtx, AccessType.CONTROL);

        databaseDriver.loadAll();

        errorReporter.logInfo("Core objects load from database completed");
    }

    private void initializeDisklessStorPoolDfn()
        throws AccessDeniedException, DatabaseException
    {
        StorPoolDefinitionData disklessStorPoolDfn = storPoolDfnDbDriver.createDefaultDisklessStorPool();
        storPoolDefinitionRepository.put(initCtx, disklessStorPoolDfn.getName(), disklessStorPoolDfn);
    }
}
