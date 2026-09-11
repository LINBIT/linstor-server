package com.linbit.linstor.core;

import com.linbit.linstor.InitializationException;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.repository.StorPoolDefinitionRepository;
import com.linbit.linstor.dbdrivers.DatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDefinitionDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.systemstarter.StartupInitializer;
import com.linbit.linstor.transaction.TransactionException;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.transaction.manager.TransactionMgrGenerator;
import com.linbit.linstor.transaction.manager.TransactionMgrUtil;

import jakarta.inject.Inject;
import jakarta.inject.Named;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;


public class DbDataInitializer implements StartupInitializer
{
    private final ErrorReporter errorReporter;
    private final LinStorScope initScope;
    private final StorPoolDefinitionRepository storPoolDefinitionRepository;
    private final ReadWriteLock reconfigurationLock;
    private final DatabaseDriver databaseDriver;
    private final StorPoolDefinitionDatabaseDriver storPoolDfnDbDriver;
    private final TransactionMgrGenerator transactionMgrGenerator;

    @Inject
    public DbDataInitializer(
        ErrorReporter errorReporterRef,
        LinStorScope initScopeRef,
        StorPoolDefinitionRepository storPoolDefinitionRepositoryRef,
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        DatabaseDriver databaseDriverRef,
        StorPoolDefinitionDatabaseDriver storPoolDfnDbDriverRef,
        TransactionMgrGenerator transactionMgrGeneratorRef
    )
    {
        errorReporter = errorReporterRef;
        initScope = initScopeRef;
        storPoolDefinitionRepository = storPoolDefinitionRepositoryRef;
        reconfigurationLock = reconfigurationLockRef;
        databaseDriver = databaseDriverRef;
        storPoolDfnDbDriver = storPoolDfnDbDriverRef;
        transactionMgrGenerator = transactionMgrGeneratorRef;
    }

    @Override
    public void initialize()
        throws InitializationException
    {
        TransactionMgr transMgr = null;
        Lock recfgWriteLock = reconfigurationLock.writeLock();
        boolean locked = false;

        InitializationException initExc = null;

        try (LinStorScope.ScopeAutoCloseable close = initScope.enter())
        {
            transMgr = transactionMgrGenerator.startTransaction();
            TransactionMgrUtil.seedTransactionMgr(initScope, transMgr);

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
        throws DatabaseException, InitializationException
    {
        errorReporter.logInfo("Core objects load from database is in progress");


        databaseDriver.loadCoreObjects();

        errorReporter.logInfo("Core objects load from database completed");
    }

    private void initializeDisklessStorPoolDfn()
        throws DatabaseException
    {
        StorPoolDefinition disklessStorPoolDfn = storPoolDfnDbDriver.createDefaultDisklessStorPool();
        storPoolDefinitionRepository.put(disklessStorPoolDfn.getName(), disklessStorPoolDfn);
    }
}
