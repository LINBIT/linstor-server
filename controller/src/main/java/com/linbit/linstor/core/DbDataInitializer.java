package com.linbit.linstor.core;

import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.dbdrivers.DatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ControllerSecurityModule;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.transaction.ControllerTransactionMgr;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Named;
import java.sql.SQLException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

public class DbDataInitializer
{
    private final ErrorReporter errorReporter;
    private final AccessContext initCtx;
    private final LinStorScope initScope;
    private final ControllerDatabase dbConnPool;
    private final Props ctrlConf;
    private final Props stltConf;
    private final ObjectProtection nodesMapProt;
    private final ObjectProtection rscDfnMapProt;
    private final ObjectProtection storPoolDfnMapProt;
    private final ReadWriteLock reconfigurationLock;
    private final DatabaseDriver databaseDriver;

    @Inject
    public DbDataInitializer(
        ErrorReporter errorReporterRef,
        @SystemContext AccessContext initCtxRef,
        LinStorScope initScopeRef,
        ControllerDatabase dbConnPoolRef,
        @Named(LinStor.CONTROLLER_PROPS) Props ctrlConfRef,
        @Named(LinStor.SATELLITE_PROPS) Props stltConfRef,
        @Named(ControllerSecurityModule.NODES_MAP_PROT) ObjectProtection nodesMapProtRef,
        @Named(ControllerSecurityModule.RSC_DFN_MAP_PROT) ObjectProtection rscDfnMapProtRef,
        @Named(ControllerSecurityModule.STOR_POOL_DFN_MAP_PROT) ObjectProtection storPoolDfnMapProtRef,
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        DatabaseDriver databaseDriverRef
    )
    {
        errorReporter = errorReporterRef;
        initCtx = initCtxRef;
        initScope = initScopeRef;
        dbConnPool = dbConnPoolRef;
        ctrlConf = ctrlConfRef;
        stltConf = stltConfRef;
        nodesMapProt = nodesMapProtRef;
        rscDfnMapProt = rscDfnMapProtRef;
        storPoolDfnMapProt = storPoolDfnMapProtRef;
        reconfigurationLock = reconfigurationLockRef;
        databaseDriver = databaseDriverRef;
    }

    public void initialize()
        throws AccessDeniedException, InitializationException
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

            loadCoreConf();
            loadCoreObjects();

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

    private void loadCoreConf()
        throws SQLException, AccessDeniedException
    {
        ctrlConf.loadAll();
        stltConf.loadAll();
    }

    void loadCoreObjects()
        throws AccessDeniedException, SQLException
    {
        errorReporter.logInfo("Core objects load from database is in progress");

        nodesMapProt.requireAccess(initCtx, AccessType.CONTROL);
        rscDfnMapProt.requireAccess(initCtx, AccessType.CONTROL);
        storPoolDfnMapProt.requireAccess(initCtx, AccessType.CONTROL);

        databaseDriver.loadAll();

        errorReporter.logInfo("Core objects load from database completed");
    }
}
