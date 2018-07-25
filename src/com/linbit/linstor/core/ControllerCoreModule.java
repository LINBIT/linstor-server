package com.linbit.linstor.core;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Names;

import com.linbit.linstor.InitializationException;
import com.linbit.linstor.annotation.Uninitialized;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.dbdrivers.DatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ControllerSecurityModule;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.transaction.ControllerTransactionMgr;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Named;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ControllerCoreModule extends AbstractModule
{

    public static final String CTRL_CONF_LOCK = "ctrlConfLock";
    public static final String CTRL_ERROR_LIST_LOCK = "ctrlErrorListLock";

    private static final String DB_CONTROLLER_PROPSCON_INSTANCE_NAME = "CTRLCFG";
    private static final String DB_SATELLITE_PROPSCON_INSTANCE_NAME = "STLTCFG";

    @Override
    protected void configure()
    {
        bind(String.class).annotatedWith(Names.named(CoreModule.MODULE_NAME))
            .toInstance(Controller.MODULE);

        bind(ReadWriteLock.class).annotatedWith(Names.named(CTRL_CONF_LOCK))
            .toInstance(new ReentrantReadWriteLock(true));

        bind(ReadWriteLock.class).annotatedWith(Names.named(CTRL_ERROR_LIST_LOCK))
            .toInstance(new ReentrantReadWriteLock(true));
    }

    @Provides
    @Singleton
    @Named(LinStor.CONTROLLER_PROPS)
    public Props loadPropsContainer(
        DbConnectionPool dbConnPool,
        PropsContainerFactory propsContainerFactory,
        LinStorScope initScope
    )
        throws SQLException
    {
        Props propsContainer;
        TransactionMgr transMgr = null;
        try
        {
            transMgr = new ControllerTransactionMgr(dbConnPool);
            initScope.enter();
            initScope.seed(TransactionMgr.class, transMgr);

            propsContainer = propsContainerFactory.getInstance(DB_CONTROLLER_PROPSCON_INSTANCE_NAME);
            transMgr.commit();
            initScope.exit();
        }
        finally
        {
            if (transMgr != null)
            {
                transMgr.returnConnection();
            }
        }
        return propsContainer;
    }

    @Provides
    @Singleton
    @Named(LinStor.SATELLITE_PROPS)
    public Props loadSatellitePropsContainer(
        DbConnectionPool dbConnPool,
        PropsContainerFactory propsContainerFactory,
        LinStorScope initScope
    )
        throws SQLException
    {
        Props propsContainer;
        TransactionMgr transMgr = null;
        try
        {
            transMgr = new ControllerTransactionMgr(dbConnPool);
            initScope.enter();
            initScope.seed(TransactionMgr.class, transMgr);

            propsContainer = propsContainerFactory.getInstance(DB_SATELLITE_PROPSCON_INSTANCE_NAME);
            transMgr.commit();
            initScope.exit();
        }
        finally
        {
            if (transMgr != null)
            {
                transMgr.returnConnection();
            }
        }
        return propsContainer;
    }

    @Provides
    @Singleton
    CoreModule.NodesMap loadNodesMap(MapBundle mapBundle)
    {
        return mapBundle.nodesMap;
    }

    @Provides
    @Singleton
    CoreModule.ResourceDefinitionMap loadResourceDefinitionMap(MapBundle mapBundle)
    {
        return mapBundle.resourceDefinitionMap;
    }

    @Provides
    @Singleton
    CoreModule.StorPoolDefinitionMap loadStorPoolDefinitionMap(MapBundle mapBundle)
    {
        return mapBundle.storPoolDefinitionMap;
    }

    @Provides
    @Singleton
    MapBundle loadCoreObjects(
        ErrorReporter errorReporter,
        @SystemContext AccessContext initCtx,
        @Named(ControllerSecurityModule.NODES_MAP_PROT) ObjectProtection nodesMapProt,
        @Named(ControllerSecurityModule.RSC_DFN_MAP_PROT) ObjectProtection rscDfnMapProt,
        @Named(ControllerSecurityModule.STOR_POOL_DFN_MAP_PROT) ObjectProtection storPoolDfnMapProt,
        @Uninitialized CoreModule.NodesMap nodesMap,
        @Uninitialized CoreModule.ResourceDefinitionMap rscDfnMap,
        @Uninitialized CoreModule.StorPoolDefinitionMap storPoolDfnMap,
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLock,
        DbConnectionPool dbConnPool,
        DatabaseDriver databaseDriver,
        LinStorScope initScope
    )
        throws AccessDeniedException, InitializationException
    {
        errorReporter.logInfo("Core objects load from database is in progress");

        TransactionMgr transMgr = null;
        try
        {
            transMgr = new ControllerTransactionMgr(dbConnPool);
            nodesMapProt.requireAccess(initCtx, AccessType.CONTROL);
            rscDfnMapProt.requireAccess(initCtx, AccessType.CONTROL);
            storPoolDfnMapProt.requireAccess(initCtx, AccessType.CONTROL);

            Lock recfgWriteLock = reconfigurationLock.writeLock();
            try
            {

                // Replacing the entire configuration requires locking out all other tasks
                //
                // Since others task that use the configuration must hold the reconfiguration lock
                // in read mode before locking any of the other system objects, locking the maps
                // for nodes, resource definition, storage pool definitions, etc. can be skipped.
                recfgWriteLock.lock();

                initScope.enter();
                initScope.seed(TransactionMgr.class, transMgr);

                databaseDriver.loadAll();

                transMgr.commit();
                initScope.exit();
            }
            finally
            {
                recfgWriteLock.unlock();
            }
        }
        catch (SQLException exc)
        {
            throw new InitializationException(
                "Loading the core objects from the database failed",
                exc
            );
        }
        finally
        {
            if (transMgr != null)
            {
                try
                {
                    transMgr.rollback();
                }
                catch (Exception ignored)
                {
                }
                transMgr.returnConnection();
            }
        }

        errorReporter.logInfo("Core objects load from database completed");

        MapBundle bundle = new MapBundle();
        bundle.nodesMap = nodesMap;
        bundle.storPoolDefinitionMap = storPoolDfnMap;
        bundle.resourceDefinitionMap = rscDfnMap;
        return bundle;
    }

    // Bundle together so that the objects can be initialized together but provided separately
    private static class MapBundle
    {
        public CoreModule.NodesMap nodesMap;
        public CoreModule.StorPoolDefinitionMap storPoolDefinitionMap;
        public CoreModule.ResourceDefinitionMap resourceDefinitionMap;
    }
}
