package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.LinStorRuntimeException;
import com.linbit.linstor.NetInterfaceDataGenericDbDriver;
import com.linbit.linstor.NodeConnectionDataGenericDbDriver;
import com.linbit.linstor.NodeDataGenericDbDriver;
import com.linbit.linstor.ResourceConnectionDataGenericDbDriver;
import com.linbit.linstor.ResourceDataGenericDbDriver;
import com.linbit.linstor.ResourceDefinitionDataGenericDbDriver;
import com.linbit.linstor.SnapshotDataGenericDbDriver;
import com.linbit.linstor.SnapshotDefinitionDataGenericDbDriver;
import com.linbit.linstor.SnapshotVolumeDataGenericDbDriver;
import com.linbit.linstor.SnapshotVolumeDefinitionGenericDbDriver;
import com.linbit.linstor.StorPoolDataGenericDbDriver;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.StorPoolDefinitionData;
import com.linbit.linstor.StorPoolDefinitionDataGenericDbDriver;
import com.linbit.linstor.VolumeConnectionDataGenericDbDriver;
import com.linbit.linstor.VolumeDataGenericDbDriver;
import com.linbit.linstor.VolumeDefinitionDataGenericDbDriver;
import com.linbit.linstor.annotation.Uninitialized;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.dbdrivers.interfaces.NetInterfaceDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NodeConnectionDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NodeDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.PropsConDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceConnectionDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDefinitionDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDefinitionDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDefinitionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDefinitionDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeConnectionDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDefinitionDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsConGenericDbDriver;
import com.linbit.linstor.security.DbAccessor;
import com.linbit.linstor.security.DbPersistence;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.linstor.security.ObjectProtectionGenericDbDriver;
import com.linbit.linstor.transaction.ControllerTransactionMgr;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Named;
import javax.inject.Singleton;

import java.sql.SQLException;
import java.util.concurrent.locks.ReadWriteLock;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

public class TestDbModule extends AbstractModule
{
    public static final String DISKLESS_STOR_POOL_DFN = "disklessStorPoolDfn";

    @Override
    protected void configure()
    {
        bind(DbAccessor.class).to(DbPersistence.class);

        bind(ObjectProtectionDatabaseDriver.class).to(ObjectProtectionGenericDbDriver.class);

        bind(DatabaseDriver.class).to(GenericDbDriver.class);

        bind(PropsConDatabaseDriver.class).to(PropsConGenericDbDriver.class);
        bind(NodeDataDatabaseDriver.class).to(NodeDataGenericDbDriver.class);
        bind(ResourceDefinitionDataDatabaseDriver.class).to(ResourceDefinitionDataGenericDbDriver.class);
        bind(ResourceDataDatabaseDriver.class).to(ResourceDataGenericDbDriver.class);
        bind(VolumeDefinitionDataDatabaseDriver.class).to(VolumeDefinitionDataGenericDbDriver.class);
        bind(VolumeDataDatabaseDriver.class).to(VolumeDataGenericDbDriver.class);
        bind(StorPoolDefinitionDataDatabaseDriver.class).to(StorPoolDefinitionDataGenericDbDriver.class);
        bind(StorPoolDataDatabaseDriver.class).to(StorPoolDataGenericDbDriver.class);
        bind(NetInterfaceDataDatabaseDriver.class).to(NetInterfaceDataGenericDbDriver.class);
        bind(NodeConnectionDataDatabaseDriver.class).to(NodeConnectionDataGenericDbDriver.class);
        bind(ResourceConnectionDataDatabaseDriver.class).to(ResourceConnectionDataGenericDbDriver.class);
        bind(VolumeConnectionDataDatabaseDriver.class).to(VolumeConnectionDataGenericDbDriver.class);
        bind(SnapshotDefinitionDataDatabaseDriver.class).to(SnapshotDefinitionDataGenericDbDriver.class);
        bind(SnapshotVolumeDefinitionDatabaseDriver.class).to(SnapshotVolumeDefinitionGenericDbDriver.class);
        bind(SnapshotDataDatabaseDriver.class).to(SnapshotDataGenericDbDriver.class);
        bind(SnapshotVolumeDataDatabaseDriver.class).to(SnapshotVolumeDataGenericDbDriver.class);
    }

    @Provides
    @Singleton
    @Named(DISKLESS_STOR_POOL_DFN)
    public StorPoolDefinition initializeDisklessStorPoolDfn(
        ErrorReporter errorLogRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLock,
        DbConnectionPool dbConnPool,
        @Uninitialized CoreModule.StorPoolDefinitionMap storPoolDfnMap,
        LinStorScope initScopeScope,
        StorPoolDefinitionDataDatabaseDriver storPoolDfnDbDriver
    )
    {
        StorPoolDefinitionData disklessStorPoolDfn = null;

        TransactionMgr transMgr = null;
        try
        {
            storPoolDfnMapLock.writeLock().lock();
            transMgr = new ControllerTransactionMgr(dbConnPool);

            initScopeScope.enter();
            initScopeScope.seed(TransactionMgr.class, transMgr);

            disklessStorPoolDfn = storPoolDfnDbDriver.createDefaultDisklessStorPool();
            storPoolDfnMap.put(disklessStorPoolDfn.getName(), disklessStorPoolDfn);

            transMgr.commit();
            initScopeScope.exit();
        }
        catch (SQLException sqlExc)
        {
            errorLogRef.reportError(sqlExc);
            throw new LinStorRuntimeException(sqlExc.getMessage(), sqlExc);
        }
        finally
        {
            storPoolDfnMapLock.writeLock().unlock();

            if (transMgr != null)
            {
                transMgr.returnConnection();
            }
        }

        return disklessStorPoolDfn;
    }
}
