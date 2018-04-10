package com.linbit.linstor.dbdrivers;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorRuntimeException;
import com.linbit.linstor.NetInterfaceDataGenericDbDriver;
import com.linbit.linstor.NodeConnectionDataGenericDbDriver;
import com.linbit.linstor.NodeDataGenericDbDriver;
import com.linbit.linstor.ResourceConnectionDataGenericDbDriver;
import com.linbit.linstor.ResourceDataGenericDbDriver;
import com.linbit.linstor.ResourceDefinitionDataGenericDbDriver;
import com.linbit.linstor.SatelliteConnectionDataGenericDbDriver;
import com.linbit.linstor.StorPoolDataGenericDbDriver;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.StorPoolDefinitionData;
import com.linbit.linstor.StorPoolDefinitionDataGenericDbDriver;
import com.linbit.linstor.StorPoolDefinitionDataFactory;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.VolumeConnectionDataGenericDbDriver;
import com.linbit.linstor.VolumeDataGenericDbDriver;
import com.linbit.linstor.VolumeDefinitionDataGenericDbDriver;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.annotation.Uninitialized;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.dbdrivers.interfaces.NetInterfaceDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NodeConnectionDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NodeDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.PropsConDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceConnectionDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDefinitionDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SatelliteConnectionDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDefinitionDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeConnectionDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDefinitionDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsConGenericDbDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
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

public class ControllerDbModule extends AbstractModule
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
        bind(SatelliteConnectionDataDatabaseDriver.class).to(SatelliteConnectionDataGenericDbDriver.class);
        bind(NodeConnectionDataDatabaseDriver.class).to(NodeConnectionDataGenericDbDriver.class);
        bind(ResourceConnectionDataDatabaseDriver.class).to(ResourceConnectionDataGenericDbDriver.class);
        bind(VolumeConnectionDataDatabaseDriver.class).to(VolumeConnectionDataGenericDbDriver.class);
    }

    @Provides
    @Singleton
    @Named(DISKLESS_STOR_POOL_DFN)
    public StorPoolDefinition initializeDisklessStorPoolDfn(
        ErrorReporter errorLogRef,
        @SystemContext AccessContext initCtx,
        StorPoolDefinitionDataFactory storPoolDefinitionDataFactory,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLock,
        DbConnectionPool dbConnPool,
        @Uninitialized CoreModule.StorPoolDefinitionMap storPoolDfnMap,
        LinStorScope initScopeScope
    )
        throws AccessDeniedException
    {
        StorPoolDefinitionData disklessStorPoolDfn = null;

        try
        {
            storPoolDfnMapLock.writeLock().lock();
            TransactionMgr transMgr = new ControllerTransactionMgr(dbConnPool);

            initScopeScope.enter();
            initScopeScope.seed(TransactionMgr.class, transMgr);

            disklessStorPoolDfn = storPoolDefinitionDataFactory.getInstance(
                initCtx,
                new StorPoolName(LinStor.DISKLESS_STOR_POOL_NAME),
                true,
                false
            );

            transMgr.commit();
            initScopeScope.exit();

            storPoolDfnMap.put(disklessStorPoolDfn.getName(), disklessStorPoolDfn);
            transMgr.returnConnection();
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ImplementationError(dataAlreadyExistsExc);
        }
        catch (SQLException sqlExc)
        {
            errorLogRef.reportError(sqlExc);
            throw new LinStorRuntimeException(sqlExc.getMessage(), sqlExc);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new ImplementationError(
                "Invalid name for default diskless stor pool: " + invalidNameExc.invalidName,
                invalidNameExc
            );
        }
        finally
        {
            storPoolDfnMapLock.writeLock().unlock();
        }

        return disklessStorPoolDfn;
    }
}
