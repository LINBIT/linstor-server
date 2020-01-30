package com.linbit.linstor.dbdrivers;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.linstor.core.objects.DrbdLayerSQLDbDriver;
import com.linbit.linstor.core.objects.LuksLayerSQLDbDriver;
import com.linbit.linstor.core.objects.NetInterfaceDbDriver;
import com.linbit.linstor.core.objects.NodeConnectionDbDriver;
import com.linbit.linstor.core.objects.NodeGenericDbDriver;
import com.linbit.linstor.core.objects.NvmeLayerSQLDbDriver;
import com.linbit.linstor.core.objects.ResourceConnectionDbDriver;
import com.linbit.linstor.core.objects.ResourceDbDriver;
import com.linbit.linstor.core.objects.ResourceDefinitionDbDriver;
import com.linbit.linstor.core.objects.ResourceGroupDbDriver;
import com.linbit.linstor.core.objects.ResourceLayerIdSQLDbDriver;
import com.linbit.linstor.core.objects.SnapshotDbDriver;
import com.linbit.linstor.core.objects.SnapshotDefinitionDbDriver;
import com.linbit.linstor.core.objects.SnapshotVolumeDbDriver;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinitionDbDriver;
import com.linbit.linstor.core.objects.StorPoolDbDriver;
import com.linbit.linstor.core.objects.StorPoolDefinitionDbDriver;
import com.linbit.linstor.core.objects.StorageLayerSQLDbDriver;
import com.linbit.linstor.core.objects.VolumeConnectionDbDriver;
import com.linbit.linstor.core.objects.VolumeDbDriver;
import com.linbit.linstor.core.objects.VolumeDefinitionDbDriver;
import com.linbit.linstor.core.objects.WritecacheLayerSQLDbDriver;
import com.linbit.linstor.dbdrivers.interfaces.DrbdLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LuksLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NetInterfaceDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NodeConnectionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NodeDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NvmeLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.PropsConDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceConnectionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDefinitionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceGroupDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDefinitionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDefinitionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDefinitionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorageLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeConnectionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDefinitionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.WritecacheLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.sql.SQLEngine;
import com.linbit.linstor.propscon.PropsConSQLDbDriver;
import com.linbit.linstor.security.DbAccessor;
import com.linbit.linstor.security.DbSQLPersistence;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.linstor.security.ObjectProtectionSQLDbDriver;

import com.google.inject.AbstractModule;

public class TestDbModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(DbAccessor.class).to(DbSQLPersistence.class);
        bind(DbEngine.class).to(SQLEngine.class);

        bind(ObjectProtectionDatabaseDriver.class).to(ObjectProtectionSQLDbDriver.class);

        // bind(DatabaseDriver.class).to(DatabaseLoader.class);
        bind(DatabaseDriver.class).toInstance(new DatabaseDriver()
        {

            @Override
            public void loadAll() throws DatabaseException
            {
                // noop
            }

            @Override
            public ServiceName getDefaultServiceInstanceName()
            {
                try
                {
                    return new ServiceName("NoopTestDatabaseDriver");
                }
                catch (InvalidNameException exc)
                {
                    throw new ImplementationError(exc);
                }
            }
        });

        bind(PropsConDatabaseDriver.class).to(PropsConSQLDbDriver.class);
        bind(NodeDatabaseDriver.class).to(NodeGenericDbDriver.class);
        bind(ResourceGroupDatabaseDriver.class).to(ResourceGroupDbDriver.class);
        bind(ResourceDefinitionDatabaseDriver.class).to(ResourceDefinitionDbDriver.class);
        bind(ResourceDatabaseDriver.class).to(ResourceDbDriver.class);
        bind(VolumeDefinitionDatabaseDriver.class).to(VolumeDefinitionDbDriver.class);
        bind(VolumeDatabaseDriver.class).to(VolumeDbDriver.class);
        bind(StorPoolDefinitionDatabaseDriver.class).to(StorPoolDefinitionDbDriver.class);
        bind(StorPoolDatabaseDriver.class).to(StorPoolDbDriver.class);
        bind(NetInterfaceDatabaseDriver.class).to(NetInterfaceDbDriver.class);
        bind(NodeConnectionDatabaseDriver.class).to(NodeConnectionDbDriver.class);
        bind(ResourceConnectionDatabaseDriver.class).to(ResourceConnectionDbDriver.class);
        bind(VolumeConnectionDatabaseDriver.class).to(VolumeConnectionDbDriver.class);
        bind(SnapshotDefinitionDatabaseDriver.class).to(SnapshotDefinitionDbDriver.class);
        bind(SnapshotVolumeDefinitionDatabaseDriver.class).to(SnapshotVolumeDefinitionDbDriver.class);
        bind(SnapshotDatabaseDriver.class).to(SnapshotDbDriver.class);
        bind(SnapshotVolumeDatabaseDriver.class).to(SnapshotVolumeDbDriver.class);

        bind(ResourceLayerIdDatabaseDriver.class).to(ResourceLayerIdSQLDbDriver.class);
        bind(DrbdLayerDatabaseDriver.class).to(DrbdLayerSQLDbDriver.class);
        bind(LuksLayerDatabaseDriver.class).to(LuksLayerSQLDbDriver.class);
        bind(StorageLayerDatabaseDriver.class).to(StorageLayerSQLDbDriver.class);
        bind(NvmeLayerDatabaseDriver.class).to(NvmeLayerSQLDbDriver.class);
        bind(WritecacheLayerDatabaseDriver.class).to(WritecacheLayerSQLDbDriver.class);
    }
}
