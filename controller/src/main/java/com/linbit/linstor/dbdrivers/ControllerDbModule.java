package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.objects.DrbdLayerGenericDbDriver;
import com.linbit.linstor.core.objects.KeyValueStoreDataGenericDbDriver;
import com.linbit.linstor.core.objects.LuksLayerGenericDbDriver;
import com.linbit.linstor.core.objects.NetInterfaceDbDriver;
import com.linbit.linstor.core.objects.NodeConnectionDataGenericDbDriver;
import com.linbit.linstor.core.objects.NodeDbDriver;
import com.linbit.linstor.core.objects.NvmeLayerGenericDbDriver;
import com.linbit.linstor.core.objects.ResourceConnectionDataGenericDbDriver;
import com.linbit.linstor.core.objects.ResourceDbDriver;
import com.linbit.linstor.core.objects.ResourceDefinitionDbDriver;
import com.linbit.linstor.core.objects.ResourceGroupDbDriver;
import com.linbit.linstor.core.objects.ResourceLayerETCDDriver;
import com.linbit.linstor.core.objects.ResourceLayerIdGenericDbDriver;
import com.linbit.linstor.core.objects.SnapshotDataGenericDbDriver;
import com.linbit.linstor.core.objects.SnapshotDefinitionDataGenericDbDriver;
import com.linbit.linstor.core.objects.SnapshotVolumeDataGenericDbDriver;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinitionGenericDbDriver;
import com.linbit.linstor.core.objects.StorPoolDbDriver;
import com.linbit.linstor.core.objects.StorPoolDefinitionDbDriver;
import com.linbit.linstor.core.objects.StorageLayerETCDDriver;
import com.linbit.linstor.core.objects.StorageLayerGenericDbDriver;
import com.linbit.linstor.core.objects.SwordfishETCDDriver;
import com.linbit.linstor.core.objects.SwordfishLayerGenericDbDriver;
import com.linbit.linstor.core.objects.VolumeConnectionDataGenericDbDriver;
import com.linbit.linstor.core.objects.VolumeDbDriver;
import com.linbit.linstor.core.objects.VolumeDefinitionDbDriver;
import com.linbit.linstor.core.objects.VolumeGroupDbDriver;
import com.linbit.linstor.dbcp.DbConnectionPoolInitializer;
import com.linbit.linstor.dbcp.DbInitializer;
import com.linbit.linstor.dbcp.etcd.DbEtcdInitializer;
import com.linbit.linstor.dbdrivers.interfaces.DrbdLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.KeyValueStoreDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LuksLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NetInterfaceDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NodeConnectionDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NodeDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NvmeLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.PropsConDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceConnectionDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDefinitionDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceGroupDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDefinitionDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDefinitionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDefinitionDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorageLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SwordfishLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeConnectionDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDefinitionDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeGroupDataDatabaseDriver;
import com.linbit.linstor.propscon.PropsConETCDDriver;
import com.linbit.linstor.propscon.PropsConGenericDbDriver;
import com.linbit.linstor.security.DbAccessor;
import com.linbit.linstor.security.DbEtcdPersistence;
import com.linbit.linstor.security.DbSQLPersistence;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.linstor.security.ObjectProtectionEtcdDriver;
import com.linbit.linstor.security.ObjectProtectionGenericDbDriver;

import com.google.inject.AbstractModule;

public class ControllerDbModule extends AbstractModule
{
    private final DatabaseDriverInfo.DatabaseType dbType;

    public ControllerDbModule(DatabaseDriverInfo.DatabaseType dbTypeRef)
    {
        dbType = dbTypeRef;
    }

    @Override
    protected void configure()
    {
        bind(DatabaseDriver.class).to(DatabaseLoader.class);

        bind(NodeDataDatabaseDriver.class).to(NodeDbDriver.class);
        bind(ResourceDefinitionDataDatabaseDriver.class).to(ResourceDefinitionDbDriver.class);
        bind(ResourceGroupDataDatabaseDriver.class).to(ResourceGroupDbDriver.class);
        bind(VolumeGroupDataDatabaseDriver.class).to(VolumeGroupDbDriver.class);
        bind(VolumeDataDatabaseDriver.class).to(VolumeDbDriver.class);
        bind(ResourceDataDatabaseDriver.class).to(ResourceDbDriver.class);
        bind(VolumeDefinitionDataDatabaseDriver.class).to(VolumeDefinitionDbDriver.class);
        bind(StorPoolDefinitionDataDatabaseDriver.class).to(StorPoolDefinitionDbDriver.class);
        bind(StorPoolDataDatabaseDriver.class).to(StorPoolDbDriver.class);
        bind(NetInterfaceDataDatabaseDriver.class).to(NetInterfaceDbDriver.class);
        switch (dbType)
        {
            case SQL:
                bind(DbInitializer.class).to(DbConnectionPoolInitializer.class);
                bind(DbAccessor.class).to(DbSQLPersistence.class);

                bind(ObjectProtectionDatabaseDriver.class).to(ObjectProtectionGenericDbDriver.class);

                bind(PropsConDatabaseDriver.class).to(PropsConGenericDbDriver.class);

                bind(ResourceLayerIdDatabaseDriver.class).to(ResourceLayerIdGenericDbDriver.class);
                bind(StorageLayerDatabaseDriver.class).to(StorageLayerGenericDbDriver.class);
                bind(SwordfishLayerDatabaseDriver.class).to(SwordfishLayerGenericDbDriver.class);

                // TODO
                bind(NodeConnectionDataDatabaseDriver.class).to(NodeConnectionDataGenericDbDriver.class);
                bind(ResourceConnectionDataDatabaseDriver.class).to(ResourceConnectionDataGenericDbDriver.class);
                bind(VolumeConnectionDataDatabaseDriver.class).to(VolumeConnectionDataGenericDbDriver.class);
                bind(SnapshotDefinitionDataDatabaseDriver.class).to(SnapshotDefinitionDataGenericDbDriver.class);
                bind(SnapshotVolumeDefinitionDatabaseDriver.class).to(SnapshotVolumeDefinitionGenericDbDriver.class);
                bind(SnapshotDataDatabaseDriver.class).to(SnapshotDataGenericDbDriver.class);
                bind(SnapshotVolumeDataDatabaseDriver.class).to(SnapshotVolumeDataGenericDbDriver.class);
                bind(KeyValueStoreDataDatabaseDriver.class).to(KeyValueStoreDataGenericDbDriver.class);

                bind(DrbdLayerDatabaseDriver.class).to(DrbdLayerGenericDbDriver.class);
                bind(LuksLayerDatabaseDriver.class).to(LuksLayerGenericDbDriver.class);
                bind(NvmeLayerDatabaseDriver.class).to(NvmeLayerGenericDbDriver.class);
                break;
            case ETCD:
                bind(DbInitializer.class).to(DbEtcdInitializer.class);
                bind(DbAccessor.class).to(DbEtcdPersistence.class);

                bind(ObjectProtectionDatabaseDriver.class).to(ObjectProtectionEtcdDriver.class);

                bind(PropsConDatabaseDriver.class).to(PropsConETCDDriver.class);
                bind(ResourceLayerIdDatabaseDriver.class).to(ResourceLayerETCDDriver.class);
                bind(StorageLayerDatabaseDriver.class).to(StorageLayerETCDDriver.class);
                bind(SwordfishLayerDatabaseDriver.class).to(SwordfishETCDDriver.class);
                break;
            default:
                throw new RuntimeException("ETCD database driver not implemented.");
        }
    }
}
