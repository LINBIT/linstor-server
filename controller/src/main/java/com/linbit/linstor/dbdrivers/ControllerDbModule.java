package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.core.objects.DrbdLayerETCDDriver;
import com.linbit.linstor.core.objects.DrbdLayerGenericDbDriver;
import com.linbit.linstor.core.objects.KeyValueStoreDbDriver;
import com.linbit.linstor.core.objects.LuksLayerETCDDriver;
import com.linbit.linstor.core.objects.LuksLayerGenericDbDriver;
import com.linbit.linstor.core.objects.NetInterfaceDbDriver;
import com.linbit.linstor.core.objects.NodeConnectionDbDriver;
import com.linbit.linstor.core.objects.NodeDbDriver;
import com.linbit.linstor.core.objects.NodeETCDDriver;
import com.linbit.linstor.core.objects.NvmeLayerETCDDriver;
import com.linbit.linstor.core.objects.NvmeLayerGenericDbDriver;
import com.linbit.linstor.core.objects.ResourceConnectionDbDriver;
import com.linbit.linstor.core.objects.ResourceDbDriver;
import com.linbit.linstor.core.objects.ResourceDefinitionDbDriver;
import com.linbit.linstor.core.objects.ResourceGroupDbDriver;
import com.linbit.linstor.core.objects.ResourceLayerETCDDriver;
import com.linbit.linstor.core.objects.ResourceLayerIdGenericDbDriver;
import com.linbit.linstor.core.objects.SnapshotDbDriver;
import com.linbit.linstor.core.objects.SnapshotDefinitionDbDriver;
import com.linbit.linstor.core.objects.SnapshotVolumeDbDriver;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinitionDbDriver;
import com.linbit.linstor.core.objects.StorPoolDbDriver;
import com.linbit.linstor.core.objects.StorPoolDefinitionDbDriver;
import com.linbit.linstor.core.objects.StorageLayerETCDDriver;
import com.linbit.linstor.core.objects.StorageLayerGenericDbDriver;
import com.linbit.linstor.core.objects.VolumeConnectionDbDriver;
import com.linbit.linstor.core.objects.VolumeDbDriver;
import com.linbit.linstor.core.objects.VolumeDefinitionDbDriver;
import com.linbit.linstor.core.objects.VolumeGroupDbDriver;
import com.linbit.linstor.core.objects.WritecacheLayerETCDDriver;
import com.linbit.linstor.core.objects.WritecacheLayerGenericDbDriver;
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.dbcp.DbConnectionPoolInitializer;
import com.linbit.linstor.dbcp.DbInitializer;
import com.linbit.linstor.dbcp.etcd.DbEtcd;
import com.linbit.linstor.dbcp.etcd.DbEtcdInitializer;
import com.linbit.linstor.dbdrivers.etcd.ETCDEngine;
import com.linbit.linstor.dbdrivers.interfaces.DrbdLayerCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.DrbdLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.KeyValueStoreCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.KeyValueStoreDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LuksLayerCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LuksLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NetInterfaceCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NetInterfaceDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NodeConnectionCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NodeConnectionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NodeCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NodeDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NvmeLayerCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NvmeLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.PropsConDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceConnectionCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceConnectionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDefinitionCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDefinitionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceGroupCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceGroupDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDefinitionCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDefinitionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDefinitionCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDefinitionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDefinitionCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDefinitionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorageLayerCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorageLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeConnectionCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeConnectionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDefinitionCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDefinitionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeGroupCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeGroupDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.WritecacheLayerCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.WritecacheLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.sql.SQLEngine;
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

        bind(ResourceDefinitionCtrlDatabaseDriver.class).to(ResourceDefinitionDbDriver.class);
        bind(ResourceDefinitionDatabaseDriver.class).to(ResourceDefinitionDbDriver.class);
        bind(ResourceGroupCtrlDatabaseDriver.class).to(ResourceGroupDbDriver.class);
        bind(ResourceGroupDatabaseDriver.class).to(ResourceGroupDbDriver.class);
        bind(VolumeGroupCtrlDatabaseDriver.class).to(VolumeGroupDbDriver.class);
        bind(VolumeGroupDatabaseDriver.class).to(VolumeGroupDbDriver.class);
        bind(VolumeCtrlDatabaseDriver.class).to(VolumeDbDriver.class);
        bind(VolumeDatabaseDriver.class).to(VolumeDbDriver.class);
        bind(ResourceCtrlDatabaseDriver.class).to(ResourceDbDriver.class);
        bind(ResourceDatabaseDriver.class).to(ResourceDbDriver.class);
        bind(VolumeDefinitionCtrlDatabaseDriver.class).to(VolumeDefinitionDbDriver.class);
        bind(VolumeDefinitionDatabaseDriver.class).to(VolumeDefinitionDbDriver.class);
        bind(StorPoolDefinitionCtrlDatabaseDriver.class).to(StorPoolDefinitionDbDriver.class);
        bind(StorPoolDefinitionDatabaseDriver.class).to(StorPoolDefinitionDbDriver.class);
        bind(StorPoolCtrlDatabaseDriver.class).to(StorPoolDbDriver.class);
        bind(StorPoolDatabaseDriver.class).to(StorPoolDbDriver.class);
        bind(NetInterfaceCtrlDatabaseDriver.class).to(NetInterfaceDbDriver.class);
        bind(NetInterfaceDatabaseDriver.class).to(NetInterfaceDbDriver.class);
        bind(NodeConnectionCtrlDatabaseDriver.class).to(NodeConnectionDbDriver.class);
        bind(NodeConnectionDatabaseDriver.class).to(NodeConnectionDbDriver.class);
        bind(ResourceConnectionCtrlDatabaseDriver.class).to(ResourceConnectionDbDriver.class);
        bind(ResourceConnectionDatabaseDriver.class).to(ResourceConnectionDbDriver.class);
        bind(VolumeConnectionCtrlDatabaseDriver.class).to(VolumeConnectionDbDriver.class);
        bind(VolumeConnectionDatabaseDriver.class).to(VolumeConnectionDbDriver.class);
        bind(SnapshotDefinitionCtrlDatabaseDriver.class).to(SnapshotDefinitionDbDriver.class);
        bind(SnapshotDefinitionDatabaseDriver.class).to(SnapshotDefinitionDbDriver.class);
        bind(SnapshotVolumeDefinitionCtrlDatabaseDriver.class).to(SnapshotVolumeDefinitionDbDriver.class);
        bind(SnapshotVolumeDefinitionDatabaseDriver.class).to(SnapshotVolumeDefinitionDbDriver.class);
        bind(SnapshotCtrlDatabaseDriver.class).to(SnapshotDbDriver.class);
        bind(SnapshotDatabaseDriver.class).to(SnapshotDbDriver.class);
        bind(SnapshotVolumeCtrlDatabaseDriver.class).to(SnapshotVolumeDbDriver.class);
        bind(SnapshotVolumeDatabaseDriver.class).to(SnapshotVolumeDbDriver.class);
        bind(KeyValueStoreCtrlDatabaseDriver.class).to(KeyValueStoreDbDriver.class);
        bind(KeyValueStoreDatabaseDriver.class).to(KeyValueStoreDbDriver.class);
        switch (dbType)
        {
            case SQL:
                bind(ControllerDatabase.class).to(DbConnectionPool.class);
                bind(DbEngine.class).to(SQLEngine.class);

                bind(DbInitializer.class).to(DbConnectionPoolInitializer.class);
                bind(DbAccessor.class).to(DbSQLPersistence.class);

                bind(ObjectProtectionDatabaseDriver.class).to(ObjectProtectionGenericDbDriver.class);

                bind(PropsConDatabaseDriver.class).to(PropsConGenericDbDriver.class);

                bind(NodeCtrlDatabaseDriver.class).to(NodeDbDriver.class);
                bind(NodeDatabaseDriver.class).to(NodeDbDriver.class);

                bind(ResourceLayerIdCtrlDatabaseDriver.class).to(ResourceLayerIdGenericDbDriver.class);
                bind(ResourceLayerIdDatabaseDriver.class).to(ResourceLayerIdGenericDbDriver.class);
                bind(StorageLayerCtrlDatabaseDriver.class).to(StorageLayerGenericDbDriver.class);
                bind(StorageLayerDatabaseDriver.class).to(StorageLayerGenericDbDriver.class);
                bind(DrbdLayerCtrlDatabaseDriver.class).to(DrbdLayerGenericDbDriver.class);
                bind(DrbdLayerDatabaseDriver.class).to(DrbdLayerGenericDbDriver.class);
                bind(LuksLayerCtrlDatabaseDriver.class).to(LuksLayerGenericDbDriver.class);
                bind(LuksLayerDatabaseDriver.class).to(LuksLayerGenericDbDriver.class);
                bind(NvmeLayerCtrlDatabaseDriver.class).to(NvmeLayerGenericDbDriver.class);
                bind(NvmeLayerDatabaseDriver.class).to(NvmeLayerGenericDbDriver.class);
                bind(WritecacheLayerCtrlDatabaseDriver.class).to(WritecacheLayerGenericDbDriver.class);
                bind(WritecacheLayerDatabaseDriver.class).to(WritecacheLayerGenericDbDriver.class);
                break;
            case ETCD:
                bind(ControllerDatabase.class).to(DbEtcd.class);
                bind(DbEngine.class).to(ETCDEngine.class);

                bind(DbInitializer.class).to(DbEtcdInitializer.class);
                bind(DbAccessor.class).to(DbEtcdPersistence.class);

                bind(ObjectProtectionDatabaseDriver.class).to(ObjectProtectionEtcdDriver.class);

                bind(PropsConDatabaseDriver.class).to(PropsConETCDDriver.class);

                bind(NodeCtrlDatabaseDriver.class).to(NodeETCDDriver.class);
                bind(NodeDatabaseDriver.class).to(NodeETCDDriver.class);

                bind(ResourceLayerIdCtrlDatabaseDriver.class).to(ResourceLayerETCDDriver.class);
                bind(ResourceLayerIdDatabaseDriver.class).to(ResourceLayerETCDDriver.class);
                bind(DrbdLayerCtrlDatabaseDriver.class).to(DrbdLayerETCDDriver.class);
                bind(DrbdLayerDatabaseDriver.class).to(DrbdLayerETCDDriver.class);
                bind(LuksLayerCtrlDatabaseDriver.class).to(LuksLayerETCDDriver.class);
                bind(LuksLayerDatabaseDriver.class).to(LuksLayerETCDDriver.class);
                bind(StorageLayerCtrlDatabaseDriver.class).to(StorageLayerETCDDriver.class);
                bind(StorageLayerDatabaseDriver.class).to(StorageLayerETCDDriver.class);
                bind(NvmeLayerCtrlDatabaseDriver.class).to(NvmeLayerETCDDriver.class);
                bind(NvmeLayerDatabaseDriver.class).to(NvmeLayerETCDDriver.class);
                bind(WritecacheLayerCtrlDatabaseDriver.class).to(WritecacheLayerETCDDriver.class);
                bind(WritecacheLayerDatabaseDriver.class).to(WritecacheLayerETCDDriver.class);
                break;
            default:
                throw new RuntimeException("Unknown database type: " + dbType);
        }
    }
}
