package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.ControllerETCDDatabase;
import com.linbit.linstor.ControllerK8sCrdDatabase;
import com.linbit.linstor.core.objects.BCacheLayerETCDDriver;
import com.linbit.linstor.core.objects.BCacheLayerK8sCrdDriver;
import com.linbit.linstor.core.objects.BCacheLayerSQLDbDriver;
import com.linbit.linstor.core.objects.CacheLayerETCDDriver;
import com.linbit.linstor.core.objects.CacheLayerK8sCrdDriver;
import com.linbit.linstor.core.objects.CacheLayerSQLDbDriver;
import com.linbit.linstor.core.objects.DrbdLayerETCDDriver;
import com.linbit.linstor.core.objects.DrbdLayerK8sCrdDriver;
import com.linbit.linstor.core.objects.DrbdLayerSQLDbDriver;
import com.linbit.linstor.core.objects.ExternalFileDbDriver;
import com.linbit.linstor.core.objects.KeyValueStoreDbDriver;
import com.linbit.linstor.core.objects.LinstorRemoteDbDriver;
import com.linbit.linstor.core.objects.LuksLayerETCDDriver;
import com.linbit.linstor.core.objects.LuksLayerK8sCrdDriver;
import com.linbit.linstor.core.objects.LuksLayerSQLDbDriver;
import com.linbit.linstor.core.objects.NetInterfaceDbDriver;
import com.linbit.linstor.core.objects.NodeConnectionDbDriver;
import com.linbit.linstor.core.objects.NodeDbDriver;
import com.linbit.linstor.core.objects.NodeETCDDriver;
import com.linbit.linstor.core.objects.NvmeLayerETCDDriver;
import com.linbit.linstor.core.objects.NvmeLayerK8sCrdDriver;
import com.linbit.linstor.core.objects.NvmeLayerSQLDbDriver;
import com.linbit.linstor.core.objects.OpenflexLayerETCDDriver;
import com.linbit.linstor.core.objects.OpenflexLayerK8sCrdDriver;
import com.linbit.linstor.core.objects.OpenflexLayerSQLDbDriver;
import com.linbit.linstor.core.objects.ResourceConnectionDbDriver;
import com.linbit.linstor.core.objects.ResourceDbDriver;
import com.linbit.linstor.core.objects.ResourceDefinitionDbDriver;
import com.linbit.linstor.core.objects.ResourceGroupDbDriver;
import com.linbit.linstor.core.objects.ResourceLayerIdETCDDriver;
import com.linbit.linstor.core.objects.ResourceLayerIdK8sCrdDriver;
import com.linbit.linstor.core.objects.ResourceLayerIdSQLDbDriver;
import com.linbit.linstor.core.objects.S3RemoteDbDriver;
import com.linbit.linstor.core.objects.SnapshotDbDriver;
import com.linbit.linstor.core.objects.SnapshotDefinitionDbDriver;
import com.linbit.linstor.core.objects.SnapshotVolumeDbDriver;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinitionDbDriver;
import com.linbit.linstor.core.objects.StorPoolDbDriver;
import com.linbit.linstor.core.objects.StorPoolDefinitionDbDriver;
import com.linbit.linstor.core.objects.StorageLayerETCDDriver;
import com.linbit.linstor.core.objects.StorageLayerK8sCrdDriver;
import com.linbit.linstor.core.objects.StorageLayerSQLDbDriver;
import com.linbit.linstor.core.objects.VolumeConnectionDbDriver;
import com.linbit.linstor.core.objects.VolumeDbDriver;
import com.linbit.linstor.core.objects.VolumeDefinitionDbDriver;
import com.linbit.linstor.core.objects.VolumeGroupDbDriver;
import com.linbit.linstor.core.objects.WritecacheLayerETCDDriver;
import com.linbit.linstor.core.objects.WritecacheLayerK8sCrdDriver;
import com.linbit.linstor.core.objects.WritecacheLayerSQLDbDriver;
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.dbcp.DbConnectionPoolInitializer;
import com.linbit.linstor.dbcp.DbInitializer;
import com.linbit.linstor.dbcp.etcd.DbEtcd;
import com.linbit.linstor.dbcp.etcd.DbEtcdInitializer;
import com.linbit.linstor.dbcp.k8s.crd.DbK8sCrd;
import com.linbit.linstor.dbcp.k8s.crd.DbK8sCrdInitializer;
import com.linbit.linstor.dbdrivers.etcd.ETCDEngine;
import com.linbit.linstor.dbdrivers.interfaces.BCacheLayerCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.BCacheLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.CacheLayerCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.CacheLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.DrbdLayerCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.DrbdLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ExternalFileCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ExternalFileDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.KeyValueStoreCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.KeyValueStoreDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LinstorRemoteCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LinstorRemoteDatabaseDriver;
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
import com.linbit.linstor.dbdrivers.interfaces.OpenflexLayerCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.OpenflexLayerDatabaseDriver;
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
import com.linbit.linstor.dbdrivers.interfaces.S3RemoteCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.S3RemoteDatabaseDriver;
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
import com.linbit.linstor.dbdrivers.k8s.crd.K8sCrdEngine;
import com.linbit.linstor.dbdrivers.sql.SQLEngine;
import com.linbit.linstor.propscon.PropsConETCDDriver;
import com.linbit.linstor.propscon.PropsConK8sCrdDriver;
import com.linbit.linstor.propscon.PropsConSQLDbDriver;
import com.linbit.linstor.security.DbAccessor;
import com.linbit.linstor.security.DbEtcdPersistence;
import com.linbit.linstor.security.DbK8sCrdPersistence;
import com.linbit.linstor.security.DbSQLPersistence;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.linstor.security.ObjectProtectionEtcdDriver;
import com.linbit.linstor.security.ObjectProtectionK8sCrdDriver;
import com.linbit.linstor.security.ObjectProtectionSQLDbDriver;

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
        bind(ExternalFileCtrlDatabaseDriver.class).to(ExternalFileDbDriver.class);
        bind(ExternalFileDatabaseDriver.class).to(ExternalFileDbDriver.class);
        bind(S3RemoteCtrlDatabaseDriver.class).to(S3RemoteDbDriver.class);
        bind(S3RemoteDatabaseDriver.class).to(S3RemoteDbDriver.class);
        bind(LinstorRemoteCtrlDatabaseDriver.class).to(LinstorRemoteDbDriver.class);
        bind(LinstorRemoteDatabaseDriver.class).to(LinstorRemoteDbDriver.class);
        switch (dbType)
        {
            case SQL:
                bind(ControllerDatabase.class).to(DbConnectionPool.class);
                bind(DbEngine.class).to(SQLEngine.class);

                bind(DbInitializer.class).to(DbConnectionPoolInitializer.class);
                bind(DbAccessor.class).to(DbSQLPersistence.class);

                bind(ObjectProtectionDatabaseDriver.class).to(ObjectProtectionSQLDbDriver.class);

                bind(PropsConDatabaseDriver.class).to(PropsConSQLDbDriver.class);

                bind(NodeCtrlDatabaseDriver.class).to(NodeDbDriver.class);
                bind(NodeDatabaseDriver.class).to(NodeDbDriver.class);

                bind(ResourceLayerIdCtrlDatabaseDriver.class).to(ResourceLayerIdSQLDbDriver.class);
                bind(ResourceLayerIdDatabaseDriver.class).to(ResourceLayerIdSQLDbDriver.class);
                bind(StorageLayerCtrlDatabaseDriver.class).to(StorageLayerSQLDbDriver.class);
                bind(StorageLayerDatabaseDriver.class).to(StorageLayerSQLDbDriver.class);
                bind(DrbdLayerCtrlDatabaseDriver.class).to(DrbdLayerSQLDbDriver.class);
                bind(DrbdLayerDatabaseDriver.class).to(DrbdLayerSQLDbDriver.class);
                bind(LuksLayerCtrlDatabaseDriver.class).to(LuksLayerSQLDbDriver.class);
                bind(LuksLayerDatabaseDriver.class).to(LuksLayerSQLDbDriver.class);
                bind(NvmeLayerCtrlDatabaseDriver.class).to(NvmeLayerSQLDbDriver.class);
                bind(NvmeLayerDatabaseDriver.class).to(NvmeLayerSQLDbDriver.class);
                bind(OpenflexLayerCtrlDatabaseDriver.class).to(OpenflexLayerSQLDbDriver.class);
                bind(OpenflexLayerDatabaseDriver.class).to(OpenflexLayerSQLDbDriver.class);
                bind(WritecacheLayerCtrlDatabaseDriver.class).to(WritecacheLayerSQLDbDriver.class);
                bind(WritecacheLayerDatabaseDriver.class).to(WritecacheLayerSQLDbDriver.class);
                bind(CacheLayerCtrlDatabaseDriver.class).to(CacheLayerSQLDbDriver.class);
                bind(CacheLayerDatabaseDriver.class).to(CacheLayerSQLDbDriver.class);
                bind(BCacheLayerCtrlDatabaseDriver.class).to(BCacheLayerSQLDbDriver.class);
                bind(BCacheLayerDatabaseDriver.class).to(BCacheLayerSQLDbDriver.class);
                break;
            case ETCD:
                bind(ControllerDatabase.class).to(DbEtcd.class);
                bind(ControllerETCDDatabase.class).to(DbEtcd.class);
                bind(DbEngine.class).to(ETCDEngine.class);

                bind(DbInitializer.class).to(DbEtcdInitializer.class);
                bind(DbAccessor.class).to(DbEtcdPersistence.class);

                bind(ObjectProtectionDatabaseDriver.class).to(ObjectProtectionEtcdDriver.class);

                bind(PropsConDatabaseDriver.class).to(PropsConETCDDriver.class);

                bind(NodeCtrlDatabaseDriver.class).to(NodeETCDDriver.class);
                bind(NodeDatabaseDriver.class).to(NodeETCDDriver.class);

                bind(ResourceLayerIdCtrlDatabaseDriver.class).to(ResourceLayerIdETCDDriver.class);
                bind(ResourceLayerIdDatabaseDriver.class).to(ResourceLayerIdETCDDriver.class);
                bind(DrbdLayerCtrlDatabaseDriver.class).to(DrbdLayerETCDDriver.class);
                bind(DrbdLayerDatabaseDriver.class).to(DrbdLayerETCDDriver.class);
                bind(LuksLayerCtrlDatabaseDriver.class).to(LuksLayerETCDDriver.class);
                bind(LuksLayerDatabaseDriver.class).to(LuksLayerETCDDriver.class);
                bind(StorageLayerCtrlDatabaseDriver.class).to(StorageLayerETCDDriver.class);
                bind(StorageLayerDatabaseDriver.class).to(StorageLayerETCDDriver.class);
                bind(NvmeLayerCtrlDatabaseDriver.class).to(NvmeLayerETCDDriver.class);
                bind(NvmeLayerDatabaseDriver.class).to(NvmeLayerETCDDriver.class);
                bind(OpenflexLayerCtrlDatabaseDriver.class).to(OpenflexLayerETCDDriver.class);
                bind(OpenflexLayerDatabaseDriver.class).to(OpenflexLayerETCDDriver.class);
                bind(WritecacheLayerCtrlDatabaseDriver.class).to(WritecacheLayerETCDDriver.class);
                bind(WritecacheLayerDatabaseDriver.class).to(WritecacheLayerETCDDriver.class);
                bind(CacheLayerCtrlDatabaseDriver.class).to(CacheLayerETCDDriver.class);
                bind(CacheLayerDatabaseDriver.class).to(CacheLayerETCDDriver.class);
                bind(BCacheLayerCtrlDatabaseDriver.class).to(BCacheLayerETCDDriver.class);
                bind(BCacheLayerDatabaseDriver.class).to(BCacheLayerETCDDriver.class);
                break;
            case K8S_CRD:
                bind(ControllerDatabase.class).to(DbK8sCrd.class);
                bind(ControllerK8sCrdDatabase.class).to(DbK8sCrd.class);
                bind(DbEngine.class).to(K8sCrdEngine.class);

                bind(DbInitializer.class).to(DbK8sCrdInitializer.class);
                bind(DbAccessor.class).to(DbK8sCrdPersistence.class);

                bind(ObjectProtectionDatabaseDriver.class).to(ObjectProtectionK8sCrdDriver.class);

                bind(PropsConDatabaseDriver.class).to(PropsConK8sCrdDriver.class);

                bind(NodeCtrlDatabaseDriver.class).to(NodeDbDriver.class);
                bind(NodeDatabaseDriver.class).to(NodeDbDriver.class);

                bind(ResourceLayerIdCtrlDatabaseDriver.class).to(ResourceLayerIdK8sCrdDriver.class);
                bind(ResourceLayerIdDatabaseDriver.class).to(ResourceLayerIdK8sCrdDriver.class);
                bind(DrbdLayerCtrlDatabaseDriver.class).to(DrbdLayerK8sCrdDriver.class);
                bind(DrbdLayerDatabaseDriver.class).to(DrbdLayerK8sCrdDriver.class);
                bind(LuksLayerCtrlDatabaseDriver.class).to(LuksLayerK8sCrdDriver.class);
                bind(LuksLayerDatabaseDriver.class).to(LuksLayerK8sCrdDriver.class);
                bind(StorageLayerCtrlDatabaseDriver.class).to(StorageLayerK8sCrdDriver.class);
                bind(StorageLayerDatabaseDriver.class).to(StorageLayerK8sCrdDriver.class);
                bind(NvmeLayerCtrlDatabaseDriver.class).to(NvmeLayerK8sCrdDriver.class);
                bind(NvmeLayerDatabaseDriver.class).to(NvmeLayerK8sCrdDriver.class);
                bind(OpenflexLayerCtrlDatabaseDriver.class).to(OpenflexLayerK8sCrdDriver.class);
                bind(OpenflexLayerDatabaseDriver.class).to(OpenflexLayerK8sCrdDriver.class);
                bind(WritecacheLayerCtrlDatabaseDriver.class).to(WritecacheLayerK8sCrdDriver.class);
                bind(WritecacheLayerDatabaseDriver.class).to(WritecacheLayerK8sCrdDriver.class);
                bind(CacheLayerCtrlDatabaseDriver.class).to(CacheLayerK8sCrdDriver.class);
                bind(CacheLayerDatabaseDriver.class).to(CacheLayerK8sCrdDriver.class);
                bind(BCacheLayerCtrlDatabaseDriver.class).to(BCacheLayerK8sCrdDriver.class);
                bind(BCacheLayerDatabaseDriver.class).to(BCacheLayerK8sCrdDriver.class);
                break;
            default:
                throw new RuntimeException("Unknown database type: " + dbType);
        }
    }
}
