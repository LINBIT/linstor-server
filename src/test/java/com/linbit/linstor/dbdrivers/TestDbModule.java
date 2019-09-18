package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.objects.DrbdLayerGenericDbDriver;
import com.linbit.linstor.core.objects.LuksLayerGenericDbDriver;
import com.linbit.linstor.core.objects.NetInterfaceGenericDbDriver;
import com.linbit.linstor.core.objects.NodeConnectionGenericDbDriver;
import com.linbit.linstor.core.objects.NodeGenericDbDriver;
import com.linbit.linstor.core.objects.NvmeLayerGenericDbDriver;
import com.linbit.linstor.core.objects.ResourceConnectionGenericDbDriver;
import com.linbit.linstor.core.objects.ResourceGenericDbDriver;
import com.linbit.linstor.core.objects.ResourceDefinitionGenericDbDriver;
import com.linbit.linstor.core.objects.ResourceGroupGenericDbDriver;
import com.linbit.linstor.core.objects.ResourceLayerIdGenericDbDriver;
import com.linbit.linstor.core.objects.SnapshotGenericDbDriver;
import com.linbit.linstor.core.objects.SnapshotDefinitionGenericDbDriver;
import com.linbit.linstor.core.objects.SnapshotVolumeGenericDbDriver;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinitionGenericDbDriver;
import com.linbit.linstor.core.objects.StorPoolGenericDbDriver;
import com.linbit.linstor.core.objects.StorPoolDefinitionGenericDbDriver;
import com.linbit.linstor.core.objects.StorageLayerGenericDbDriver;
import com.linbit.linstor.core.objects.SwordfishLayerGenericDbDriver;
import com.linbit.linstor.core.objects.VolumeConnectionDataGenericDbDriver;
import com.linbit.linstor.core.objects.VolumeDataGenericDbDriver;
import com.linbit.linstor.core.objects.VolumeDefinitionDataGenericDbDriver;
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
import com.linbit.linstor.dbdrivers.interfaces.SwordfishLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeConnectionDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDefinitionDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.sql.SQLEngine;
import com.linbit.linstor.propscon.PropsConGenericDbDriver;
import com.linbit.linstor.security.DbAccessor;
import com.linbit.linstor.security.DbSQLPersistence;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.linstor.security.ObjectProtectionGenericDbDriver;

import com.google.inject.AbstractModule;

public class TestDbModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(DbAccessor.class).to(DbSQLPersistence.class);
        bind(DbEngine.class).to(SQLEngine.class);

        bind(ObjectProtectionDatabaseDriver.class).to(ObjectProtectionGenericDbDriver.class);

        bind(DatabaseDriver.class).to(DatabaseLoader.class);

        bind(PropsConDatabaseDriver.class).to(PropsConGenericDbDriver.class);
        bind(NodeDatabaseDriver.class).to(NodeGenericDbDriver.class);
        bind(ResourceGroupDatabaseDriver.class).to(ResourceGroupGenericDbDriver.class);
        bind(ResourceDefinitionDatabaseDriver.class).to(ResourceDefinitionGenericDbDriver.class);
        bind(ResourceDatabaseDriver.class).to(ResourceGenericDbDriver.class);
        bind(VolumeDefinitionDataDatabaseDriver.class).to(VolumeDefinitionDataGenericDbDriver.class);
        bind(VolumeDataDatabaseDriver.class).to(VolumeDataGenericDbDriver.class);
        bind(StorPoolDefinitionDatabaseDriver.class).to(StorPoolDefinitionGenericDbDriver.class);
        bind(StorPoolDatabaseDriver.class).to(StorPoolGenericDbDriver.class);
        bind(NetInterfaceDatabaseDriver.class).to(NetInterfaceGenericDbDriver.class);
        bind(NodeConnectionDatabaseDriver.class).to(NodeConnectionGenericDbDriver.class);
        bind(ResourceConnectionDatabaseDriver.class).to(ResourceConnectionGenericDbDriver.class);
        bind(VolumeConnectionDataDatabaseDriver.class).to(VolumeConnectionDataGenericDbDriver.class);
        bind(SnapshotDefinitionDatabaseDriver.class).to(SnapshotDefinitionGenericDbDriver.class);
        bind(SnapshotVolumeDefinitionDatabaseDriver.class).to(SnapshotVolumeDefinitionGenericDbDriver.class);
        bind(SnapshotDatabaseDriver.class).to(SnapshotGenericDbDriver.class);
        bind(SnapshotVolumeDatabaseDriver.class).to(SnapshotVolumeGenericDbDriver.class);

        bind(ResourceLayerIdDatabaseDriver.class).to(ResourceLayerIdGenericDbDriver.class);
        bind(DrbdLayerDatabaseDriver.class).to(DrbdLayerGenericDbDriver.class);
        bind(LuksLayerDatabaseDriver.class).to(LuksLayerGenericDbDriver.class);
        bind(StorageLayerDatabaseDriver.class).to(StorageLayerGenericDbDriver.class);
        bind(SwordfishLayerDatabaseDriver.class).to(SwordfishLayerGenericDbDriver.class);
        bind(NvmeLayerDatabaseDriver.class).to(NvmeLayerGenericDbDriver.class);
    }
}
