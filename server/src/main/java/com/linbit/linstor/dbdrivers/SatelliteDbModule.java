package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.SatelliteDbDriver;
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
import com.linbit.linstor.dbdrivers.interfaces.VolumeGroupDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.WritecacheLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteDrbdLayerDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteLuksDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteNiDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteNodeConDfnDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteNodeDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteNvmeLayerDriver;
import com.linbit.linstor.dbdrivers.satellite.SatellitePropDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteResConDfnDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteResDfnDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteResDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteResourceLayerIdDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteRscGrpDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteSnapshotDfnDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteSnapshotDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteSnapshotVlmDfnDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteSnapshotVlmDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteStorPoolDfnDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteStorPoolDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteStorageLayerDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteVlmGrpDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteVolConDfnDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteVolDfnDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteVolDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteWritecacheLayerDriver;
import com.linbit.linstor.security.DbAccessor;
import com.linbit.linstor.security.EmptySecurityDbDriver;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;

import com.google.inject.AbstractModule;

public class SatelliteDbModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(DbAccessor.class).to(EmptySecurityDbDriver.class);

        bind(ObjectProtectionDatabaseDriver.class).to(EmptySecurityDbDriver.EmptyObjectProtectionDatabaseDriver.class);

        bind(DatabaseDriver.class).to(SatelliteDbDriver.class);

        bind(PropsConDatabaseDriver.class).to(SatellitePropDriver.class);
        bind(NodeDatabaseDriver.class).to(SatelliteNodeDriver.class);
        bind(ResourceDefinitionDatabaseDriver.class).to(SatelliteResDfnDriver.class);
        bind(ResourceDatabaseDriver.class).to(SatelliteResDriver.class);
        bind(VolumeDefinitionDatabaseDriver.class).to(SatelliteVolDfnDriver.class);
        bind(VolumeDatabaseDriver.class).to(SatelliteVolDriver.class);
        bind(StorPoolDefinitionDatabaseDriver.class).to(SatelliteStorPoolDfnDriver.class);
        bind(StorPoolDatabaseDriver.class).to(SatelliteStorPoolDriver.class);
        bind(NetInterfaceDatabaseDriver.class).to(SatelliteNiDriver.class);
        bind(NodeConnectionDatabaseDriver.class).to(SatelliteNodeConDfnDriver.class);
        bind(ResourceConnectionDatabaseDriver.class).to(SatelliteResConDfnDriver.class);
        bind(VolumeConnectionDatabaseDriver.class).to(SatelliteVolConDfnDriver.class);
        bind(SnapshotDefinitionDatabaseDriver.class).to(SatelliteSnapshotDfnDriver.class);
        bind(SnapshotVolumeDefinitionDatabaseDriver.class).to(SatelliteSnapshotVlmDfnDriver.class);
        bind(SnapshotDatabaseDriver.class).to(SatelliteSnapshotDriver.class);
        bind(SnapshotVolumeDatabaseDriver.class).to(SatelliteSnapshotVlmDriver.class);

        bind(ResourceGroupDatabaseDriver.class).to(SatelliteRscGrpDriver.class);
        bind(VolumeGroupDatabaseDriver.class).to(SatelliteVlmGrpDriver.class);

        bind(ResourceLayerIdDatabaseDriver.class).to(SatelliteResourceLayerIdDriver.class);
        bind(DrbdLayerDatabaseDriver.class).to(SatelliteDrbdLayerDriver.class);
        bind(LuksLayerDatabaseDriver.class).to(SatelliteLuksDriver.class);
        bind(StorageLayerDatabaseDriver.class).to(SatelliteStorageLayerDriver.class);
        bind(NvmeLayerDatabaseDriver.class).to(SatelliteNvmeLayerDriver.class);
        bind(WritecacheLayerDatabaseDriver.class).to(SatelliteWritecacheLayerDriver.class);
    }
}
