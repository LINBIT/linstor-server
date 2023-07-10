package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.SatelliteDbDriver;
import com.linbit.linstor.dbdrivers.interfaces.BCacheLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.CacheLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ExternalFileDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerDrbdRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerDrbdRscDfnDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerDrbdVlmDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerDrbdVlmDfnDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerStorageRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerStorageVlmDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LuksLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NetInterfaceDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NodeConnectionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NodeDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NvmeLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.OpenflexLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.PropsConDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceConnectionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDefinitionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceGroupDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDefinitionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDefinitionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDefinitionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeConnectionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDefinitionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeGroupDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.WritecacheLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.remotes.EbsRemoteDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.remotes.S3RemoteDatabaseDriver;
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
        bind(ExternalFileDatabaseDriver.class).to(SatelliteExternalFileDriver.class);
        bind(S3RemoteDatabaseDriver.class).to(SatelliteS3RemoteDriver.class);
        bind(EbsRemoteDatabaseDriver.class).to(SatelliteEbsRemoteDriver.class);

        bind(ResourceGroupDatabaseDriver.class).to(SatelliteRscGrpDriver.class);
        bind(VolumeGroupDatabaseDriver.class).to(SatelliteVlmGrpDriver.class);

        bind(LayerResourceIdDatabaseDriver.class).to(SatelliteLayerResourceIdDriver.class);
        bind(LayerDrbdRscDfnDatabaseDriver.class).to(SatelliteLayerDrbdRscDfnDbDriver.class);
        bind(LayerDrbdVlmDfnDatabaseDriver.class).to(SatelliteLayerDrbdVlmDfnDbDriver.class);
        bind(LayerDrbdRscDatabaseDriver.class).to(SatelliteLayerDrbdRscDbDriver.class);
        bind(LayerDrbdVlmDatabaseDriver.class).to(SatelliteLayerDrbdVlmDbDriver.class);

        bind(LayerStorageRscDatabaseDriver.class).to(SatelliteLayerStorageRscDbDriver.class);
        bind(LayerStorageVlmDatabaseDriver.class).to(SatelliteLayerStorageVlmDbDriver.class);

        bind(LuksLayerDatabaseDriver.class).to(SatelliteLuksDriver.class);
        bind(NvmeLayerDatabaseDriver.class).to(SatelliteNvmeLayerDriver.class);
        bind(OpenflexLayerDatabaseDriver.class).to(SatelliteOpenflexLayerDriver.class);
        bind(WritecacheLayerDatabaseDriver.class).to(SatelliteWritecacheLayerDriver.class);
        bind(CacheLayerDatabaseDriver.class).to(SatelliteCacheLayerDriver.class);
        bind(BCacheLayerDatabaseDriver.class).to(SatelliteBCacheLayerDriver.class);
    }
}
