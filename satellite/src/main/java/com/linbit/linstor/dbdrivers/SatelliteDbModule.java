package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.SatelliteDbDriver;
import com.linbit.linstor.dbdrivers.interfaces.ExternalFileDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerBCacheRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerBCacheVlmDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerCacheRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerCacheVlmDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerDrbdRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerDrbdRscDfnDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerDrbdVlmDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerDrbdVlmDfnDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerLuksRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerLuksVlmDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerNvmeRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerStorageRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerStorageVlmDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerWritecacheRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerWritecacheVlmDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NetInterfaceDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NodeConnectionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NodeDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.PropsDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceConnectionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDefinitionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceGroupDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecConfigDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecIdRoleDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecIdentityDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecObjProtAclDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecObjProtDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecRoleDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecTypeDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecTypeRulesDatabaseDriver;
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
import com.linbit.linstor.dbdrivers.interfaces.remotes.EbsRemoteDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.remotes.S3RemoteDatabaseDriver;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.security.ObjectProtectionStltFactory;
import com.linbit.linstor.security.SatelliteSecConfigDbDriver;
import com.linbit.linstor.security.SatelliteSecIdRoleDbDriver;
import com.linbit.linstor.security.SatelliteSecIdentityDbDriver;
import com.linbit.linstor.security.SatelliteSecObjProtAclDbDriver;
import com.linbit.linstor.security.SatelliteSecObjProtDbDriver;
import com.linbit.linstor.security.SatelliteSecRoleDbDriver;
import com.linbit.linstor.security.SatelliteSecTypeDbDriver;
import com.linbit.linstor.security.SatelliteSecTypeRulesDbDriver;

import com.google.inject.AbstractModule;

public class SatelliteDbModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        // we need to override objProtFactory so that every requested objProt already exists without needing to change
        // the "failIfNotExists" everywhere in production code
        bind(ObjectProtectionFactory.class).to(ObjectProtectionStltFactory.class);

        bind(DatabaseDriver.class).to(SatelliteDbDriver.class);

        bind(PropsDatabaseDriver.class).to(SatellitePropDriver.class);
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

        bind(LayerLuksRscDatabaseDriver.class).to(SatelliteLayerLuksRscDriver.class);
        bind(LayerLuksVlmDatabaseDriver.class).to(SatelliteLayerLuksVlmDriver.class);

        bind(LayerNvmeRscDatabaseDriver.class).to(SatelliteLayerNvmeRscDbDriver.class);

        bind(LayerWritecacheRscDatabaseDriver.class).to(SatelliteLayerWritecacheRscDbDriver.class);
        bind(LayerWritecacheVlmDatabaseDriver.class).to(SatelliteLayerWritecacheVlmDbDriver.class);

        bind(LayerCacheRscDatabaseDriver.class).to(SatelliteLayerCacheRscDbDriver.class);
        bind(LayerCacheVlmDatabaseDriver.class).to(SatelliteLayerCacheVlmDbDriver.class);

        bind(LayerBCacheRscDatabaseDriver.class).to(SatelliteLayerBCacheRscDbDriver.class);
        bind(LayerBCacheVlmDatabaseDriver.class).to(SatelliteLayerBCacheVlmDbDriver.class);

        bind(SecConfigDatabaseDriver.class).to(SatelliteSecConfigDbDriver.class);
        bind(SecIdentityDatabaseDriver.class).to(SatelliteSecIdentityDbDriver.class);
        bind(SecIdRoleDatabaseDriver.class).to(SatelliteSecIdRoleDbDriver.class);
        bind(SecObjProtAclDatabaseDriver.class).to(SatelliteSecObjProtAclDbDriver.class);
        bind(SecObjProtDatabaseDriver.class).to(SatelliteSecObjProtDbDriver.class);
        bind(SecRoleDatabaseDriver.class).to(SatelliteSecRoleDbDriver.class);
        bind(SecTypeDatabaseDriver.class).to(SatelliteSecTypeDbDriver.class);
        bind(SecTypeRulesDatabaseDriver.class).to(SatelliteSecTypeRulesDbDriver.class);
    }
}
