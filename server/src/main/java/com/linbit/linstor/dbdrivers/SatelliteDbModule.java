package com.linbit.linstor.dbdrivers;

import com.google.inject.AbstractModule;
import com.linbit.linstor.SatelliteDbDriver;
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
import com.linbit.linstor.dbdrivers.satellite.SatelliteNiDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteNodeConDfnDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteNodeDriver;
import com.linbit.linstor.dbdrivers.satellite.SatellitePropDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteResConDfnDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteResDfnDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteResDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteSnapshotDfnDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteSnapshotDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteSnapshotVlmDfnDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteSnapshotVlmDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteStorPoolDfnDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteStorPoolDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteVolConDfnDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteVolDfnDriver;
import com.linbit.linstor.dbdrivers.satellite.SatelliteVolDriver;
import com.linbit.linstor.security.DbAccessor;
import com.linbit.linstor.security.EmptySecurityDbDriver;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;

public class SatelliteDbModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(DbAccessor.class).to(EmptySecurityDbDriver.class);

        bind(ObjectProtectionDatabaseDriver.class).to(EmptySecurityDbDriver.EmptyObjectProtectionDatabaseDriver.class);

        bind(DatabaseDriver.class).to(SatelliteDbDriver.class);

        bind(PropsConDatabaseDriver.class).to(SatellitePropDriver.class);
        bind(NodeDataDatabaseDriver.class).to(SatelliteNodeDriver.class);
        bind(ResourceDefinitionDataDatabaseDriver.class).to(SatelliteResDfnDriver.class);
        bind(ResourceDataDatabaseDriver.class).to(SatelliteResDriver.class);
        bind(VolumeDefinitionDataDatabaseDriver.class).to(SatelliteVolDfnDriver.class);
        bind(VolumeDataDatabaseDriver.class).to(SatelliteVolDriver.class);
        bind(StorPoolDefinitionDataDatabaseDriver.class).to(SatelliteStorPoolDfnDriver.class);
        bind(StorPoolDataDatabaseDriver.class).to(SatelliteStorPoolDriver.class);
        bind(NetInterfaceDataDatabaseDriver.class).to(SatelliteNiDriver.class);
        bind(NodeConnectionDataDatabaseDriver.class).to(SatelliteNodeConDfnDriver.class);
        bind(ResourceConnectionDataDatabaseDriver.class).to(SatelliteResConDfnDriver.class);
        bind(VolumeConnectionDataDatabaseDriver.class).to(SatelliteVolConDfnDriver.class);
        bind(SnapshotDefinitionDataDatabaseDriver.class).to(SatelliteSnapshotDfnDriver.class);
        bind(SnapshotVolumeDefinitionDatabaseDriver.class).to(SatelliteSnapshotVlmDfnDriver.class);
        bind(SnapshotDataDatabaseDriver.class).to(SatelliteSnapshotDriver.class);
        bind(SnapshotVolumeDataDatabaseDriver.class).to(SatelliteSnapshotVlmDriver.class);
    }
}
