package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDatabaseDriver;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
public class SatelliteSnapshotVlmDriver
    extends AbsSatelliteDbDriver<SnapshotVolume>
    implements SnapshotVolumeDatabaseDriver
{
    @Inject
    public SatelliteSnapshotVlmDriver()
    {
        // noop
    }
}
