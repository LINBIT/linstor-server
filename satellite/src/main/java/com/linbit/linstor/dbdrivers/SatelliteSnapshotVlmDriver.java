package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDatabaseDriver;

import javax.inject.Inject;
import javax.inject.Singleton;

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
