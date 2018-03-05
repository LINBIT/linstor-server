package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.linstor.stateflags.StateFlagsPersistence;

public class SatelliteFlagDriver implements StateFlagsPersistence<Object>
{
    @Override
    public void persist(Object parent, long flags)
    {
        // no-op
    }
}
