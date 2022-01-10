package com.linbit.linstor.dbdrivers.noop;

import com.linbit.linstor.stateflags.StateFlagsPersistence;

public class NoOpFlagDriver implements StateFlagsPersistence<Object>
{
    @Override
    public void persist(Object parent, long oldFlagBits, long newFlagBits)
    {
        // no-op
    }
}
