package com.linbit.linstor.numberpool;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.ValueInUseException;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteDynamicNumberPool implements DynamicNumberPool
{
    @Inject
    public SatelliteDynamicNumberPool()
    {
    }

    @Override
    public void reloadRange()
    {
        // no-op
    }

    @Override
    public void allocate(int nrRef) throws ValueInUseException
    {
        // no-op
    }

    @Override
    public int autoAllocate() throws ExhaustedPoolException
    {
        throw new ImplementationError("Satellite should not autoAllocate numbers!");
    }

    @Override
    public void deallocate(int nrRef)
    {
        // no-op
    }

    @Override
    public int getRangeMin() { return 0; }

    @Override
    public int getRangeMax() { return 0; }
}
