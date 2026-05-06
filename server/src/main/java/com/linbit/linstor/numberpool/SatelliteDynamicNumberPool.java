package com.linbit.linstor.numberpool;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.ValueInUseException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.propscon.Props;

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
    public void reloadRange(@Nullable Props ignoredRef)
    {
        // no-op
    }

    @Override
    public void reloadBlockedRange(@Nullable Props ignoredRef)
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
    public boolean tryAllocate(int nrRef)
    {
        return true;
    }
}
