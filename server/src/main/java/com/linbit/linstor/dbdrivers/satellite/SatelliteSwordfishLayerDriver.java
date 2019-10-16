package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SwordfishLayerDatabaseDriver;
import com.linbit.linstor.storage.data.provider.swordfish.SfInitiatorData;
import com.linbit.linstor.storage.data.provider.swordfish.SfTargetData;
import com.linbit.linstor.storage.data.provider.swordfish.SfVlmDfnData;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteSwordfishLayerDriver implements SwordfishLayerDatabaseDriver
{
    private final SingleColumnDatabaseDriver<?, ?> noopSingleColDriver = new SatelliteSingleColDriver<>();

    @Inject
    public SatelliteSwordfishLayerDriver()
    {
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<SfVlmDfnData, String> getVlmDfnOdataDriver()
    {
        return (SingleColumnDatabaseDriver<SfVlmDfnData, String>) noopSingleColDriver;
    }

    @Override
    public void persist(SfVlmDfnData vlmDfnDataRef)
    {
        // no-op
    }

    @Override
    public void delete(SfVlmDfnData vlmDfnDataRef)
    {
        // no-op
    }

    @Override
    public void persist(SfInitiatorData<?> sfInitiatorDataRef)
    {
        // no-op
    }

    @Override
    public void delete(SfInitiatorData<?> sfInitiatorDataRef)
    {
        // no-op
    }

    @Override
    public void persist(SfTargetData<?> sfInitiatorDataRef)
    {
        // no-op
    }

    @Override
    public void delete(SfTargetData<?> sfInitiatorDataRef)
    {
        // no-op
    }
}
