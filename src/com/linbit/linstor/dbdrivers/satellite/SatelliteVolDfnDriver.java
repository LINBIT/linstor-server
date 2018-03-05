package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.MinorNumber;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.VolumeDefinitionData;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDefinitionDataDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import javax.inject.Inject;

public class SatelliteVolDfnDriver implements VolumeDefinitionDataDatabaseDriver
{
    private final StateFlagsPersistence<?> stateFlagsDriver = new SatelliteFlagDriver();
    private final SingleColumnDatabaseDriver<?, ?> singleColDriver = new SatelliteSingleColDriver<>();
    private final AccessContext dbCtx;

    @Inject
    public SatelliteVolDfnDriver(@SystemContext AccessContext dbCtxRef)
    {
        dbCtx = dbCtxRef;
    }

    @SuppressWarnings("unchecked")
    @Override
    public StateFlagsPersistence<VolumeDefinitionData> getStateFlagsPersistence()
    {
        return (StateFlagsPersistence<VolumeDefinitionData>) stateFlagsDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<VolumeDefinitionData, MinorNumber> getMinorNumberDriver()
    {
        return (SingleColumnDatabaseDriver<VolumeDefinitionData, MinorNumber>) singleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<VolumeDefinitionData, Long> getVolumeSizeDriver()
    {
        return (SingleColumnDatabaseDriver<VolumeDefinitionData, Long>) singleColDriver;
    }

    @Override
    public void create(VolumeDefinitionData volDfnData)
    {
        // no-op
    }

    @Override
    public VolumeDefinitionData load(
        ResourceDefinition resourceDefinition,
        VolumeNumber volumeNumber,
        boolean logWarnIfNotExists
    )
    {
        VolumeDefinitionData volumeDfn = null;
        try
        {
            volumeDfn = (VolumeDefinitionData) resourceDefinition.getVolumeDfn(dbCtx, volumeNumber);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            SatelliteDbDriverExceptionHandler.handleAccessDeniedException(accDeniedExc);
        }
        return volumeDfn;
    }

    @Override
    public void delete(VolumeDefinitionData data)
    {
        // no-op
    }
}
