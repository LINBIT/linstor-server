package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDefinitionDataDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import javax.inject.Inject;

public class SatelliteResDfnDriver implements ResourceDefinitionDataDatabaseDriver
{
    private final StateFlagsPersistence<?> stateFlagsDriver = new SatelliteFlagDriver();
    private final SingleColumnDatabaseDriver<?, ?> singleColDriver = new SatelliteSingleColDriver<>();
    private final CoreModule.ResourceDefinitionMap resDfnMap;

    @Inject
    public SatelliteResDfnDriver(CoreModule.ResourceDefinitionMap resDfnMapRef)
    {
        resDfnMap = resDfnMapRef;
    }

    @SuppressWarnings("unchecked")
    @Override
    public StateFlagsPersistence<ResourceDefinitionData> getStateFlagsPersistence()
    {
        return (StateFlagsPersistence<ResourceDefinitionData>) stateFlagsDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<ResourceDefinitionData, TcpPortNumber> getPortDriver()
    {
        return (SingleColumnDatabaseDriver<ResourceDefinitionData, TcpPortNumber>) singleColDriver;
    }

    @Override
    public void create(ResourceDefinitionData resDfn)
    {
        // no-op
    }

    @Override
    public boolean exists(ResourceName resourceName)
    {
        return resDfnMap.containsKey(resourceName);
    }

    @Override
    public ResourceDefinitionData load(ResourceName resourceName, boolean logWarnIfNotExists)
    {
        return (ResourceDefinitionData) resDfnMap.get(resourceName);
    }

    @Override
    public void delete(ResourceDefinitionData data)
    {
        // no-op
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<ResourceDefinitionData, ResourceDefinition.TransportType> getTransportTypeDriver()
    {
        return (SingleColumnDatabaseDriver<ResourceDefinitionData, ResourceDefinition.TransportType>) singleColDriver;
    }
}
