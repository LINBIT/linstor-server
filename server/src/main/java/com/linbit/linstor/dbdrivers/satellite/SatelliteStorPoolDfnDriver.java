package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDefinitionDatabaseDriver;

import javax.inject.Inject;

public class SatelliteStorPoolDfnDriver implements StorPoolDefinitionDatabaseDriver
{
    private final CoreModule.StorPoolDefinitionMap storPoolDfnMap;

    @Inject
    public SatelliteStorPoolDfnDriver(CoreModule.StorPoolDefinitionMap storPoolDfnMapRef)
    {
        storPoolDfnMap = storPoolDfnMapRef;
    }

    @Override
    public void create(StorPoolDefinition storPoolDefinition)
    {
        // no-op
    }

    @Override
    public void delete(StorPoolDefinition data)
    {
        // no-op
    }

    @Override
    public StorPoolDefinition createDefaultDisklessStorPool()
    {
        return null;
    }
}
