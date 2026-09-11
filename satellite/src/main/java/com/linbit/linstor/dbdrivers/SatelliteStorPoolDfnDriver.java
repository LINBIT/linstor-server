package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDefinitionDatabaseDriver;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
public class SatelliteStorPoolDfnDriver
    extends AbsSatelliteDbDriver<StorPoolDefinition>
    implements StorPoolDefinitionDatabaseDriver
{
    @Inject
    public SatelliteStorPoolDfnDriver()
    {
        // no-op
    }

    @Override
    public @Nullable StorPoolDefinition createDefaultDisklessStorPool()
    {
        return null;
    }
}
