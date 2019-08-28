package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.ImplementationError;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.LuksLayerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.luks.LuksRscData;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.utils.Pair;

import javax.inject.Inject;

import java.util.Set;

public class SatelliteLuksDriver implements LuksLayerDatabaseDriver
{
    private final SingleColumnDatabaseDriver<?, ?> noopSingleColDriver = new SatelliteSingleColDriver<>();
    private final ResourceLayerIdDatabaseDriver noopResourceLayerIdDriver = new SatelliteResourceLayerIdDriver();

    @Inject
    public SatelliteLuksDriver()
    {
    }

    @Override
    public ResourceLayerIdDatabaseDriver getIdDriver()
    {
        return noopResourceLayerIdDriver;
    }

    @Override
    public void persist(LuksRscData luksRscDataRef)
    {
        // no-op
    }

    @Override
    public void delete(LuksRscData luksRscDataRef)
    {
        // no-op
    }

    @Override
    public void persist(LuksVlmData luksVlmDataRef)
    {
        // no-op
    }

    @Override
    public void delete(LuksVlmData luksVlmDataRef)
    {
        // no-op
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<LuksVlmData, byte[]> getVlmEncryptedPasswordDriver()
    {
        return (SingleColumnDatabaseDriver<LuksVlmData, byte[]>) noopSingleColDriver;
    }

    @Override
    public Pair<? extends RscLayerObject, Set<RscLayerObject>> load(
        Resource rscRef,
        int idRef,
        String rscSuffixRef,
        RscLayerObject parentRef
    )
        throws DatabaseException
    {
        throw new ImplementationError("This method should never be called on satellite");
    }
}
