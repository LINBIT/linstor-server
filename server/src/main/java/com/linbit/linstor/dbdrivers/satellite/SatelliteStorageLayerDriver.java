package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorageLayerDatabaseDriver;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Map;
import java.util.Set;

@Singleton
public class SatelliteStorageLayerDriver implements StorageLayerDatabaseDriver
{
    private static final SatelliteResourceLayerIdDriver NO_OP_ID_DRIVER = new SatelliteResourceLayerIdDriver();
    private static final SingleColumnDatabaseDriver<?, ?> NO_OP_SINGLE_COL_DRIVER = new SatelliteSingleColDriver<>();

    @Inject
    public SatelliteStorageLayerDriver()
    {
    }

    @Override
    public void persist(StorageRscData storageRscDataRef)
    {
        // no-op
    }

    @Override
    public void delete(StorageRscData storgeRscDataRef)
    {
        // no-op
    }

    @Override
    public void persist(VlmProviderObject vlmDataRef)
    {
        // no-op
    }

    @Override
    public void delete(VlmProviderObject vlmDataRef)
    {
        // no-op
    }

    @Override
    public ResourceLayerIdDatabaseDriver getIdDriver()
    {
        return NO_OP_ID_DRIVER;
    }

    @Override
    public SingleColumnDatabaseDriver<VlmProviderObject, StorPool> getStorPoolDriver()
    {
        return (SingleColumnDatabaseDriver<VlmProviderObject, StorPool>) NO_OP_SINGLE_COL_DRIVER;
    }

    @Override
    public void fetchForLoadAll(Map<Pair<NodeName, StorPoolName>, Pair<StorPool, StorPool.InitMaps>> tmpStorPoolMapRef)
        throws DatabaseException
    {
        // no-op
    }

    @Override
    public void loadLayerData(Map<ResourceName, ResourceDefinition> tmpRscDfnMapRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public void clearLoadAllCache()
    {
        // no-op
    }

    @Override
    public Pair<? extends RscLayerObject, Set<RscLayerObject>> load(
        Resource rscRef,
        int idRef,
        String rscSuffixRef,
        RscLayerObject parentRef
    )
        throws AccessDeniedException, DatabaseException
    {
        return null; // should never be called
    }
}
