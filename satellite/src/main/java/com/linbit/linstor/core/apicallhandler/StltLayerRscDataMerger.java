package com.linbit.linstor.core.apicallhandler;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.FreeSpaceMgrSatelliteFactory;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolDataSatelliteFactory;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.StorPoolDefinitionDataSatelliteFactory;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.Volume;
import com.linbit.linstor.StorPool.StorPoolApi;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.core.CoreModule.StorPoolDefinitionMap;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.utils.LayerDataFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class StltLayerRscDataMerger extends LayerRscDataMerger
{
    private final StorPoolDefinitionMap storPoolDfnMap;
    private final StorPoolDefinitionDataSatelliteFactory storPoolDefinitionDataFactory;
    private final StorPoolDataSatelliteFactory storPoolDataFactory;
    private final FreeSpaceMgrSatelliteFactory freeSpaceMgrFactory;

    @Inject
    public StltLayerRscDataMerger(
        @SystemContext AccessContext apiCtxRef,
        LayerDataFactory layerDataFactoryRef,
        StorPoolDefinitionMap storPoolDfnMapRef,
        StorPoolDefinitionDataSatelliteFactory storPoolDefinitionDataFactoryRef,
        StorPoolDataSatelliteFactory storPoolDataFactoryRef,
        FreeSpaceMgrSatelliteFactory freeSpaceMgrFactoryRef
    )
    {
        super(apiCtxRef, layerDataFactoryRef);
        storPoolDfnMap = storPoolDfnMapRef;
        storPoolDefinitionDataFactory = storPoolDefinitionDataFactoryRef;
        storPoolDataFactory = storPoolDataFactoryRef;
        freeSpaceMgrFactory = freeSpaceMgrFactoryRef;
    }

    @Override
    protected StorPool getStoragePool(
        Volume vlm,
        VlmLayerDataApi vlmPojo,
        boolean remoteResource
    )
        throws InvalidNameException, AccessDeniedException
    {
        StorPool storPool = super.getStoragePool(vlm, vlmPojo, remoteResource);
        if (storPool == null)
        {
            if (remoteResource)
            {
                StorPoolApi storPoolApi = vlmPojo.getStorPoolApi();

                StorPoolDefinition storPoolDfn = storPoolDfnMap.get(new StorPoolName(storPoolApi.getStorPoolName()));
                if (storPoolDfn == null)
                {
                    storPoolDfn = storPoolDefinitionDataFactory.getInstance(
                        apiCtx,
                        storPoolApi.getStorPoolDfnUuid(),
                        new StorPoolName(storPoolApi.getStorPoolName())
                    );
                    storPoolDfn.getProps(apiCtx).map().putAll(storPoolApi.getStorPoolDfnProps());
                    storPoolDfnMap.put(storPoolDfn.getName(), storPoolDfn);
                }
                storPool = storPoolDataFactory.getInstanceSatellite(
                    apiCtx,
                    storPoolApi.getStorPoolUuid(),
                    vlm.getResource().getAssignedNode(),
                    storPoolDfn,
                    storPoolApi.getDeviceProviderKind(),
                    freeSpaceMgrFactory.getInstance()
                );
                storPool.getProps(apiCtx).map().putAll(storPoolApi.getStorPoolProps());
            }
            else
            {
                throw new ImplementationError("Unknown storage pool '" + vlmPojo.getStorPoolApi().getStorPoolName() +
                    "' for volume " + vlm);
            }
        }
        return storPool;
    }
}
