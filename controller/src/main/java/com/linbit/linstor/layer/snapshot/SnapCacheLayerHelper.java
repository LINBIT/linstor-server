package com.linbit.linstor.layer.snapshot;

import com.linbit.ExhaustedPoolException;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.adapter.cache.CacheRscData;
import com.linbit.linstor.storage.data.adapter.cache.CacheVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerDataFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

@Singleton
class SnapCacheLayerHelper extends AbsSnapLayerHelper<
    CacheRscData<Snapshot>, CacheVlmData<Snapshot>,
    RscDfnLayerObject, VlmDfnLayerObject>
{
    @Inject
    SnapCacheLayerHelper(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        LayerDataFactory layerDataFactoryRef,
        @Named(NumberPoolModule.LAYER_RSC_ID_POOL) DynamicNumberPool layerRscIdPoolRef
    )
    {
        super(
            errorReporterRef,
            apiCtxRef,
            layerDataFactoryRef,
            layerRscIdPoolRef,
            DeviceLayerKind.CACHE
        );
    }

    @Override
    protected RscDfnLayerObject createSnapDfnData(
        SnapshotDefinition rscDfnRef,
        String rscNameSuffixRef
    )
    {
        // CacheLayer does not have resource-definition specific data
        return null;
    }

    @Override
    protected VlmDfnLayerObject createSnapVlmDfnData(
        SnapshotVolumeDefinition snapVlmDfnRef,
        String rscNameSuffixRef
    )
    {
        // CacheLayer does not have volume-definition specific data
        return null;
    }

    @Override
    protected CacheRscData<Snapshot> createSnapData(
        Snapshot snapRef,
        AbsRscLayerObject<Resource> rscDataRef,
        AbsRscLayerObject<Snapshot> parentRef
    ) throws AccessDeniedException, DatabaseException, ExhaustedPoolException
    {
        return layerDataFactory.createCacheRscData(
            layerRscIdPool.autoAllocate(),
            snapRef,
            rscDataRef.getResourceNameSuffix(),
            parentRef
        );
    }

    @Override
    protected CacheVlmData<Snapshot> createSnapVlmLayerData(
        SnapshotVolume snapVlmRef,
        CacheRscData<Snapshot> snapDataRef,
        VlmProviderObject<Resource> vlmProviderObjectRef
    ) throws DatabaseException, AccessDeniedException
    {
        return layerDataFactory.createCacheVlmData(
            snapVlmRef,
            ((CacheVlmData<Resource>) vlmProviderObjectRef).getCacheStorPool(),
            ((CacheVlmData<Resource>) vlmProviderObjectRef).getMetaStorPool(),
            snapDataRef
        );
    }
}
