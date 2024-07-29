package com.linbit.linstor.layer.snapshot;

import com.linbit.ExhaustedPoolException;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.BCacheRscPojo.BCacheVlmPojo;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.AbsLayerHelperUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.adapter.bcache.BCacheRscData;
import com.linbit.linstor.storage.data.adapter.bcache.BCacheVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerDataFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import java.util.Map;

@Singleton
class SnapBCacheLayerHelper extends AbsSnapLayerHelper<
    BCacheRscData<Snapshot>, BCacheVlmData<Snapshot>,
    RscDfnLayerObject, VlmDfnLayerObject>
{
    @Inject
    SnapBCacheLayerHelper(
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
            DeviceLayerKind.BCACHE
        );
    }

    @Override
    protected @Nullable RscDfnLayerObject createSnapDfnData(
        SnapshotDefinition rscDfnRef,
        String rscNameSuffixRef
    )
    {
        // BCacheLayer does not have resource-definition specific data
        return null;
    }

    @Override
    protected @Nullable VlmDfnLayerObject createSnapVlmDfnData(
        SnapshotVolumeDefinition snapVlmDfnRef,
        String rscNameSuffixRef
    )
    {
        // BCacheLayer does not have volume-definition specific data
        return null;
    }

    @Override
    protected BCacheRscData<Snapshot> createSnapData(
        Snapshot snapRef,
        AbsRscLayerObject<Resource> rscDataRef,
        AbsRscLayerObject<Snapshot> parentRef
    ) throws AccessDeniedException, DatabaseException, ExhaustedPoolException
    {
        return layerDataFactory.createBCacheRscData(
            layerRscIdPool.autoAllocate(),
            snapRef,
            rscDataRef.getResourceNameSuffix(),
            parentRef
        );
    }

    @Override
    protected BCacheVlmData<Snapshot> createSnapVlmLayerData(
        SnapshotVolume snapVlmRef,
        BCacheRscData<Snapshot> snapDataRef,
        VlmProviderObject<Resource> vlmProviderObjectRef
    ) throws DatabaseException, AccessDeniedException
    {
        return layerDataFactory.createBCacheVlmData(
            snapVlmRef,
            ((BCacheVlmData<Resource>) vlmProviderObjectRef).getCacheStorPool(),
            snapDataRef
        );
    }

    @Override
    protected @Nullable RscDfnLayerObject restoreSnapDfnData(
        SnapshotDefinition snapshotDefinitionRef,
        RscLayerDataApi rscLayerDataApiRef,
        Map<String, String> renameStorPoolMapRef
    ) throws DatabaseException, IllegalArgumentException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        // BCacheLayer does not have resource-definition specific data
        return null;
    }

    @Override
    protected @Nullable VlmDfnLayerObject restoreSnapVlmDfnData(
        SnapshotVolumeDefinition snapshotVolumeDefinitionRef,
        VlmLayerDataApi vlmLayerDataApiRef,
        Map<String, String> renameStorPoolMapRef
    ) throws DatabaseException, AccessDeniedException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        // BCacheLayer does not have volume-definition specific data
        return null;
    }

    @Override
    protected BCacheRscData<Snapshot> restoreSnapDataImpl(
        Snapshot snapRef,
        RscLayerDataApi rscLayerDataApiRef,
        @Nullable AbsRscLayerObject<Snapshot> parentRef,
        Map<String, String> renameStorPoolMapRef
    ) throws DatabaseException, ExhaustedPoolException, ValueOutOfRangeException, AccessDeniedException
    {
        return layerDataFactory.createBCacheRscData(
            layerRscIdPool.autoAllocate(),
            snapRef,
            rscLayerDataApiRef.getRscNameSuffix(),
            parentRef
        );
    }

    @Override
    protected BCacheVlmData<Snapshot> restoreSnapVlmLayerData(
        SnapshotVolume snapVlmRef,
        BCacheRscData<Snapshot> snapDataRef,
        VlmLayerDataApi vlmLayerDataApiRef,
        Map<String, String> renameStorPoolMapRef,
        @Nullable ApiCallRc apiCallRc
    ) throws AccessDeniedException, InvalidNameException, DatabaseException
    {
        StorPool cacheStorPool = AbsLayerHelperUtils.getStorPool(
            apiCtx,
            snapVlmRef,
            snapDataRef,
            ((BCacheVlmPojo) vlmLayerDataApiRef).getCacheStorPoolName(),
            renameStorPoolMapRef,
            apiCallRc
        );
        return layerDataFactory.createBCacheVlmData(
            snapVlmRef,
            cacheStorPool,
            snapDataRef
        );
    }
}
