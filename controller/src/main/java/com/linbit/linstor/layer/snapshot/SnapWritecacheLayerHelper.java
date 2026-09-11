package com.linbit.linstor.layer.snapshot;

import com.linbit.ExhaustedPoolException;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.WritecacheRscPojo.WritecacheVlmPojo;
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
import com.linbit.linstor.storage.data.adapter.writecache.WritecacheRscData;
import com.linbit.linstor.storage.data.adapter.writecache.WritecacheVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerDataFactory;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

import java.util.Map;

@Singleton
class SnapWritecacheLayerHelper
    extends AbsSnapLayerHelper<
        WritecacheRscData<Snapshot>, WritecacheVlmData<Snapshot>,
        RscDfnLayerObject, VlmDfnLayerObject>
{
    @Inject
    SnapWritecacheLayerHelper(
        ErrorReporter errorReporterRef,
        LayerDataFactory layerDataFactoryRef,
        @Named(NumberPoolModule.LAYER_RSC_ID_POOL) DynamicNumberPool layerRscIdPoolRef
    )
    {
        super(
            errorReporterRef,
            layerDataFactoryRef,
            layerRscIdPoolRef,
            DeviceLayerKind.WRITECACHE
        );
    }

    @Override
    protected @Nullable RscDfnLayerObject createSnapDfnData(
        SnapshotDefinition rscDfnRef,
        String rscNameSuffixRef
    )
    {
        // WritecacheLayer does not have resource-definition specific data
        return null;
    }

    @Override
    protected @Nullable VlmDfnLayerObject createSnapVlmDfnData(
        SnapshotVolumeDefinition snapVlmDfnRef,
        String rscNameSuffixRef
    )
    {
        // WritecacheLayer does not have volume-definition specific data
        return null;
    }

    @Override
    protected WritecacheRscData<Snapshot> createSnapData(
        Snapshot snapRef,
        AbsRscLayerObject<Resource> rscDataRef,
        AbsRscLayerObject<Snapshot> parentRef
    ) throws DatabaseException, ExhaustedPoolException
    {
        return layerDataFactory.createWritecacheRscData(
            layerRscIdPool.autoAllocate(),
            snapRef,
            rscDataRef.getResourceNameSuffix(),
            parentRef
        );
    }

    @Override
    protected WritecacheVlmData<Snapshot> createSnapVlmLayerData(
        SnapshotVolume snapVlmRef,
        WritecacheRscData<Snapshot> snapDataRef,
        VlmProviderObject<Resource> vlmProviderObjectRef
    ) throws DatabaseException
    {
        return layerDataFactory.createWritecacheVlmData(
            snapVlmRef,
            ((WritecacheVlmData<Resource>) vlmProviderObjectRef).getCacheStorPool(),
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
        // WritecacheLayer does not have resource-definition specific data
        return null;
    }

    @Override
    protected @Nullable VlmDfnLayerObject restoreSnapVlmDfnData(
        SnapshotVolumeDefinition snapshotVolumeDefinitionRef,
        VlmLayerDataApi vlmLayerDataApiRef,
        Map<String, String> renameStorPoolMapRef
    ) throws DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        // WritecacheLayer does not have volume-definition specific data
        return null;
    }

    @Override
    protected WritecacheRscData<Snapshot> restoreSnapDataImpl(
        Snapshot snapRef,
        RscLayerDataApi rscLayerDataApiRef,
        @Nullable AbsRscLayerObject<Snapshot> parentRef,
        Map<String, String> renameStorPoolMapRef
    ) throws DatabaseException, ExhaustedPoolException, ValueOutOfRangeException
    {
        return layerDataFactory.createWritecacheRscData(
            layerRscIdPool.autoAllocate(),
            snapRef,
            rscLayerDataApiRef.getRscNameSuffix(),
            parentRef
        );
    }

    @Override
    protected WritecacheVlmData<Snapshot> restoreSnapVlmLayerData(
        SnapshotVolume snapVlmRef,
        WritecacheRscData<Snapshot> snapDataRef,
        VlmLayerDataApi vlmLayerDataApiRef,
        Map<String, String> renameStorPoolMapRef,
        @Nullable ApiCallRc apiCallRc
    ) throws InvalidNameException, DatabaseException
    {
        StorPool cacheStorPool = AbsLayerHelperUtils.getStorPool(
            snapVlmRef,
            snapDataRef,
            ((WritecacheVlmPojo) vlmLayerDataApiRef).getCacheStorPoolName(),
            renameStorPoolMapRef,
            apiCallRc
        );
        return layerDataFactory.createWritecacheVlmData(
            snapVlmRef,
            cacheStorPool,
            snapDataRef
        );
    }
}
