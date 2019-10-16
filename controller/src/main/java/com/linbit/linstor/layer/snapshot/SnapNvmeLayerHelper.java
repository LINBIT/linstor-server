package com.linbit.linstor.layer.snapshot;

import com.linbit.ExhaustedPoolException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeRscData;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeVlmData;
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
class SnapNvmeLayerHelper extends AbsSnapLayerHelper<
    NvmeRscData<Snapshot>, NvmeVlmData<Snapshot>,
    RscDfnLayerObject, VlmDfnLayerObject
>
{
    @Inject
    SnapNvmeLayerHelper(
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
            DeviceLayerKind.NVME
        );
    }

    @Override
    protected RscDfnLayerObject createSnapDfnData(SnapshotDefinition rscDfnRef, String rscNameSuffixRef)
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        // NvmeLayer does not have resource-definition specific data (nothing to snapshot)
        return null;
    }

    @Override
    protected VlmDfnLayerObject createSnapVlmDfnData(SnapshotVolumeDefinition snapVlmDfnRef, String rscNameSuffixRef)
        throws DatabaseException, AccessDeniedException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        // NvmeLayer does not have resource-definition specific data (nothing to snapshot)
        return null;
    }

    @Override
    protected NvmeRscData<Snapshot> createSnapData(
        Snapshot snapRef,
        AbsRscLayerObject<Resource> rscDataRef,
        AbsRscLayerObject<Snapshot> parentRef
    )
        throws AccessDeniedException, DatabaseException, ExhaustedPoolException
    {
        // nothing to copy
        return layerDataFactory.createNvmeRscData(
            layerRscIdPool.autoAllocate(),
            snapRef,
            rscDataRef.getResourceNameSuffix(),
            parentRef
        );
    }

    @Override
    protected NvmeVlmData<Snapshot> createSnapVlmLayerData(
        SnapshotVolume snapVlmRef,
        NvmeRscData<Snapshot> snapDataRef,
        VlmProviderObject<Resource> vlmProviderObjectRef
    )
        throws DatabaseException, AccessDeniedException
    {
        // nothing to copy
        return layerDataFactory.createNvmeVlmData(snapVlmRef, snapDataRef);
    }
}
