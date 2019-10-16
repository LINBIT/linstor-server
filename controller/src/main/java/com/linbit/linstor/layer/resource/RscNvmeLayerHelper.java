package com.linbit.linstor.layer.resource;

import com.linbit.ExhaustedPoolException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
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
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.List;

@Singleton
class RscNvmeLayerHelper extends AbsRscLayerHelper<
    NvmeRscData<Resource>, NvmeVlmData<Resource>,
    RscDfnLayerObject, VlmDfnLayerObject
>
{
    @Inject
    RscNvmeLayerHelper(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        LayerDataFactory layerDataFactoryRef,
        @Named(NumberPoolModule.LAYER_RSC_ID_POOL) DynamicNumberPool layerRscIdPoolRef,
        Provider<CtrlRscLayerDataFactory> rscLayerDataFactory
    )
    {
        super(
            errorReporterRef,
            apiCtxRef,
            layerDataFactoryRef,
            layerRscIdPoolRef,
            // NvmeRscData.class cannot directly be casted to Class<NvmeRscData<Resource>>. because java.
            // its type is Class<NvmeRscData> (without nested types), but that is not enough as the super constructor
            // wants a Class<RSC_PO>, where RSC_PO is NvmeRscData<Resource>.
            (Class<NvmeRscData<Resource>>) ((Object) NvmeRscData.class),
            DeviceLayerKind.NVME,
            rscLayerDataFactory
        );
    }

    @Override
    protected RscDfnLayerObject createRscDfnData(
        ResourceDefinition rscDfnRef,
        String rscNameSuffixRef,
        LayerPayload payloadRef
    )
    {
        // NvmeLayer does not have resource-definition specific data
        return null;
    }

    @Override
    protected void mergeRscDfnData(RscDfnLayerObject rscDfnRef, LayerPayload payloadRef)
    {
        // no Nvme specific resource-definition, nothing to merge
    }

    @Override
    protected VlmDfnLayerObject createVlmDfnData(
        VolumeDefinition vlmDfnRef,
        String rscNameSuffixRef,
        LayerPayload payloadRef
    )
    {
        // NvmeLayer does not have volume-definition specific data
        return null;
    }

    @Override
    protected void mergeVlmDfnData(VlmDfnLayerObject vlmDfnDataRef, LayerPayload payloadRef)
    {
        // no Nvme specific volume-definition, nothing to merge
    }

    @Override
    protected NvmeRscData<Resource> createRscData(
        Resource rscRef,
        LayerPayload payloadRef,
        String rscNameSuffixRef,
        AbsRscLayerObject<Resource> parentObjectRef,
        List<DeviceLayerKind> layerListRef
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
            ValueInUseException
    {
        return layerDataFactory.createNvmeRscData(
            layerRscIdPool.autoAllocate(),
            rscRef,
            rscNameSuffixRef,
            parentObjectRef
        );
    }

    @Override
    protected void mergeRscData(NvmeRscData<Resource> rscDataRef, LayerPayload payloadRef)
    {
        // nothing to merge
    }

    @Override
    protected boolean needsChildVlm(AbsRscLayerObject<Resource> childRscDataRef, Volume vlmRef)
        throws AccessDeniedException, InvalidKeyException
    {
        return true;
    }

    @Override
    protected NvmeVlmData<Resource> createVlmLayerData(
        NvmeRscData<Resource> nvmeRscData,
        Volume vlm,
        LayerPayload payload,
        List<DeviceLayerKind> layerListRef
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
            ValueInUseException, LinStorException
    {
        return layerDataFactory.createNvmeVlmData(vlm, nvmeRscData);
    }

    @Override
    protected void mergeVlmData(
        NvmeVlmData<Resource> vlmDataRef,
        Volume vlmRef,
        LayerPayload payloadRef,
        List<DeviceLayerKind> layerListRef
    )
    {
        // nothing to do
    }

    @Override
    protected void resetStoragePools(AbsRscLayerObject<Resource> rscDataRef)
    {
        // nothing to do
    }

    @Override
    protected boolean isExpectedToProvideDevice(NvmeRscData<Resource> nvmeRscData) throws AccessDeniedException
    {
        return nvmeRscData.getAbsResource().getStateFlags().isSet(apiCtx, Resource.Flags.NVME_INITIATOR);
    }

    @Override
    protected RscDfnLayerObject restoreRscDfnData(
        ResourceDefinition rscDfnRef,
        AbsRscLayerObject<Snapshot> fromSnapDataRef
    )
    {
        // NvmeLayer does not have resource-definition specific data
        return null;
    }

    @Override
    protected NvmeRscData<Resource> restoreRscData(
        Resource rscRef,
        AbsRscLayerObject<Snapshot> fromSnapDataRef,
        AbsRscLayerObject<Resource> rscParentRef
    )
        throws DatabaseException, ExhaustedPoolException
    {
       return layerDataFactory.createNvmeRscData(
            layerRscIdPool.autoAllocate(),
            rscRef,
            fromSnapDataRef.getResourceNameSuffix(),
            rscParentRef
       );
    }

    @Override
    protected VlmDfnLayerObject restoreVlmDfnData(
        VolumeDefinition vlmDfnRef,
        VlmProviderObject<Snapshot> fromSnapVlmDataRef
    )
    {
        // NvmeLayer does not have volume-definition specific data
        return null;
    }

    @Override
    protected NvmeVlmData<Resource> restoreVlmData(
        Volume vlmRef,
        NvmeRscData<Resource> rscDataRef,
        VlmProviderObject<Snapshot> vlmProviderObjectRef
    )
        throws DatabaseException, AccessDeniedException
    {
        return layerDataFactory.createNvmeVlmData(vlmRef, rscDataRef);
    }
}
