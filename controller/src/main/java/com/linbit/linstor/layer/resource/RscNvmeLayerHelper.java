package com.linbit.linstor.layer.resource;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.core.apicallhandler.response.ApiException;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.LayerIgnoreReason;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.layer.resource.CtrlRscLayerDataFactory.ChildResourceData;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeRscData;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerDataFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

@Singleton
class RscNvmeLayerHelper
    extends AbsRscLayerHelper<
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
        ensureTargetNodeNameIsSet(rscRef, apiCtx);
        return layerDataFactory.createNvmeRscData(
            layerRscIdPool.autoAllocate(),
            rscRef,
            rscNameSuffixRef,
            parentObjectRef
        );
    }

    Resource getTarget(Resource initiator)
        throws AccessDeniedException, DatabaseException, ImplementationError, InvalidNameException
    {
        ensureTargetNodeNameIsSet(initiator, apiCtx);
        String nodeNameStr = initiator.getProps(apiCtx).getProp(InternalApiConsts.PROP_NVME_TARGET_NODE_NAME);
        return initiator.getResourceDefinition().getResource(apiCtx, new NodeName(nodeNameStr));
    }

    // also called by RscOpenflexLayerHelper
    static void ensureTargetNodeNameIsSet(Resource rscRef, AccessContext apiCtx)
        throws AccessDeniedException, DatabaseException, ImplementationError
    {
        Props rscProps = rscRef.getProps(apiCtx);
        if (rscProps.getProp(InternalApiConsts.PROP_NVME_TARGET_NODE_NAME) == null &&
            rscRef.isNvmeInitiator(apiCtx))
        {
            ResourceDefinition rscDfn = rscRef.getResourceDefinition();
            Iterator<Resource> rscIt = rscDfn.iterateResource(apiCtx);

            HashMap<String, Integer> initCountPerTarget = new HashMap<>();

            while (rscIt.hasNext())
            {
                Resource otherRsc = rscIt.next();
                if (!otherRsc.equals(rscRef))
                {
                    if (otherRsc.isNvmeInitiator(apiCtx))
                    {
                        String othersTarget = otherRsc.getProps(apiCtx)
                            .getProp(InternalApiConsts.PROP_NVME_TARGET_NODE_NAME);
                        Integer count = initCountPerTarget.get(othersTarget);
                        if (count == null)
                        {
                            initCountPerTarget.put(othersTarget, 1);
                        }
                        else
                        {
                            initCountPerTarget.put(othersTarget, count + 1);
                        }
                    }
                    else
                    {
                        if (!initCountPerTarget.containsKey(otherRsc.getNode().getName().displayValue))
                        {
                            initCountPerTarget.put(otherRsc.getNode().getName().displayValue, 0);
                        }
                    }
                }
            }

            int lowestCount = Integer.MAX_VALUE;
            String targetWithLowestInitCount = null;
            for (Entry<String, Integer> entry : initCountPerTarget.entrySet())
            {
                if (lowestCount > entry.getValue())
                {
                    lowestCount = entry.getValue();
                    targetWithLowestInitCount = entry.getKey();
                }
            }

            if (targetWithLowestInitCount == null)
            {
                throw new ApiException("No available nvme target found");
            }

            try
            {
                rscProps.setProp(
                    InternalApiConsts.PROP_NVME_TARGET_NODE_NAME,
                    targetWithLowestInitCount
                );
            }
            catch (InvalidKeyException | InvalidValueException exc)
            {
                throw new ImplementationError(exc);
            }
        }
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
    protected Set<StorPool> getNeededStoragePools(
        Resource rsc,
        VolumeDefinition vlmDfn,
        LayerPayload payloadRef,
        List<DeviceLayerKind> layerListRef
    )
        throws AccessDeniedException, InvalidNameException
    {
        // no special StorPool - only the one from "below" us, and that will be found when asking the next layer
        return Collections.emptySet();
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
    protected List<ChildResourceData> getChildRsc(NvmeRscData<Resource> rscDataRef, List<DeviceLayerKind> layerListRef)
        throws AccessDeniedException, InvalidKeyException
    {
        return Arrays.asList(new ChildResourceData(RscLayerSuffixes.SUFFIX_DATA));
    }

    @Override
    protected void resetStoragePools(AbsRscLayerObject<Resource> rscDataRef)
    {
        // nothing to do
    }

    @Override
    protected boolean recalculateVolatilePropertiesImpl(
        NvmeRscData<Resource> rscDataRef,
        List<DeviceLayerKind> layerListRef,
        LayerPayload payloadRef
    )
        throws AccessDeniedException, DatabaseException
    {
        boolean changed = false;
        if (rscDataRef.getAbsResource().isNvmeInitiator(apiCtx))
        {
            // we are initiator, ignore everything below us
            changed = addIgnoreReason(rscDataRef, LayerIgnoreReason.NVME_INITIATOR, false, true, true);
        }
        else
        {
            // we are target, so tell all of our ancestors to ignoreNonDataPaths
            changed = addIgnoreReason(rscDataRef, LayerIgnoreReason.NVME_TARGET, true, false, true);
        }
        return changed;
    }

    @Override
    protected boolean isExpectedToProvideDevice(NvmeRscData<Resource> nvmeRscData) throws AccessDeniedException
    {
        return nvmeRscData.getAbsResource().isNvmeInitiator(apiCtx) &&
            !nvmeRscData.hasAnyPreventExecutionIgnoreReason();
    }

    @Override
    protected <RSC extends AbsResource<RSC>> RscDfnLayerObject restoreRscDfnData(
        ResourceDefinition rscDfnRef,
        AbsRscLayerObject<RSC> fromSnapDataRef
    )
    {
        // NvmeLayer does not have resource-definition specific data
        return null;
    }

    @Override
    protected <RSC extends AbsResource<RSC>> NvmeRscData<Resource> restoreRscData(
        Resource rscRef,
        AbsRscLayerObject<RSC> fromAbsRscDataRef,
        AbsRscLayerObject<Resource> rscParentRef
    )
        throws DatabaseException, ExhaustedPoolException
    {
       return layerDataFactory.createNvmeRscData(
            layerRscIdPool.autoAllocate(),
            rscRef,
            fromAbsRscDataRef.getResourceNameSuffix(),
           rscParentRef
       );
    }

    @Override
    protected <RSC extends AbsResource<RSC>> VlmDfnLayerObject restoreVlmDfnData(
        VolumeDefinition vlmDfnRef,
        VlmProviderObject<RSC> fromSnapVlmDataRef
    )
    {
        // NvmeLayer does not have volume-definition specific data
        return null;
    }

    @Override
    protected <RSC extends AbsResource<RSC>> NvmeVlmData<Resource> restoreVlmData(
        Volume vlmRef,
        NvmeRscData<Resource> rscDataRef,
        VlmProviderObject<RSC> vlmProviderObjectRef,
        Map<String, String> storpoolRenameMap,
        @Nullable ApiCallRc apiCallRc
    )
        throws DatabaseException, AccessDeniedException
    {
        return layerDataFactory.createNvmeVlmData(vlmRef, rscDataRef);
    }
}
