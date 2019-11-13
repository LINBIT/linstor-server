package com.linbit.linstor.layer;

import com.linbit.ExhaustedPoolException;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.CtrlLayerDataHelper.ChildResourceData;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.adapter.writecache.WritecacheRscData;
import com.linbit.linstor.storage.data.adapter.writecache.WritecacheVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerDataFactory;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlVlmListApiCallHandler.getVlmDescriptionInline;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.List;

@Singleton
class WritecacheLayerHelper
    extends AbsLayerHelper<WritecacheRscData, WritecacheVlmData, RscDfnLayerObject, VlmDfnLayerObject>
{
    @Inject
    WritecacheLayerHelper(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        LayerDataFactory layerDataFactoryRef,
        @Named(NumberPoolModule.LAYER_RSC_ID_POOL) DynamicNumberPool layerRscIdPoolRef,
        Provider<CtrlLayerDataHelper> layerHelperProviderRef
    )
    {
        super(
            errorReporterRef,
            apiCtxRef,
            layerDataFactoryRef,
            layerRscIdPoolRef,
            WritecacheRscData.class,
            DeviceLayerKind.NVME,
            layerHelperProviderRef
        );
    }

    @Override
    protected RscDfnLayerObject createRscDfnData(
        ResourceDefinition rscDfnRef,
        String rscNameSuffixRef,
        LayerPayload payloadRef
    )
    {
        // WritecacheLayer does not have resource-definition specific data
        return null;
    }

    @Override
    protected void mergeRscDfnData(RscDfnLayerObject rscDfnRef, LayerPayload payloadRef)
    {
        // no Writecache specific resource-definition, nothing to merge
    }

    @Override
    protected VlmDfnLayerObject createVlmDfnData(
        VolumeDefinition vlmDfnRef,
        String rscNameSuffixRef,
        LayerPayload payloadRef
    )
    {
        // WritecacheLayer does not have volume-definition specific data
        return null;
    }

    @Override
    protected void mergeVlmDfnData(VlmDfnLayerObject vlmDfnDataRef, LayerPayload payloadRef)
    {
        // no Writecache specific volume-definition, nothing to merge
    }

    @Override
    protected WritecacheRscData createRscData(
        Resource rscRef,
        LayerPayload payloadRef,
        String rscNameSuffixRef,
        RscLayerObject parentObjectRef,
        List<DeviceLayerKind> layerListRef
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
            ValueInUseException
    {
        return layerDataFactory.createWritecacheRscData(
            layerRscIdPool.autoAllocate(),
            rscRef,
            rscNameSuffixRef,
            parentObjectRef
        );
    }

    @Override
    protected void mergeRscData(WritecacheRscData rscDataRef, LayerPayload payloadRef)
    {
        // nothing to merge
    }

    @Override
    protected boolean needsChildVlm(RscLayerObject childRscDataRef, Volume vlmRef)
        throws AccessDeniedException, InvalidKeyException
    {
        return true;
    }

    @Override
    protected WritecacheVlmData createVlmLayerData(
        WritecacheRscData writecacheRscData,
        Volume vlm,
        LayerPayload payload,
        List<DeviceLayerKind> layerListRef
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
            ValueInUseException, LinStorException
    {

        return layerDataFactory.createWritecacheVlmData(vlm, getCacheStorPool(vlm), writecacheRscData);
    }

    @Override
    protected void mergeVlmData(
        WritecacheVlmData vlmDataRef,
        Volume vlmRef,
        LayerPayload payloadRef,
        List<DeviceLayerKind> layerListRef
    )
        throws AccessDeniedException, InvalidKeyException
    {
        // nothing to do
    }

    @Override
    protected List<ChildResourceData> getChildRsc(
        WritecacheRscData rscDataRef,
        List<DeviceLayerKind> layerListRef
    )
        throws AccessDeniedException, InvalidKeyException
    {
        // always return data and cache child
        List<ChildResourceData> children = new ArrayList<>();
        children.add(new ChildResourceData(WritecacheRscData.SUFFIX_DATA));

        boolean isNvmeBelow = layerListRef.contains(DeviceLayerKind.NVME);
        boolean isNvmeInitiator = rscDataRef.getResource().getStateFlags().isSet(apiCtx, Resource.Flags.NVME_INITIATOR);
        if (!isNvmeBelow || isNvmeInitiator)
        {
            children.add(new ChildResourceData(WritecacheRscData.SUFFIX_CACHE, DeviceLayerKind.STORAGE));
        }

        return children;
    }

    @Override
    protected StorPool getStorPool(Volume vlmRef, RscLayerObject childRef)
        throws AccessDeniedException, InvalidKeyException, InvalidNameException
    {
        StorPool pool;
        WritecacheVlmData writecacheVlmData = (WritecacheVlmData) childRef.getParent().getVlmProviderObject(
            vlmRef.getVolumeDefinition().getVolumeNumber()
        );
        if (childRef.getSuffixedResourceName().contains(WritecacheRscData.SUFFIX_CACHE))
        {
            pool = writecacheVlmData.getCacheStorPool();
        }
        else
        {
            pool = writecacheVlmData.getStorPool();
        }
        return pool;
    }

    @Override
    protected void resetStoragePools(RscLayerObject rscDataRef) throws AccessDeniedException, DatabaseException
    {
        // no-op
    }

    @Override
    protected boolean isExpectedToProvideDevice(WritecacheRscData writecacheRscData) throws AccessDeniedException
    {
        return true;
    }

    private StorPool getCacheStorPool(Volume vlm) throws InvalidKeyException, AccessDeniedException
    {
        String cacheStorPoolNameStr = getCacheStorPoolName(vlm);
        if (cacheStorPoolNameStr == null)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_NOT_FOUND_STOR_POOL,
                    "You have to set the property " +
                    ApiConsts.NAMESPC_WRITECACHE + "/" + ApiConsts.KEY_WRITECACHE_POOL_NAME +
                    " in order to use the writecache layer."
                )
            );
        }
        StorPool cacheStorPool = null;
        try
        {
            cacheStorPool = vlm.getResource().getAssignedNode().getStorPool(
                apiCtx,
                new StorPoolName(cacheStorPoolNameStr)
            );

            if (cacheStorPool == null)
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_NOT_FOUND_STOR_POOL,
                        "The " + getVlmDescriptionInline(vlm) + " specified '" + cacheStorPoolNameStr +
                            "' as the storage pool for external meta-data. Node " +
                            vlm.getResource().getAssignedNode().getName() + " does not have a storage pool" +
                            " with that name"
                    )
                );
            }

        }
        catch (InvalidNameException exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_STOR_POOL_NAME,
                    "The " + getVlmDescriptionInline(vlm) + " specified '" + cacheStorPoolNameStr +
                        "' as the storage pool for external meta-data. That name is invalid."
                ),
                exc
            );
        }
        return cacheStorPool;
    }

    private String getCacheStorPoolName(Volume vlmRef) throws InvalidKeyException, AccessDeniedException
    {
        return getPrioProps(vlmRef).getProp(
            ApiConsts.KEY_WRITECACHE_POOL_NAME, ApiConsts.NAMESPC_WRITECACHE
        );
    }

    private String getCacheSize(Volume vlmRef) throws InvalidKeyException, AccessDeniedException
    {
        return getPrioProps(vlmRef).getProp(
            ApiConsts.KEY_WRITECACHE_SIZE, ApiConsts.NAMESPC_WRITECACHE
        );
    }

    private PriorityProps getPrioProps(Volume vlmRef) throws AccessDeniedException
    {
        VolumeDefinition vlmDfn = vlmRef.getVolumeDefinition();
        ResourceDefinition rscDfn = vlmRef.getResourceDefinition();
        ResourceGroup rscGrp = rscDfn.getResourceGroup();
        Resource rsc = vlmRef.getResource();
        PriorityProps prioProps = new PriorityProps(
            vlmDfn.getProps(apiCtx),
            rscGrp.getVolumeGroupProps(apiCtx, vlmDfn.getVolumeNumber()),
            rsc.getProps(apiCtx),
            rscDfn.getProps(apiCtx),
            rscGrp.getProps(apiCtx),
            rsc.getAssignedNode().getProps(apiCtx)
        );
        return prioProps;
    }
}
