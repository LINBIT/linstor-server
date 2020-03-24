package com.linbit.linstor.layer.resource;

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
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.layer.resource.CtrlRscLayerDataFactory.ChildResourceData;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.InvalidKeyException;
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

import static com.linbit.linstor.core.apicallhandler.controller.CtrlVlmListApiCallHandler.getVlmDescriptionInline;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.List;

@Singleton
class RscCacheLayerHelper extends AbsRscLayerHelper<
    CacheRscData<Resource>, CacheVlmData<Resource>,
    RscDfnLayerObject, VlmDfnLayerObject>
{
    @Inject
    RscCacheLayerHelper(
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
            // CacheRscData.class cannot directly be casted to Class<CacheRscData<Resource>>. because java.
            // its type is Class<CacheRscData> (without nested types), but that is not enough as the
            // super constructor wants a Class<RSC_PO>, where RSC_PO is CacheRscData<Resource>.
            (Class<CacheRscData<Resource>>) ((Object) CacheRscData.class),
            DeviceLayerKind.CACHE,
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
        // CacheLayer does not have resource-definition specific data
        return null;
    }

    @Override
    protected void mergeRscDfnData(RscDfnLayerObject rscDfnRef, LayerPayload payloadRef)
    {
        // no Cache specific resource-definition, nothing to merge
    }

    @Override
    protected VlmDfnLayerObject createVlmDfnData(
        VolumeDefinition vlmDfnRef,
        String rscNameSuffixRef,
        LayerPayload payloadRef
    )
    {
        // CacheLayer does not have volume-definition specific data
        return null;
    }

    @Override
    protected void mergeVlmDfnData(VlmDfnLayerObject vlmDfnDataRef, LayerPayload payloadRef)
    {
        // no Cache specific volume-definition, nothing to merge
    }

    @Override
    protected CacheRscData<Resource> createRscData(
        Resource rscRef,
        LayerPayload payloadRef,
        String rscNameSuffixRef,
        AbsRscLayerObject<Resource> parentObjectRef,
        List<DeviceLayerKind> layerListRef
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
            ValueInUseException
    {
        return layerDataFactory.createCacheRscData(
            layerRscIdPool.autoAllocate(),
            rscRef,
            rscNameSuffixRef,
            parentObjectRef
        );
    }

    @Override
    protected void mergeRscData(CacheRscData<Resource> rscDataRef, LayerPayload payloadRef)
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
    protected CacheVlmData<Resource> createVlmLayerData(
        CacheRscData<Resource> cacheRscData,
        Volume vlm,
        LayerPayload payload,
        List<DeviceLayerKind> layerListRef
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
            ValueInUseException, LinStorException
    {
        StorPool cacheStorPool = null;
        StorPool metaStorPool = null;
        if (needsCacheDevice(cacheRscData, layerListRef))
        {
            cacheStorPool = getCacheStorPool(vlm);
            metaStorPool = getMetaStorPool(vlm);

            if (metaStorPool == null)
            {
                metaStorPool = cacheStorPool; // fallback
            }
        }
        return layerDataFactory.createCacheVlmData(
            vlm,
            cacheStorPool,
            metaStorPool,
            cacheRscData
        );
    }

    @Override
    protected void mergeVlmData(
        CacheVlmData<Resource> vlmDataRef,
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
        CacheRscData<Resource> rscDataRef,
        List<DeviceLayerKind> layerListRef
    )
        throws AccessDeniedException, InvalidKeyException
    {
        // always return data and cache child
        List<ChildResourceData> children = new ArrayList<>();
        children.add(new ChildResourceData(CacheRscData.SUFFIX_DATA));

        if (needsCacheDevice(rscDataRef, layerListRef))
        {
            children.add(new ChildResourceData(CacheRscData.SUFFIX_CACHE, DeviceLayerKind.STORAGE));
            children.add(new ChildResourceData(CacheRscData.SUFFIX_META, DeviceLayerKind.STORAGE));
        }

        return children;
    }

    private boolean needsCacheDevice(
        CacheRscData<Resource> rscDataRef,
        List<DeviceLayerKind> layerListRef
    )
        throws AccessDeniedException
    {
        boolean isNvmeBelow = layerListRef.contains(DeviceLayerKind.NVME);
        boolean isOpenflexBelow = layerListRef.contains(DeviceLayerKind.OPENFLEX);
        boolean isNvmeInitiator = rscDataRef.getAbsResource().getStateFlags()
            .isSet(apiCtx, Resource.Flags.NVME_INITIATOR);
        boolean needsCacheDevice = !(isNvmeBelow || isOpenflexBelow) || isNvmeInitiator;
        return needsCacheDevice;
    }

    @Override
    public StorPool getStorPool(Volume vlmRef, AbsRscLayerObject<Resource> childRef)
        throws AccessDeniedException, InvalidKeyException, InvalidNameException
    {
        StorPool pool;
        CacheVlmData<Resource> cacheVlmData = (CacheVlmData<Resource>) childRef
            .getParent()
            .getVlmProviderObject(
                vlmRef.getVolumeDefinition().getVolumeNumber()
            );
        if (childRef.getSuffixedResourceName().contains(CacheRscData.SUFFIX_CACHE))
        {
            pool = cacheVlmData.getCacheStorPool();
        }
        else if (childRef.getSuffixedResourceName().contains(CacheRscData.SUFFIX_META))
        {
            pool = cacheVlmData.getMetaStorPool();
        }
        else
        {
            pool = cacheVlmData.getStorPool();
        }
        return pool;
    }

    @Override
    protected void resetStoragePools(AbsRscLayerObject<Resource> rscDataRef)
        throws AccessDeniedException, DatabaseException
    {
        // no-op
    }

    @Override
    protected boolean isExpectedToProvideDevice(
        CacheRscData<Resource> cacheRscData
    )
        throws AccessDeniedException
    {
        return true;
    }

    private StorPool getSpecialStorPool(
        Volume vlm,
        String propKey,
        String usage,
        boolean throwIfNull
    ) throws InvalidKeyException, AccessDeniedException
    {
        String poolName = getPrioProps(vlm).getProp(
            propKey, ApiConsts.NAMESPC_CACHE
        );
        if (poolName == null && throwIfNull)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_NOT_FOUND_STOR_POOL,
                    "You have to set the property " +
                        ApiConsts.NAMESPC_CACHE + "/" +
                        propKey +
                        " in order to use the cache layer."
                )
            );
        }
        StorPool specStorPool = null;
        try
        {
            if (poolName != null)
            {
                specStorPool = vlm.getAbsResource().getNode().getStorPool(
                    apiCtx,
                    new StorPoolName(poolName)
                );

                if (specStorPool == null)
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_NOT_FOUND_STOR_POOL,
                            "The " + getVlmDescriptionInline(vlm) + " specified '" + poolName +
                                "' as the storage pool for " + usage + ". Node " +
                                vlm.getAbsResource().getNode().getName() + " does not have a storage pool" +
                                " with that name"
                        )
                    );
                }
            }
        }
        catch (InvalidNameException exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_STOR_POOL_NAME,
                    "The " + getVlmDescriptionInline(vlm) + " specified '" + poolName +
                        "' as the storage pool for " + usage + ". That name is invalid."
                ),
                exc
            );
        }
        return specStorPool;
    }

    private StorPool getCacheStorPool(Volume vlm) throws InvalidKeyException, AccessDeniedException
    {
        return getSpecialStorPool(
            vlm,
            ApiConsts.KEY_CACHE_CACHE_POOL_NAME,
            "cached data",
            true
        );
    }

    private StorPool getMetaStorPool(Volume vlm) throws InvalidKeyException, AccessDeniedException
    {
        return getSpecialStorPool(
            vlm,
            ApiConsts.KEY_CACHE_META_POOL_NAME,
            "meta data",
            false
        );
    }

    private PriorityProps getPrioProps(Volume vlmRef) throws AccessDeniedException
    {
        VolumeDefinition vlmDfn = vlmRef.getVolumeDefinition();
        ResourceDefinition rscDfn = vlmRef.getResourceDefinition();
        ResourceGroup rscGrp = rscDfn.getResourceGroup();
        Resource rsc = vlmRef.getAbsResource();
        PriorityProps prioProps = new PriorityProps(
            vlmDfn.getProps(apiCtx),
            rscGrp.getVolumeGroupProps(apiCtx, vlmDfn.getVolumeNumber()),
            rsc.getProps(apiCtx),
            rscDfn.getProps(apiCtx),
            rscGrp.getProps(apiCtx),
            rsc.getNode().getProps(apiCtx)
        );
        return prioProps;
    }

    @Override
    protected RscDfnLayerObject restoreRscDfnData(
        ResourceDefinition rscDfnRef,
        AbsRscLayerObject<Snapshot> fromSnapDataRef
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        // CacheLayer does not have resource-definition specific data
        return null;
    }

    @Override
    protected CacheRscData<Resource> restoreRscData(
        Resource rscRef,
        AbsRscLayerObject<Snapshot> fromSnapDataRef,
        AbsRscLayerObject<Resource> rscParentRef
    )
        throws DatabaseException, AccessDeniedException, ExhaustedPoolException
    {
        return layerDataFactory.createCacheRscData(
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
    ) throws DatabaseException, AccessDeniedException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        // CacheLayer does not have volume-definition specific data
        return null;
    }

    @Override
    protected CacheVlmData<Resource> restoreVlmData(
        Volume vlmRef,
        CacheRscData<Resource> rscDataRef,
        VlmProviderObject<Snapshot> vlmProviderObjectRef
    )
        throws DatabaseException, AccessDeniedException
    {
        return layerDataFactory.createCacheVlmData(
            vlmRef,
            ((CacheVlmData<Snapshot>) vlmProviderObjectRef).getCacheStorPool(),
            ((CacheVlmData<Snapshot>) vlmProviderObjectRef).getMetaStorPool(),
            rscDataRef
        );
    }
}
