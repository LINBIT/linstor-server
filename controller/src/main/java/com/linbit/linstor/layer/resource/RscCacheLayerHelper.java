package com.linbit.linstor.layer.resource;

import com.linbit.ExhaustedPoolException;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.controller.CtrlVlmApiCallHandler;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.AbsLayerHelperUtils;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.layer.resource.CtrlRscLayerDataFactory.ChildResourceData;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.adapter.cache.CacheRscData;
import com.linbit.linstor.storage.data.adapter.cache.CacheVlmData;
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Singleton
class RscCacheLayerHelper extends AbsRscLayerHelper<
    CacheRscData<Resource>, CacheVlmData<Resource>,
    RscDfnLayerObject, VlmDfnLayerObject>
{
    private final SystemConfRepository systemConfRepository;

    @Inject
    RscCacheLayerHelper(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        LayerDataFactory layerDataFactoryRef,
        @Named(NumberPoolModule.LAYER_RSC_ID_POOL) DynamicNumberPool layerRscIdPoolRef,
        Provider<CtrlRscLayerDataFactory> rscLayerDataFactory,
        SystemConfRepository systemConfRepositoryRef
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
        systemConfRepository = systemConfRepositoryRef;
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
    protected Set<StorPool> getNeededStoragePools(
        Resource rscRef,
        VolumeDefinition vlmDfn,
        LayerPayload payloadRef,
        List<DeviceLayerKind> layerListRef
    )
        throws AccessDeniedException
    {
        Set<StorPool> storPools = new HashSet<>();
        if (needsCacheDevice(rscRef, layerListRef))
        {
            storPools.add(getCacheStorPool(rscRef, vlmDfn));
            StorPool metaSP = getMetaStorPool(rscRef, vlmDfn);

            if (metaSP != null)
            {
                storPools.add(metaSP);
            }
        }

        return storPools;
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
        if (needsCacheDevice(cacheRscData.getAbsResource(), layerListRef))
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
        children.add(new ChildResourceData(RscLayerSuffixes.SUFFIX_DATA));

        if (needsCacheDevice(rscDataRef.getAbsResource(), layerListRef))
        {
            children.add(
                new ChildResourceData(
                    RscLayerSuffixes.SUFFIX_CACHE_CACHE,
                    null,
                    DeviceLayerKind.STORAGE
                )
            );
            children.add(
                new ChildResourceData(
                    RscLayerSuffixes.SUFFIX_CACHE_META,
                    null,
                    DeviceLayerKind.STORAGE
                )
            );
        }

        return children;
    }

    private boolean needsCacheDevice(
        Resource rscRef,
        List<DeviceLayerKind> layerListRef
    )
        throws AccessDeniedException
    {
        boolean isNvmeBelow = layerListRef.contains(DeviceLayerKind.NVME);
        boolean isNvmeInitiator = rscRef.getStateFlags().isSet(apiCtx, Resource.Flags.NVME_INITIATOR);
        boolean isEbsInitiator = rscRef.getStateFlags().isSet(apiCtx, Resource.Flags.EBS_INITIATOR);
        return !isNvmeBelow || isNvmeInitiator || isEbsInitiator;
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
        if (childRef.getSuffixedResourceName().contains(RscLayerSuffixes.SUFFIX_CACHE_CACHE))
        {
            pool = cacheVlmData.getCacheStorPool();
        }
        else if (childRef.getSuffixedResourceName().contains(RscLayerSuffixes.SUFFIX_CACHE_META))
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
    protected boolean recalculateVolatilePropertiesImpl(
        CacheRscData<Resource> rscDataRef,
        List<DeviceLayerKind> layerListRef,
        LayerPayload payloadRef
    )
    {
        return false; // no change
    }


    @Override
    protected boolean isExpectedToProvideDevice(CacheRscData<Resource> cacheRscData) throws AccessDeniedException
    {
        return !cacheRscData.hasAnyPreventExecutionIgnoreReason();
    }

    private StorPool getSpecialStorPool(
        Volume vlm,
        String propKey,
        String usage,
        boolean throwIfNull
    ) throws InvalidKeyException, AccessDeniedException
    {
        return getSpecialStorPool(
            getPrioProps(vlm),
            vlm.getAbsResource().getNode(),
            propKey,
            usage,
            throwIfNull,
            CtrlVlmApiCallHandler.getVlmDescriptionInline(vlm)
        );
    }

    private StorPool getSpecialStorPool(
        Resource rsc,
        VolumeDefinition vlmDfn,
        String propKey,
        String usage,
        boolean throwIfNull
    )
        throws InvalidKeyException, AccessDeniedException
    {
        return getSpecialStorPool(
            getPrioProps(rsc, vlmDfn),
            rsc.getNode(),
            propKey,
            usage,
            throwIfNull,
            CtrlVlmApiCallHandler.getVlmDescription(rsc, vlmDfn)
        );
    }

    private StorPool getSpecialStorPool(
        PriorityProps prioProps,
        Node node,
        String propKey,
        String usage,
        boolean throwIfNull,
        String vlmDescription
    )
        throws AccessDeniedException
    {
        String poolName = prioProps.getProp(
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
                        " for " + vlmDescription +
                        " in order to use the cache layer."
                )
            );
        }
        StorPool specStorPool = null;
        try
        {
            if (poolName != null)
            {
                specStorPool = node.getStorPool(
                    apiCtx,
                    new StorPoolName(poolName)
                );

                if (specStorPool == null)
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_NOT_FOUND_STOR_POOL,
                            "The " + vlmDescription + " specified '" + poolName +
                                "' as the storage pool for " + usage + ". Node " +
                                node.getName() + " does not have a storage pool" +
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
                    "The " + vlmDescription + " specified '" + poolName +
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

    private StorPool getMetaStorPool(Volume vlm)
        throws InvalidKeyException, AccessDeniedException
    {
        return getSpecialStorPool(
            vlm,
            ApiConsts.KEY_CACHE_META_POOL_NAME,
            "meta data",
            false
        );
    }

    private StorPool getCacheStorPool(Resource rsc, VolumeDefinition vlmDfn)
        throws InvalidKeyException, AccessDeniedException
    {
        return getSpecialStorPool(
            rsc,
            vlmDfn,
            ApiConsts.KEY_CACHE_CACHE_POOL_NAME,
            "cached data",
            true
        );
    }

    private StorPool getMetaStorPool(Resource rsc, VolumeDefinition vlmDfn)
        throws InvalidKeyException, AccessDeniedException
    {
        return getSpecialStorPool(
            rsc,
            vlmDfn,
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
            rsc.getNode().getProps(apiCtx),
            systemConfRepository.getStltConfForView(apiCtx)
        );
        return prioProps;
    }

    private PriorityProps getPrioProps(Resource rsc, VolumeDefinition vlmDfn) throws AccessDeniedException
    {
        ResourceDefinition rscDfn = rsc.getResourceDefinition();
        ResourceGroup rscGrp = rscDfn.getResourceGroup();
        PriorityProps prioProps = new PriorityProps(
            vlmDfn.getProps(apiCtx),
            rscGrp.getVolumeGroupProps(apiCtx, vlmDfn.getVolumeNumber()),
            rsc.getProps(apiCtx),
            rscDfn.getProps(apiCtx),
            rscGrp.getProps(apiCtx),
            rsc.getNode().getProps(apiCtx),
            systemConfRepository.getStltConfForView(apiCtx)
        );
        return prioProps;
    }

    @Override
    protected <RSC extends AbsResource<RSC>> RscDfnLayerObject restoreRscDfnData(
        ResourceDefinition rscDfnRef,
        AbsRscLayerObject<RSC> fromSnapDataRef
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        // CacheLayer does not have resource-definition specific data
        return null;
    }

    @Override
    protected <RSC extends AbsResource<RSC>> CacheRscData<Resource> restoreRscData(
        Resource rscRef,
        AbsRscLayerObject<RSC> fromAbsRscDataRef,
        AbsRscLayerObject<Resource> rscParentRef
    )
        throws DatabaseException, AccessDeniedException, ExhaustedPoolException
    {
        return layerDataFactory.createCacheRscData(
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
    ) throws DatabaseException, AccessDeniedException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        // CacheLayer does not have volume-definition specific data
        return null;
    }

    @Override
    protected <RSC extends AbsResource<RSC>> CacheVlmData<Resource> restoreVlmData(
        Volume vlmRef,
        CacheRscData<Resource> rscDataRef,
        VlmProviderObject<RSC> vlmProviderObjectRef,
        Map<String, String> storpoolRenameMap,
        @Nullable ApiCallRc apiCallRc
    )
        throws DatabaseException, AccessDeniedException, InvalidNameException
    {
        CacheVlmData<RSC> cacheVlmData = (CacheVlmData<RSC>) vlmProviderObjectRef;
        StorPool cachePool = AbsLayerHelperUtils.getStorPool(
            apiCtx,
            vlmRef,
            rscDataRef,
            cacheVlmData.getCacheStorPool(),
            storpoolRenameMap,
            apiCallRc
        );
        StorPool metaPool = AbsLayerHelperUtils.getStorPool(
            apiCtx,
            vlmRef,
            rscDataRef,
            cacheVlmData.getMetaStorPool(),
            storpoolRenameMap,
            apiCallRc
        );

        return layerDataFactory.createCacheVlmData(
            vlmRef,
            cachePool,
            metaPool,
            rscDataRef
        );
    }
}
