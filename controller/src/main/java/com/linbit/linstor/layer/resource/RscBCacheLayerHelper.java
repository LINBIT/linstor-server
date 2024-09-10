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
import com.linbit.linstor.storage.data.adapter.bcache.BCacheRscData;
import com.linbit.linstor.storage.data.adapter.bcache.BCacheVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerDataFactory;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlVlmListApiCallHandler.getVlmDescriptionInline;

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
class RscBCacheLayerHelper
    extends AbsRscLayerHelper<
        BCacheRscData<Resource>, BCacheVlmData<Resource>,
        RscDfnLayerObject, VlmDfnLayerObject>
{
    private final SystemConfRepository systemConfRepository;

    @Inject
    RscBCacheLayerHelper(
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
            // BCacheRscData.class cannot directly be casted to Class<BCacheRscData<Resource>>. because java.
            // its type is Class<BCacheRscData> (without nested types), but that is not enough as the
            // super constructor wants a Class<RSC_PO>, where RSC_PO is BCacheRscData<Resource>.
            (Class<BCacheRscData<Resource>>) ((Object) BCacheRscData.class),
            DeviceLayerKind.BCACHE,
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
        // BCacheLayer does not have resource-definition specific data
        return null;
    }

    @Override
    protected void mergeRscDfnData(RscDfnLayerObject rscDfnRef, LayerPayload payloadRef)
    {
        // no BCache specific resource-definition, nothing to merge
    }

    @Override
    protected VlmDfnLayerObject createVlmDfnData(
        VolumeDefinition vlmDfnRef,
        String rscNameSuffixRef,
        LayerPayload payloadRef
    )
    {
        // BCacheLayer does not have volume-definition specific data
        return null;
    }

    @Override
    protected void mergeVlmDfnData(VlmDfnLayerObject vlmDfnDataRef, LayerPayload payloadRef)
    {
        // no BCache specific volume-definition, nothing to merge
    }

    @Override
    protected BCacheRscData<Resource> createRscData(
        Resource rscRef,
        LayerPayload payloadRef,
        String rscNameSuffixRef,
        AbsRscLayerObject<Resource> parentObjectRef,
        List<DeviceLayerKind> layerListRef
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
            ValueInUseException
    {
        return layerDataFactory.createBCacheRscData(
            layerRscIdPool.autoAllocate(),
            rscRef,
            rscNameSuffixRef,
            parentObjectRef
        );
    }

    @Override
    protected void mergeRscData(BCacheRscData<Resource> rscDataRef, LayerPayload payloadRef)
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
        throws AccessDeniedException
    {
        Set<StorPool> storPools = new HashSet<>();
        if (needsCacheDevice(rsc, layerListRef))
        {
            storPools.add(getCacheStorPool(rsc, vlmDfn));
        }
        return storPools;
    }

    @Override
    protected BCacheVlmData<Resource> createVlmLayerData(
        BCacheRscData<Resource> bcacheRscData,
        Volume vlm,
        LayerPayload payload,
        List<DeviceLayerKind> layerListRef
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
            ValueInUseException, LinStorException
    {
        StorPool cacheStorPool = null;
        if (needsCacheDevice(bcacheRscData.getAbsResource(), layerListRef))
        {
            cacheStorPool = getCacheStorPool(vlm);
        }
        return layerDataFactory.createBCacheVlmData(vlm, cacheStorPool, bcacheRscData);
    }

    @Override
    protected void mergeVlmData(
        BCacheVlmData<Resource> vlmDataRef,
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
        BCacheRscData<Resource> rscDataRef,
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
                    RscLayerSuffixes.SUFFIX_BCACHE_CACHE,
                    null,
                    DeviceLayerKind.STORAGE
                )
            );
        }

        return children;
    }

    private boolean needsCacheDevice(Resource rsc, List<DeviceLayerKind> layerListRef)
        throws AccessDeniedException
    {
        boolean isNvmeBelow = layerListRef.contains(DeviceLayerKind.NVME);
        boolean isNvmeInitiator = rsc.getStateFlags().isSet(apiCtx, Resource.Flags.NVME_INITIATOR);
        boolean isEbsInitiator = rsc.getStateFlags().isSet(apiCtx, Resource.Flags.EBS_INITIATOR);

        return !isNvmeBelow || isNvmeInitiator || isEbsInitiator;
    }

    @Override
    public StorPool getStorPool(Volume vlmRef, AbsRscLayerObject<Resource> childRef)
        throws AccessDeniedException, InvalidKeyException, InvalidNameException
    {
        StorPool pool;
        BCacheVlmData<Resource> bcacheVlmData = (BCacheVlmData<Resource>) childRef
            .getParent()
            .getVlmProviderObject(
                vlmRef.getVolumeDefinition().getVolumeNumber()
            );
        if (childRef.getSuffixedResourceName().contains(RscLayerSuffixes.SUFFIX_BCACHE_CACHE))
        {
            pool = bcacheVlmData.getCacheStorPool();
        }
        else
        {
            pool = bcacheVlmData.getStorPool();
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
        BCacheRscData<Resource> rscDataRef,
        List<DeviceLayerKind> layerListRef,
        LayerPayload payloadRef
    )
        throws AccessDeniedException, DatabaseException
    {
        return false; // no change
    }

    @Override
    protected boolean isExpectedToProvideDevice(BCacheRscData<Resource> bcacheRscData)
        throws AccessDeniedException
    {
        return !bcacheRscData.hasIgnoreReason();
    }

    private StorPool getCacheStorPool(Resource rsc, VolumeDefinition vlmDfn)
        throws InvalidKeyException, AccessDeniedException
    {
        PriorityProps prioProps = getPrioProps(rsc, vlmDfn);
        String poolName = prioProps.getProp(
            ApiConsts.KEY_BCACHE_POOL_NAME,
            ApiConsts.NAMESPC_BCACHE
        );

        if (poolName == null)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_NOT_FOUND_STOR_POOL,
                    "You have to set the property " +
                        ApiConsts.NAMESPC_BCACHE + "/" +
                        ApiConsts.KEY_BCACHE_POOL_NAME +
                        " for " + CtrlVlmApiCallHandler.getVlmDescription(rsc, vlmDfn) +
                        " in order to use the bcache layer."
                )
            );
        }
        StorPool specStorPool = null;
        try
        {
            specStorPool = rsc.getNode().getStorPool(
                apiCtx,
                new StorPoolName(poolName)
            );

            if (specStorPool == null)
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_NOT_FOUND_STOR_POOL,
                        "The " + CtrlVlmApiCallHandler.getVlmDescription(rsc, vlmDfn) + " specified '" + poolName +
                            "' as the storage pool for bcache. Node " +
                            rsc.getNode().getName() + " does not have a storage pool" +
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
                    "The " + CtrlVlmApiCallHandler.getVlmDescription(rsc, vlmDfn) + " specified '" + poolName +
                        "' as the storage pool for bcache. That name is invalid."
                ),
                exc
            );
        }
        return specStorPool;
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
                    ApiConsts.NAMESPC_BCACHE + "/" + ApiConsts.KEY_BCACHE_POOL_NAME +
                    " for " + CtrlVlmApiCallHandler.getVlmDescriptionInline(vlm) +
                        " in order to use the bcache layer."
                )
            );
        }
        StorPool cacheStorPool = null;
        try
        {
            cacheStorPool = vlm.getAbsResource().getNode().getStorPool(
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
                            vlm.getAbsResource().getNode().getName() + " does not have a storage pool" +
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
            ApiConsts.KEY_BCACHE_POOL_NAME,
            ApiConsts.NAMESPC_BCACHE
        );
    }

    private PriorityProps getPrioProps(Volume vlmRef) throws AccessDeniedException
    {
        return getPrioProps(vlmRef.getAbsResource(), vlmRef.getVolumeDefinition());
    }

    private PriorityProps getPrioProps(Resource rsc, VolumeDefinition vlmDfn) throws AccessDeniedException
    {
        ResourceDefinition rscDfn = vlmDfn.getResourceDefinition();
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
        // BCacheLayer does not have resource-definition specific data
        return null;
    }

    @Override
    protected <RSC extends AbsResource<RSC>> BCacheRscData<Resource> restoreRscData(
        Resource rscRef,
        AbsRscLayerObject<RSC> fromAbsRscDataRef,
        AbsRscLayerObject<Resource> rscParentRef
    )
        throws DatabaseException, AccessDeniedException, ExhaustedPoolException
    {
        return layerDataFactory.createBCacheRscData(
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
        // BCacheLayer does not have volume-definition specific data
        return null;
    }

    @Override
    protected <RSC extends AbsResource<RSC>> BCacheVlmData<Resource> restoreVlmData(
        Volume vlmRef,
        BCacheRscData<Resource> rscDataRef,
        VlmProviderObject<RSC> vlmProviderObjectRef,
        Map<String, String> storpoolRenameMap,
        @Nullable ApiCallRc apiCallRc
    )
        throws DatabaseException, AccessDeniedException, InvalidNameException
    {
        return layerDataFactory.createBCacheVlmData(
            vlmRef,
            AbsLayerHelperUtils.getStorPool(
                apiCtx,
                vlmRef,
                rscDataRef,
                ((BCacheVlmData<RSC>) vlmProviderObjectRef).getCacheStorPool(),
                storpoolRenameMap,
                apiCallRc
            ),
            rscDataRef
        );
    }
}
