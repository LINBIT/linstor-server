package com.linbit.linstor.core.objects.utils;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.data.provider.zfs.ZfsData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.storage.utils.ZfsPropsUtils;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;
import com.linbit.linstor.utils.layer.DrbdLayerUtils;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.linstor.utils.layer.LayerVlmUtils;
import com.linbit.utils.MathUtils;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

@Singleton
public class MixedStorPoolHelper
{
    private final AccessContext sysCtx;
    private final SystemConfRepository systemConfRepository;

    @Inject
    public MixedStorPoolHelper(@SystemContext AccessContext sysCtxRef, SystemConfRepository systemConfRepositoryRef)
    {
        sysCtx = sysCtxRef;
        systemConfRepository = systemConfRepositoryRef;
    }

    public void handleMixedStoragePools(Volume vlmRef)
        throws AccessDeniedException, DatabaseException, ImplementationError, StorageException
    {
        VolumeDefinition vlmDfn = vlmRef.getVolumeDefinition();
        ResourceDefinition rscDfn = vlmDfn.getResourceDefinition();
        Set<StorPool> storPoolSet = getAllStorPools(rscDfn);
        if (hasMixedStoragePools(rscDfn, storPoolSet))
        {
            ensureAllStltsHaveDrbdVersion(storPoolSet);
            ensureNoGrossSize(vlmDfn);
            try
            {
                rscDfn.getProps(sysCtx)
                    .setProp(
                        InternalApiConsts.KEY_FORCE_INITIAL_SYNC_PERMA,
                        ApiConsts.VAL_TRUE,
                        ApiConsts.NAMESPC_DRBD_OPTIONS
                    );
            }
            catch (AccessDeniedException | InvalidKeyException | InvalidValueException exc)
            {
                throw new ImplementationError(exc);
            }
        }

        Props vlmDfnProps = vlmDfn.getProps(sysCtx);

        try
        {
            @Nullable Long leastCommonExtentSizeInKib = getLeastCommonExtentSizeInKib(vlmRef);
            if (leastCommonExtentSizeInKib != null)
            {
                vlmDfnProps.setProp(
                    InternalApiConsts.ALLOCATION_GRANULARITY,
                    Long.toString(leastCommonExtentSizeInKib),
                    StorageConstants.NAMESPACE_INTERNAL
                );
            }
        }
        catch (InvalidKeyException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (InterruptedException exc)
        {
            Thread.currentThread().interrupt();
            throw new ApiRcException(
                ApiCallRcImpl.singleApiCallRc(
                    ApiConsts.MASK_ERROR,
                    "AllocationGranularity calculation was interrupted"
                ),
                exc,
                false
            );
        }
    }

    private void ensureNoGrossSize(VolumeDefinition vlmDfnRef)
    {
        try
        {
            // TODO: moving the size-calculations to the server project is NOT enough to remove this check
            // To properly remove this check, we need DRBD support to set the specific size of the DRBD device
            // regardless of the size of the backing disk
            if (vlmDfnRef.getFlags().isSet(sysCtx, VolumeDefinition.Flags.GROSS_SIZE))
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_SP_MIXING_NOT_ALLOWED,
                        "Mixed Storage pools is (currently) not allowed with gross-sizes"
                    )
                );
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private void ensureAllStltsHaveDrbdVersion(Set<StorPool> storPoolSetRef)
    {
        try
        {
            for (StorPool sp : storPoolSetRef)
            {
                ExtToolsManager extToolsMgr = sp.getNode()
                    .getPeer(sysCtx)
                    .getExtToolsManager();
                ExtToolsInfo info = extToolsMgr.getExtToolInfo(ExtTools.DRBD9_KERNEL);
                if (info == null)
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_STLT_DOES_NOT_SUPPORT_LAYER,
                            sp.getNode() + " does not support DRBD9!"
                        )
                    );
                }
                if (!DeviceProviderKind.doesDrbdVersionSupportStorPoolMixing(info.getVersion()))
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_STLT_DOES_NOT_SUPPORT_LAYER,
                            String.format(
                                "%s has DRBD version %s, but version %s (or higher) is required",
                                sp.getNode(),
                                info.getVersion(),
                                info.getVersionMinor() == 1 ?
                                    DeviceProviderKind.SP_MIXING_REQ_DRBD91_MIN_VERISON :
                                    DeviceProviderKind.SP_MIXING_REQ_DRBD92_MIN_VERISON
                            )
                        )
                    );
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private Set<StorPool> getAllStorPools(ResourceDefinition resourceDefinitionRef)
        throws AccessDeniedException
    {
        Set<StorPool> ret = new HashSet<>();
        Iterator<Resource> rscIt = resourceDefinitionRef.iterateResource(sysCtx);
        while (rscIt.hasNext())
        {
            Resource rsc = rscIt.next();
            Set<StorPool> storPools = LayerVlmUtils.getStorPools(rsc, sysCtx, false);
            ret.addAll(storPools);
        }
        return ret;
    }

    private boolean hasMixedStoragePools(ResourceDefinition rscDfnRef, Set<StorPool> storPoolSetRef)
    {
        boolean usesMixedSkipInitSync;

        Set<String> allocationGranularities = new HashSet<>();
        try
        {
            usesMixedSkipInitSync = usesMixedSkipInitialSync(rscDfnRef);

            // no need to go through storage pools if the result of this method will already be true at this point
            if (!usesMixedSkipInitSync)
            {
                for (StorPool sp : storPoolSetRef)
                {
                    DeviceProviderKind kind = sp.getDeviceProviderKind();
                    if (!kind.equals(DeviceProviderKind.DISKLESS))
                    {
                        allocationGranularities.add(
                            sp.getProps(sysCtx)
                                .getProp(InternalApiConsts.ALLOCATION_GRANULARITY, StorageConstants.NAMESPACE_INTERNAL)
                        );
                    }
                }
            }
        }
        catch (InvalidKeyException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return usesMixedSkipInitSync || allocationGranularities.size() > 1;
    }

    private boolean usesMixedSkipInitialSync(ResourceDefinition rscDfnRef) throws AccessDeniedException
    {
        boolean doesNotUseSkipInitSync = false;
        boolean usesSkipInitSync = false;
        Iterator<Resource> rscIt = rscDfnRef.iterateResource(sysCtx);
        while (rscIt.hasNext())
        {
            Resource rsc = rscIt.next();
            if (!rsc.getStateFlags().isSet(sysCtx, Resource.Flags.DRBD_DISKLESS))
            {
                Set<AbsRscLayerObject<Resource>> drbdRscDataSet = LayerRscUtils.getRscDataByLayer(
                    rsc.getLayerData(sysCtx),
                    DeviceLayerKind.DRBD
                );
                for (AbsRscLayerObject<Resource> rscData : drbdRscDataSet)
                {
                    DrbdRscData<Resource> drbdRscData = (DrbdRscData<Resource>) rscData;
                    for (DrbdVlmData<Resource> drbdVlmData : drbdRscData.getVlmLayerObjects().values())
                    {
                        if (DrbdLayerUtils.skipInitSync(sysCtx, drbdVlmData))
                        {
                            usesSkipInitSync = true;
                        }
                        else
                        {
                            doesNotUseSkipInitSync = true;
                        }
                    }
                }
            }
        }
        return usesSkipInitSync && doesNotUseSkipInitSync;
    }

    private @Nullable Long getLeastCommonExtentSizeInKib(Volume vlmRef)
        throws AccessDeniedException, StorageException, InterruptedException
    {
        SortedSet<Long> minGranularities = new TreeSet<>();
        @Nullable Long vlmDfnExtentSize = getCurrentVlmDfnExtentSize(vlmRef.getVolumeDefinition());
        if (vlmDfnExtentSize != null)
        {
            minGranularities.add(vlmDfnExtentSize);
        }
        Set<Long> storPoolAndVlmExtentSizes = extractStorPoolAndVlmExtentSizes(vlmRef);
        minGranularities.addAll(storPoolAndVlmExtentSizes);

        // in case we are creating a new volume-definition while we already have a few resources deployed, the first
        // volume that we have to process here might be a diskless volume (DRBD, NVME, ...). In that case we have no
        // sources for AllocationGranularities, i.e. the set is empty.
        // Calling MathUtils.LCM with an empty set returns 0, which we MUST NOT store in the vlmDfn properties, since
        // the next MathUtils.LCM call would include that 0 in the set, which the LCM will complain about.

        @Nullable Long ret;
        // just in case we did some mistake and a 0 slipped into the set - we still have to prevent a 0 getting stored
        // in the vlmDfn
        minGranularities.remove(0L);
        if (minGranularities.isEmpty())
        {
            ret = null;
        }
        else
        {
            ret = MathUtils.leastCommonMultiple(minGranularities);
        }
        return ret;
    }

    private @Nullable Long getCurrentVlmDfnExtentSize(VolumeDefinition vlmDfnRef)
        throws AccessDeniedException
    {
        String currentGranularityStr;
        try
        {
            currentGranularityStr = vlmDfnRef.getProps(sysCtx)
                .getProp(
                    InternalApiConsts.ALLOCATION_GRANULARITY,
                    StorageConstants.NAMESPACE_INTERNAL
                );
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        return currentGranularityStr == null ? null : Long.parseLong(currentGranularityStr);
    }

    private Set<Long> extractStorPoolAndVlmExtentSizes(Volume vlmRef)
        throws AccessDeniedException, StorageException
    {
        Set<Long> extentSizes = new HashSet<>();

        AbsRscLayerObject<Resource> rscData = vlmRef.getAbsResource().getLayerData(sysCtx);
        Set<AbsRscLayerObject<Resource>> storRscDataSet = LayerRscUtils.getRscDataByLayer(
            rscData,
            DeviceLayerKind.STORAGE,
            RscLayerSuffixes::isNonMetaDataLayerSuffix
        );
        for (AbsRscLayerObject<Resource> storRscData : storRscDataSet)
        {
            for (VlmProviderObject<Resource> vlmData : storRscData.getVlmLayerObjects().values())
            {
                StorPool sp = vlmData.getStorPool();
                String spGranu = sp.getProps(sysCtx)
                    .getProp(
                        InternalApiConsts.ALLOCATION_GRANULARITY,
                        StorageConstants.NAMESPACE_INTERNAL
                    );
                if (spGranu != null)
                {
                    extentSizes.add(Long.parseLong(spGranu));
                }
                Long vlmExtentSize = null;
                switch (sp.getDeviceProviderKind())
                {
                    case DISKLESS: // fall-through
                    case EBS_INIT:// fall-through
                    case EBS_TARGET:// fall-through
                    case FILE:// fall-through
                    case FILE_THIN:// fall-through
                    case LVM:// fall-through
                    case LVM_THIN:// fall-through
                    case REMOTE_SPDK:// fall-through
                    case SPDK:// fall-through
                    case STORAGE_SPACES:// fall-through
                    case STORAGE_SPACES_THIN:
                        break;
                    case ZFS:// fall-through
                    case ZFS_THIN:
                        vlmExtentSize = ZfsPropsUtils.extractZfsVolBlockSizePrivileged(
                            (ZfsData<?>) vlmData,
                            sysCtx,
                            systemConfRepository.getStltConfForView(sysCtx)
                        );
                        break;
                    case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER:
                    default:
                        throw new ImplementationError("Not implemented for kind: " + sp.getDeviceProviderKind());
                }
                if (vlmExtentSize != null)
                {
                    extentSizes.add(vlmExtentSize);
                }
            }
        }

        return extentSizes;
    }
}
