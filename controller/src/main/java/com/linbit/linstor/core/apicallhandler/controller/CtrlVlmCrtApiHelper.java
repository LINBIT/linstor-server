package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.drbd.md.MaxSizeException;
import com.linbit.drbd.md.MinSizeException;
import com.linbit.linstor.CtrlStorPoolResolveHelper;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcWith;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeControllerFactory;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.utils.LayerUtils;
import com.linbit.linstor.utils.layer.LayerVlmUtils;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlVlmListApiCallHandler.getVlmDescriptionInline;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Singleton
public class CtrlVlmCrtApiHelper
{
    private final VolumeControllerFactory volumeFactory;
    private final CtrlStorPoolResolveHelper storPoolResolveHelper;
    private final SystemConfRepository sysCfgRepo;

    @Inject
    CtrlVlmCrtApiHelper(
        VolumeControllerFactory volumeFactoryRef,
        CtrlStorPoolResolveHelper storPoolResolveHelperRef,
        SystemConfRepository sysCfgRepoRef
    )
    {
        volumeFactory = volumeFactoryRef;
        storPoolResolveHelper = storPoolResolveHelperRef;
        sysCfgRepo = sysCfgRepoRef;
    }

    public ApiCallRcWith<StorPool> resolveStorPool(
        final Resource rsc,
        final VolumeDefinition vlmDfn
    )
    {
        final boolean disklessFlag = isDiskless(rsc);
        return storPoolResolveHelper.resolveStorPool(rsc, vlmDfn, disklessFlag);
    }

    public ApiCallRcWith<Volume> createVolumeResolvingStorPool(
        Resource rsc,
        VolumeDefinition vlmDfn,
        @Nullable Map<StorPool.Key, Long> thinFreeCapacities,
        Map<String, String> storpoolRenameMap
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        boolean isDiskless = isDiskless(rsc);
        StorPool storPool = storPoolResolveHelper.resolveStorPool(rsc, vlmDfn, isDiskless).extractApiCallRc(apiCallRc);

        LayerPayload payload = new LayerPayload();
        payload.putStorageVlmPayload("", vlmDfn.getVolumeNumber().value, storPool);

        return new ApiCallRcWith<>(
            apiCallRc,
            createVolume(
                rsc,
                vlmDfn,
                payload,
                thinFreeCapacities,
                storpoolRenameMap,
                apiCallRc
            )
        );
    }

    public Volume createVolume(
        Resource rsc,
        VolumeDefinition vlmDfn,
        LayerPayload payload,
        @Nullable Map<StorPool.Key, Long> thinFreeCapacities,
        Map<String, String> storpoolRenameMap,
        @Nullable ApiCallRc apiCallRc
    )
    {
        return createVlmImpl(rsc, vlmDfn, payload, thinFreeCapacities, null, storpoolRenameMap, apiCallRc);
    }

    public <RSC extends AbsResource<RSC>> Volume createVolumeFromAbsVolume(
        Resource rscRef,
        VolumeDefinition toVlmDfnRef,
        LayerPayload payload,
        @Nullable Map<StorPool.Key, Long> thinFreeCapacities,
        AbsVolume<RSC> fromAbsVolumeRef,
        Map<String, String> storpoolRenameMap,
        @Nullable ApiCallRc apiCallRc
    )
    {
        return createVlmImpl(
            rscRef,
            toVlmDfnRef,
            payload,
            thinFreeCapacities,
            fromAbsVolumeRef,
            storpoolRenameMap,
            apiCallRc
        );
    }

    private <RSC extends AbsResource<RSC>> Volume createVlmImpl(
        Resource rsc,
        VolumeDefinition vlmDfn,
        LayerPayload payload,
        @Nullable Map<StorPool.Key, Long> thinFreeCapacities,
        @Nullable AbsVolume<RSC> snapVlmRef,
        Map<String, String> storpoolRenameMap,
        @Nullable ApiCallRc apiCallRc
    )
    {
        Set<StorPool> storPoolSet = payload.storagePayload.values().stream().map(vlmPayload -> vlmPayload.storPool)
            .collect(Collectors.toSet());
        checkIfStorPoolsAreUsable(rsc, vlmDfn, storPoolSet, thinFreeCapacities);

        Volume vlm;
        long vlmDfnSize = -1;
        try
        {
            vlmDfnSize = vlmDfn.getVolumeSize();
            if (snapVlmRef == null)
            {
                vlm = volumeFactory.create(
                    rsc,
                    vlmDfn,
                    null, // flags
                    payload,
                    null,
                    storpoolRenameMap,
                    apiCallRc
                );
            }
            else
            {
                vlm = volumeFactory.create(
                    rsc,
                    vlmDfn,
                    null, // flags
                    payload,
                    snapVlmRef.getAbsResource().getLayerData(),
                    storpoolRenameMap,
                    apiCallRc
                );
            }
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_EXISTS_VLM,
                "The " + getVlmDescriptionInline(rsc, vlmDfn) + " already exists",
                true
            ), dataAlreadyExistsExc);
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
        catch (StorageException exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.copyFromLinstorExc(ApiConsts.FAIL_STOR_POOL_CONFIGURATION_ERROR, exc)
            );
        }
        catch (MinSizeException | MaxSizeException exc)
        {
            final String smallLarge = exc instanceof MinSizeException ? "small" : "large";
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_VLM_SIZE,
                    "The size of the volume-definition [" + vlmDfnSize + "KiB] is too " + smallLarge
                )
            );
        }

        return vlm;
    }

    private void checkIfStorPoolsAreUsable(
        Resource rsc,
        VolumeDefinition vlmDfn,
        Set<StorPool> storPoolSetRef,
        @Nullable Map<StorPool.Key, Long> thinFreeCapacities
    )
    {
        /*
         * check if StorPool is usable only if
         * - storPool has Backing storage
         * - satellite is online
         * - storPool is Fat-provisioned or we have a map of the thin-free-spaces available
         * - the overrideVlmId property is not set; in this case we assume the volume already
         * exists on the storPool, which means we will not consume additional $volumeSize space
         * - the volume does not reuse the data of an already existing volume of a shared storage
         * pool, which also does not consume additional space
         */

        Set<StorPool> disklessPools = new HashSet<>();
        Set<StorPool> thinPools = new HashSet<>();
        Set<StorPool> poolsToCheck = new HashSet<>();

        for (StorPool storPool : storPoolSetRef)
        {
            DeviceProviderKind devProviderKind = storPool.getDeviceProviderKind();
            if (devProviderKind.hasBackingDevice())
            {
                if (devProviderKind.usesThinProvisioning())
                {
                    thinPools.add(storPool);
                }
                else
                {
                    poolsToCheck.add(storPool);
                }
            }
            else
            {
                disklessPools.add(storPool);
            }
        }

        if (thinFreeCapacities != null)
        {
            poolsToCheck.addAll(thinPools);
        }
        final ReadOnlyProps ctrlProps = getCtrlPropsPrivileged();
        for (StorPool storPool : poolsToCheck)
        {
            if (getPeerPrivileged(rsc.getNode()).getConnectionStatus() == ApiConsts.ConnectionStatus.ONLINE &&
                !isOverrideVlmIdPropertySetPrivileged(vlmDfn) &&
                !reusesSharedVolume(rsc, vlmDfn, storPool)
            )
            {
                /*
                 * TODO: improve this size check. Problem is that i.e. snapshot (and backup) restore have layerData to
                 * grab meta-storage pools from.
                 * Resource create kinda has that information but no accurate sizes for the meta-devices since those are
                 * only calculated on the satellite.
                 *
                 * That is why (for now) the snapshot restore is dumbed down (in
                 * CtrlSnapshotRestoreApiCallHAndler#restoreOnNode) to only include the data-storage pool to
                 * this set of SP that will be checked here. This might fail later if a metapool runs out of space on
                 * the satellite which is also not really what one would desire.
                 */
                if (!FreeCapacityAutoPoolSelectorUtils
                    .isStorPoolUsable(
                        getVolumeSizePrivileged(vlmDfn),
                        thinFreeCapacities,
                        true,
                        storPool.getName(),
                        rsc.getNode(),
                        ctrlProps
                    )
                    // allow the volume to be created if the free capacity is unknown
                    .orElse(true)
                )
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_INVLD_VLM_SIZE,
                            String.format(
                                "Not enough free space available for volume %d of resource '%s'.",
                                vlmDfn.getVolumeNumber().value,
                                rsc.getResourceDefinition().getName().getDisplayName()
                            ),
                            true
                        )
                    );
                }
            }
            if (rsc.getVolumeCount() == 1)
            {
                final String errorObj;
                final DeviceProviderKind providerKind = storPool.getDeviceProviderKind();
                if (providerKind.equals(DeviceProviderKind.EBS_TARGET))
                {
                    errorObj = "EBS";
                }
                else
                {
                    errorObj = null;
                }
                if (errorObj != null)
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_INVLD_VLM_COUNT,
                            "EBS based resources may only have one volumedefinition"
                        )
                    );
                }
            }
        }

        if (!disklessPools.isEmpty())
        {
            List<StorPool> nonEbsDisklessPools = disklessPools.stream()
                .filter(sp -> !sp.getDeviceProviderKind().equals(DeviceProviderKind.EBS_INIT))
                .collect(Collectors.toList());

            if (!nonEbsDisklessPools.isEmpty())
            {
                AbsRscLayerObject<Resource> rscData = CtrlRscToggleDiskApiCallHandler
                    .getLayerData(rsc);
                if (!LayerUtils.hasLayer(rscData, DeviceLayerKind.DRBD) &&
                    !LayerUtils.hasLayer(rscData, DeviceLayerKind.NVME))
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_INVLD_LAYER_STACK,
                            "Diskless volume is only supported in combination with DRBD and/or NVME"
                        )
                    );
                }
            }
        }
    }

    /**
     * Whether the data of the volume to be created already exists in the given shared storage pool
     * because another resource of the rsc-dfn has a volume backed by the same shared data. Creating
     * such a volume only attaches to the shared data instead of allocating new space - the shared
     * pool's free space already accounts for it - so the free-space check has to be skipped.
     */
    private boolean reusesSharedVolume(Resource rsc, VolumeDefinition vlmDfn, StorPool storPool)
    {
        boolean reuses = false;
        SharedStorPoolName sharedSpName = storPool.getSharedStorPoolName();
        if (storPool.isShared())
        {
            Iterator<Resource> rscIt = rsc.getResourceDefinition().iterateResource();
            while (rscIt.hasNext() && !reuses)
            {
                Resource otherRsc = rscIt.next();
                @Nullable Volume otherVlm = otherRsc.equals(rsc) ?
                    null :
                    otherRsc.getVolume(vlmDfn.getVolumeNumber());
                if (otherVlm != null)
                {
                    for (StorPool otherSp : LayerVlmUtils.getStorPoolMap(otherVlm).values())
                    {
                        if (sharedSpName.equals(otherSp.getSharedStorPoolName()))
                        {
                            reuses = true;
                            break;
                        }
                    }
                }
            }
        }
        return reuses;
    }

    private ReadOnlyProps getCtrlPropsPrivileged()
    {
        return sysCfgRepo.getCtrlConfForView();
    }

    private Peer getPeerPrivileged(Node assignedNode)
    {
        Peer peer;
        peer = assignedNode.getPeer();
        return peer;
    }

    private boolean isOverrideVlmIdPropertySetPrivileged(VolumeDefinition vlmDfn)
    {
        boolean isSet;
        try
        {
            isSet = vlmDfn.getProps()
                .getProp(ApiConsts.KEY_STOR_POOL_OVERRIDE_VLM_ID) != null;
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        return isSet;
    }

    private long getVolumeSizePrivileged(VolumeDefinition vlmDfn)
    {
        long volumeSize;
        volumeSize = vlmDfn.getVolumeSize();
        return volumeSize;
    }

    public boolean isDiskless(Resource rsc)
    {
        boolean isDiskless;
        StateFlags<Flags> stateFlags = rsc.getStateFlags();
        isDiskless = stateFlags.isSomeSet(
            Resource.Flags.DRBD_DISKLESS,
            Resource.Flags.NVME_INITIATOR,
            Resource.Flags.EBS_INITIATOR
        );
        return isDiskless;
    }
}
