package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.drbd.md.MaxSizeException;
import com.linbit.drbd.md.MinSizeException;
import com.linbit.linstor.CtrlStorPoolResolveHelper;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcWith;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
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
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.utils.LayerUtils;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlVlmListApiCallHandler.getVlmDescriptionInline;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Singleton
public class CtrlVlmCrtApiHelper
{
    private final AccessContext apiCtx;
    private final VolumeControllerFactory volumeFactory;
    private final Provider<AccessContext> peerAccCtx;
    private final CtrlStorPoolResolveHelper storPoolResolveHelper;
    private final SystemConfRepository sysCfgRepo;

    @Inject
    CtrlVlmCrtApiHelper(
        @ApiContext AccessContext apiCtxRef,
        VolumeControllerFactory volumeFactoryRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        CtrlStorPoolResolveHelper storPoolResolveHelperRef,
        SystemConfRepository sysCfgRepoRef
    )
    {
        apiCtx = apiCtxRef;
        volumeFactory = volumeFactoryRef;
        peerAccCtx = peerAccCtxRef;
        storPoolResolveHelper = storPoolResolveHelperRef;
        sysCfgRepo = sysCfgRepoRef;
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
            vlmDfnSize = vlmDfn.getVolumeSize(peerAccCtx.get());
            if (snapVlmRef == null)
            {
                vlm = volumeFactory.create(
                    peerAccCtx.get(),
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
                    peerAccCtx.get(),
                    rsc,
                    vlmDfn,
                    null, // flags
                    payload,
                    snapVlmRef.getAbsResource().getLayerData(peerAccCtx.get()),
                    storpoolRenameMap,
                    apiCallRc
                );
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "register " + getVlmDescriptionInline(rsc, vlmDfn),
                ApiConsts.FAIL_ACC_DENIED_VLM
            );
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
                !isOverrideVlmIdPropertySetPrivileged(vlmDfn)
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
                        ctrlProps,
                        apiCtx
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
                            )
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
                    .getLayerData(peerAccCtx.get(), rsc);
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

    private ReadOnlyProps getCtrlPropsPrivileged()
    {
        try
        {
            return sysCfgRepo.getCtrlConfForView(apiCtx);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private Peer getPeerPrivileged(Node assignedNode)
    {
        Peer peer;
        try
        {
            peer = assignedNode.getPeer(apiCtx);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return peer;
    }

    private boolean isOverrideVlmIdPropertySetPrivileged(VolumeDefinition vlmDfn)
    {
        boolean isSet;
        try
        {
            isSet = vlmDfn.getProps(apiCtx)
                .getProp(ApiConsts.KEY_STOR_POOL_OVERRIDE_VLM_ID) != null;
        }
        catch (AccessDeniedException | InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        return isSet;
    }

    private long getVolumeSizePrivileged(VolumeDefinition vlmDfn)
    {
        long volumeSize;
        try
        {
            volumeSize = vlmDfn.getVolumeSize(apiCtx);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return volumeSize;
    }

    public boolean isDiskless(Resource rsc)
    {
        boolean isDiskless;
        try
        {
            StateFlags<Flags> stateFlags = rsc.getStateFlags();
            isDiskless = stateFlags.isSomeSet(
                apiCtx,
                Resource.Flags.DRBD_DISKLESS,
                Resource.Flags.NVME_INITIATOR,
                Resource.Flags.EBS_INITIATOR
            );
        }
        catch (AccessDeniedException implError)
        {
            throw new ImplementationError(implError);
        }
        return isDiskless;
    }
}
