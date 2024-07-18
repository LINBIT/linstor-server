package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcWith;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.controller.CtrlPropsHelper;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo.Version;
import com.linbit.linstor.utils.layer.LayerVlmUtils;

import static com.linbit.linstor.api.ApiConsts.FAIL_INVLD_STOR_POOL_NAME;
import static com.linbit.linstor.api.ApiConsts.FAIL_NOT_FOUND_DFLT_STOR_POOL;
import static com.linbit.linstor.api.ApiConsts.KEY_RSC_ALLOW_MIXING_DEVICE_KIND;
import static com.linbit.linstor.api.ApiConsts.KEY_STOR_POOL_NAME;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Singleton
public class CtrlStorPoolResolveHelper
{
    private final AccessContext apiCtx;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final Provider<AccessContext> peerCtxProvider;

    /**
     * Disables all checks of this class. Used while loading data from a database that was created with more
     * relaxed limitations
     */
    private boolean enableChecks;

    @Inject
    public CtrlStorPoolResolveHelper(
        @ApiContext AccessContext apiCtxRef,
        @PeerContext Provider<AccessContext> peerCtxProviderRef,
        CtrlPropsHelper ctrlPropsHelperRef
    )
    {
        apiCtx = apiCtxRef;
        peerCtxProvider = peerCtxProviderRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
    }

    /**
     * Resolves the correct storage pool and also handles error/warnings in diskless modes.
     */
    public ApiCallRcWith<StorPool> resolveStorPool(
        Resource rsc,
        VolumeDefinition vlmDfn,
        boolean isRscDiskless
    )
    {
        return resolveStorPool(peerCtxProvider.get(), rsc, vlmDfn, isRscDiskless, true, true);
    }

    /**
     * Resolves the correct storage pool and also handles error/warnings in diskless modes.
     */
    public ApiCallRcWith<StorPool> resolveStorPool(
        Resource rsc,
        VolumeDefinition vlmDfn,
        boolean isRscDiskless,
        boolean throwExcIfStorPoolIsNull
    )
    {
        return resolveStorPool(
            peerCtxProvider.get(),
            rsc,
            vlmDfn,
            isRscDiskless,
            throwExcIfStorPoolIsNull,
            true
        );
    }

    /**
     * Resolves the correct storage pool and also handles error/warnings in diskless modes.
     */
    public ApiCallRcWith<StorPool> resolveStorPool(
        AccessContext accCtx,
        Resource rsc,
        VolumeDefinition vlmDfn,
        boolean isRscDiskless,
        boolean throwExcIfStorPoolIsNull,
        boolean searchForAlternatives
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        StorPool storPool = null;
        try
        {
            ReadOnlyProps rscProps = ctrlPropsHelper.getProps(accCtx, rsc);
            ReadOnlyProps vlmDfnProps = ctrlPropsHelper.getProps(accCtx, vlmDfn);
            ReadOnlyProps rscDfnProps = ctrlPropsHelper.getProps(accCtx, rsc.getResourceDefinition());
            ReadOnlyProps rscGrpProps = ctrlPropsHelper.getProps(
                accCtx,
                rsc.getResourceDefinition().getResourceGroup()
            );
            ReadOnlyProps nodeProps = ctrlPropsHelper.getProps(accCtx, rsc.getNode());

            PriorityProps vlmPrioProps = new PriorityProps(
                rscProps, vlmDfnProps, rscDfnProps, rscGrpProps, nodeProps
            );
            PriorityProps spMixingPrioProps = new PriorityProps(
                rscDfnProps,
                rscGrpProps,
                ctrlPropsHelper.getCtrlPropsForView(accCtx)
            );

            String storPoolNameStr = vlmPrioProps.getProp(KEY_STOR_POOL_NAME);
            if (isRscDiskless)
            {
                if (storPoolNameStr == null || "".equals(storPoolNameStr))
                {
                    // If the resource was marked as diskless then there should be a resource property identifying the
                    // diskless pool.
                    storPool = null;
                }
                else
                {
                    storPool = rsc.getNode().getStorPool(
                        apiCtx,
                        LinstorParsingUtils.asStorPoolName(storPoolNameStr)
                    );
                }

                checkBackingDiskWithDiskless(rsc, storPool);
            }
            else
            {
                List<StorPoolName> possibleStorPools;
                if (storPoolNameStr == null || storPoolNameStr.isEmpty())
                {
                    if (searchForAlternatives)
                    {
                        possibleStorPools = resolveNeighbourDiskfulStorPool(rsc);
                    }
                    else
                    {
                        possibleStorPools = new ArrayList<>();
                    }
                }
                else
                {
                    possibleStorPools = Collections.singletonList(LinstorParsingUtils.asStorPoolName(storPoolNameStr));
                }

                for (StorPoolName storPoolName : possibleStorPools)
                {
                    storPool = rsc.getNode().getStorPool(apiCtx, storPoolName);
                    if (storPool != null)
                    {
                        boolean spMixingAllowed = spMixingPrioProps.getProp(
                            KEY_RSC_ALLOW_MIXING_DEVICE_KIND,
                            null,
                            "false"
                        )
                            .equalsIgnoreCase(ApiConsts.VAL_TRUE);
                        if (storPool.getDeviceProviderKind().hasBackingDevice())
                        {
                            // If the storage pool has backing storage,
                            // check the current storPool's kind is compatible with the peer kinds
                            checkSameKindAsPeers(vlmDfn, storPool, spMixingAllowed);
                        }
                        break;
                    }
                }
            }

            if (throwExcIfStorPoolIsNull)
            {
                // TODO rather than throwing an error, we could try to let the auto-placer decide what storpool to take
                checkStorPoolLoaded(rsc, storPool, storPoolNameStr, vlmDfn);
            }
        }
        catch (InvalidKeyException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }

        return new ApiCallRcWith<>(responses, storPool);
    }

    public void setEnableChecks(boolean enableCheckRef)
    {
        enableChecks = enableCheckRef;
    }

    private void checkSameKindAsPeers(VolumeDefinition vlmDfn, StorPool storPool, boolean allowMixingRef)
        throws AccessDeniedException
    {
        if (!enableChecks)
        {
            return;
        }

        DeviceProviderKind currentDriverKind = storPool.getDeviceProviderKind();
        Node currentNode = storPool.getNode();
        Version currentDrbdVersion = currentNode.getPeer(apiCtx).getExtToolsManager().getVersion(ExtTools.DRBD9_KERNEL);

        for (Resource peerRsc : vlmDfn.getResourceDefinition().streamResource(apiCtx).collect(Collectors.toList()))
        {
            if (!peerRsc.isDiskless(apiCtx) && !peerRsc.getNode().equals(currentNode))
            {
                Volume peerVlm = peerRsc.getVolume(vlmDfn.getVolumeNumber());
                if (peerVlm != null)
                {
                    Version peerDrbdVersion = peerVlm.getAbsResource()
                        .getNode()
                        .getPeer(apiCtx)
                        .getExtToolsManager()
                        .getVersion(ExtTools.DRBD9_KERNEL);
                    for (StorPool peerStorPool : LayerVlmUtils.getStorPoolSet(peerVlm, apiCtx, false))
                    {
                        DeviceProviderKind peerKind = peerStorPool.getDeviceProviderKind();
                        if (!DeviceProviderKind.isMixingAllowed(
                            currentDriverKind,
                            currentDrbdVersion,
                            peerKind,
                            peerDrbdVersion,
                            allowMixingRef
                        ))
                        {
                            if (!allowMixingRef)
                            {
                                boolean wouldBeAllowedWithSpMixing = DeviceProviderKind.isMixingAllowed(
                                    currentDriverKind,
                                    currentDrbdVersion,
                                    peerKind,
                                    peerDrbdVersion,
                                    true
                                );
                                if (wouldBeAllowedWithSpMixing)
                                {
                                    throw new ApiRcException(
                                        makeInvalidDriverKindError(currentDriverKind, peerKind, true)
                                    );
                                }
                            }
                            throw new ApiRcException(makeInvalidDriverKindError(currentDriverKind, peerKind, false));
                        }
                    }
                }
            }
        }
    }

    private List<StorPoolName> resolveNeighbourDiskfulStorPool(final Resource rsc)
        throws AccessDeniedException
    {
        ArrayList<StorPoolName> storpools = new ArrayList<>();
        for (Resource peerRsc : rsc.getResourceDefinition().streamResource(apiCtx).collect(Collectors.toList()))
        {
            if (!peerRsc.isDiskless(apiCtx) && !peerRsc.getNode().getName().equals(rsc.getNode().getName()))
            {
                for (Volume peerVlm : peerRsc.streamVolumes().collect(Collectors.toList()))
                {
                    for (StorPool peerStorPool : LayerVlmUtils.getStorPoolSet(peerVlm, apiCtx, false))
                    {
                        if (peerStorPool.getDeviceProviderKind().hasBackingDevice())
                        {
                            storpools.add(peerStorPool.getName());
                        }
                    }
                }
            }
        }

        storpools.add(LinstorParsingUtils.asStorPoolName(InternalApiConsts.DEFAULT_STOR_POOL_NAME));

        return storpools;
    }

    private void checkBackingDiskWithDiskless(final Resource rsc, final StorPool storPool)
    {
        if (enableChecks && storPool != null && storPool.getDeviceProviderKind().hasBackingDevice())
        {
            throw new ApiRcException(ApiCallRcImpl
                .entryBuilder(FAIL_INVLD_STOR_POOL_NAME,
                              "Storage pool with backing disk not allowed with diskless resource.")
                .setCause(String.format("Resource '%s' flagged as diskless, but a storage pool '%s' " +
                        "with backing disk was specified.",
                    rsc.getResourceDefinition().getName().displayValue,
                    storPool.getName().displayValue))
                .setCorrection("Use a storage pool with a diskless driver or remove the diskless flag.")
                .build(),
                new LinStorException("Incorrect storage pool used.")
            );
        }
    }

    private void checkStorPoolLoaded(
        final Resource rsc,
        StorPool storPool,
        String storPoolNameStr,
        final VolumeDefinition vlmDfn
    )
    {
        if (storPool == null)
        {
            throw new ApiRcException(ApiCallRcImpl
                .entryBuilder(FAIL_NOT_FOUND_DFLT_STOR_POOL, "The storage pool '" + storPoolNameStr + "' " +
                    "for resource '" + rsc.getResourceDefinition().getName().displayValue + "' " +
                    "for volume number '" + vlmDfn.getVolumeNumber().value + "' " +
                    "is not deployed on node '" + rsc.getNode().getName().displayValue + "'.")
                .setDetails("The resource which should be deployed had at least one volume definition " +
                    "(volume number '" + vlmDfn.getVolumeNumber().value + "') which LINSTOR " +
                    "tried to automatically create. " +
                    "The storage pool name for this new volume was looked for, in order, in: " +
                    "the properties of the resource, volume definition, resource definition and node, " +
                    "and finally in a system wide default storage pool name defined by " +
                    "the LINSTOR Controller.")
                .build(),
                new LinStorException("Dependency not found")
            );
        }
    }

    private ApiCallRcImpl.ApiCallRcEntry makeInvalidDriverKindError(
        DeviceProviderKind driverKind,
        DeviceProviderKind peerKind,
        boolean wouldBeAllowedWithSpMixingRef
    )
    {
        return ApiCallRcImpl
            .entryBuilder(
                ApiConsts.FAIL_INVLD_STOR_DRIVER,
                String.format("Storage driver '%s' not allowed for volume.", driverKind)
            )
            .setDetails("Using storage pools with different storage drivers on the same volume definition " +
                "is not supported.")
            .setCorrection(
                wouldBeAllowedWithSpMixingRef ?
                    String.format(
                        "Either use a storage pool with a driver compatible with %s, or enable storage pool mixing " +
                            "using the property '%s'",
                        peerKind,
                        KEY_RSC_ALLOW_MIXING_DEVICE_KIND
                    ) :
                    String.format("Use a storage pool with the driver '%s'", peerKind)
            )
            .build();
    }
}
