package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.Node;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeData;
import com.linbit.linstor.VolumeDataFactory;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcWith;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.ConfigModule;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSQLException;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageDriverKind;

import static com.linbit.linstor.api.ApiConsts.FAIL_INVLD_STOR_POOL_NAME;
import static com.linbit.linstor.api.ApiConsts.FAIL_NOT_FOUND_DFLT_STOR_POOL;
import static com.linbit.linstor.api.ApiConsts.KEY_STOR_POOL_NAME;
import static com.linbit.linstor.api.ApiConsts.MASK_STOR_POOL;
import static com.linbit.linstor.api.ApiConsts.MASK_WARN;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlVlmApiCallHandler.getVlmDescriptionInline;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.Map;
import java.util.stream.Collectors;

@Singleton
class CtrlVlmCrtApiHelper
{
    private final AccessContext apiCtx;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final VolumeDataFactory volumeDataFactory;
    private final Provider<AccessContext> peerAccCtx;
    private final String defaultStorPoolName;

    @Inject
    CtrlVlmCrtApiHelper(
        @ApiContext AccessContext apiCtxRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        VolumeDataFactory volumeDataFactoryRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        @Named(ConfigModule.CONFIG_STOR_POOL_NAME) String defaultStorPoolNameRef
    )
    {
        apiCtx = apiCtxRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        volumeDataFactory = volumeDataFactoryRef;
        peerAccCtx = peerAccCtxRef;
        defaultStorPoolName = defaultStorPoolNameRef;
    }

    public ApiCallRcWith<VolumeData> createVolumeResolvingStorPool(
        Resource rsc,
        VolumeDefinition vlmDfn,
        Map<StorPool.Key, Long> thinFreeCapacities,
        StorageDriverKind allowedKind
    )
    {
        return createVolumeResolvingStorPool(rsc, vlmDfn, thinFreeCapacities, null, null, allowedKind);
    }

    public ApiCallRcWith<VolumeData> createVolumeResolvingStorPool(
        Resource rsc,
        VolumeDefinition vlmDfn,
        Map<StorPool.Key, Long> thinFreeCapacities,
        String blockDevice,
        String metaDisk,
        StorageDriverKind allowedKind
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        boolean isDless = isDiskless(rsc);
        StorPool storPool = resolveStorPool(rsc, vlmDfn, isDless).extractApiCallRc(apiCallRc);
        isDless = isDiskless(rsc);  // recheck diskless, resolveStorPool might mark resource diskless if dless storage

        if (allowedKind != null && !isDless && storPool.getDriverKind() != allowedKind)
        {
            throw new ApiRcException(
                ApiCallRcImpl.entryBuilder(
                    ApiConsts.FAIL_INVLD_STOR_DRIVER,
                    String.format(
                        "Storage driver '%s' not allowed for volume.",
                        storPool.getDriverKind().getDriverName()))
                    .setDetails("It is not supported to use storage pools with different" +
                        "storage drivers on the same volume definition.")
                    .setCorrection(
                        String.format("Use a storage pool with the driver kind '%s'", allowedKind.getDriverName()))
                    .build()
            );
        }

        return new ApiCallRcWith<>(apiCallRc, createVolume(
            rsc,
            vlmDfn,
            storPool,
            thinFreeCapacities,
            blockDevice,
            metaDisk
        ));
    }

    public static StorageDriverKind firstStorageDriverKind(
        final ResourceDefinition rscDfn,
        final VolumeDefinition vlmDfn,
        final AccessContext apiCtx
    )
    {
        StorageDriverKind allowedDriverKind = null;
        try
        {
            for (Resource rsc : rscDfn.streamResource(apiCtx).collect(Collectors.toList()))
            {
                if (!rsc.isDiskless(apiCtx))
                {
                    if (rsc.streamVolumes().findFirst().isPresent())
                    {
                        Volume vlm = rsc.getVolume(vlmDfn.getVolumeNumber());
                        if (vlm != null)
                        {
                            StorPool storPool = vlm.getStorPool(apiCtx);
                            allowedDriverKind = storPool.getDriverKind();
                            break;
                        }
                    }
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return allowedDriverKind;
    }

    public VolumeData createVolume(
        Resource rsc,
        VolumeDefinition vlmDfn,
        StorPool storPool,
        Map<StorPool.Key, Long> thinFreeCapacities,
        String blockDevice,
        String metaDisk
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
        if (storPool.getDriverKind().hasBackingStorage() &&
            getPeerPrivileged(rsc.getAssignedNode()).getConnectionStatus() == Peer.ConnectionStatus.ONLINE &&
            (thinFreeCapacities != null || !storPool.getDriverKind().usesThinProvisioning()) &&
            !isOverrideVlmIdPropertySetPrivileged(vlmDfn)
        )
        {
            if (!FreeCapacityAutoPoolSelectorUtils
                .isStorPoolUsable(
                    getVolumeSizePrivileged(vlmDfn),
                    thinFreeCapacities,
                    true,
                    storPool.getName(),
                    rsc.getAssignedNode(),
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
                            rsc.getDefinition().getName().getDisplayName()
                        )
                    )
                );
            }
        }

        VolumeData vlm;
        try
        {
            vlm = volumeDataFactory.create(
                peerAccCtx.get(),
                rsc,
                vlmDfn,
                storPool,
                blockDevice,
                metaDisk,
                null // flags
            );
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
                "The " + getVlmDescriptionInline(rsc, vlmDfn) + " already exists"
            ), dataAlreadyExistsExc);
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
        return vlm;
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

    /**
     * Resolves the correct storage pool and also handles error/warnings in diskless modes.
     */
    public ApiCallRcWith<StorPool> resolveStorPool(
        Resource rsc,
        VolumeDefinition vlmDfn,
        boolean isRscDiskless
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        StorPool storPool;
        try
        {
            Props rscProps = ctrlPropsHelper.getProps(rsc);
            Props vlmDfnProps = ctrlPropsHelper.getProps(vlmDfn);
            Props rscDfnProps = ctrlPropsHelper.getProps(rsc.getDefinition());
            Props nodeProps = ctrlPropsHelper.getProps(rsc.getAssignedNode());

            PriorityProps vlmPrioProps = new PriorityProps(
                rscProps, vlmDfnProps, rscDfnProps, nodeProps
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
                    storPool = rsc.getAssignedNode().getStorPool(
                        apiCtx,
                        LinstorParsingUtils.asStorPoolName(storPoolNameStr)
                    );
                }

                checkBackingDiskWithDiskless(rsc, storPool);
            }
            else
            {
                if (storPoolNameStr == null || "".equals(storPoolNameStr))
                {
                    storPoolNameStr = defaultStorPoolName;
                }
                storPool = rsc.getAssignedNode().getStorPool(
                    apiCtx,
                    LinstorParsingUtils.asStorPoolName(storPoolNameStr)
                );

                responses.addEntries(warnAndFlagDiskless(rsc, storPool));
            }

            checkStorPoolLoaded(rsc, storPool, storPoolNameStr, vlmDfn);
        }
        catch (InvalidKeyException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }

        return new ApiCallRcWith<>(responses, storPool);
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
            isDiskless = rsc.getStateFlags().isSet(apiCtx, Resource.RscFlags.DISKLESS);
        }
        catch (AccessDeniedException implError)
        {
            throw new ImplementationError(implError);
        }
        return isDiskless;
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
                    "for resource '" + rsc.getDefinition().getName().displayValue + "' " +
                    "for volume number '" + vlmDfn.getVolumeNumber().value + "' " +
                    "is not deployed on node '" + rsc.getAssignedNode().getName().displayValue + "'.")
                .setDetails("The resource which should be deployed had at least one volume definition " +
                    "(volume number '" + vlmDfn.getVolumeNumber().value + "') which LinStor " +
                    "tried to automatically create. " +
                    "The storage pool name for this new volume was looked for in order in " +
                    "the properties of the resource, volume definition, resource definition and node, " +
                    "and finally in a system wide default storage pool name defined by " +
                    "the LinStor controller.")
                .build(),
                new LinStorException("Dependency not found")
            );
        }
    }

    private void checkBackingDiskWithDiskless(final Resource rsc, final StorPool storPool)
    {
        if (storPool != null && storPool.getDriverKind().hasBackingStorage())
        {
            throw new ApiRcException(ApiCallRcImpl
                .entryBuilder(FAIL_INVLD_STOR_POOL_NAME,
                              "Storage pool with backing disk not allowed with diskless resource.")
                .setCause(String.format("Resource '%s' flagged as diskless, but a storage pool '%s' " +
                        "with backing disk was specified.",
                    rsc.getDefinition().getName().displayValue,
                    storPool.getName().displayValue))
                .setCorrection("Use a storage pool with a diskless driver or remove the diskless flag.")
                .build(),
                new LinStorException("Incorrect storage pool used.")
            );
        }
    }

    private ApiCallRc warnAndFlagDiskless(Resource rsc, final StorPool storPool)
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        if (storPool != null && !storPool.getDriverKind().hasBackingStorage())
        {
            responses.addEntry(ApiCallRcImpl
                .entryBuilder(
                    MASK_WARN | MASK_STOR_POOL,
                    "Resource will be automatically flagged diskless."
                )
                .setCause(String.format("Used storage pool '%s' is diskless, " +
                    "but resource was not flagged diskless", storPool.getName().displayValue))
                .build()
            );
            try
            {
                rsc.getStateFlags().enableFlags(apiCtx, Resource.RscFlags.DISKLESS);
            }
            catch (AccessDeniedException exc)
            {
                throw new ImplementationError(exc);
            }
            catch (SQLException exc)
            {
                throw new ApiSQLException(exc);
            }
        }

        return responses;
    }
}
