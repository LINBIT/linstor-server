package com.linbit.linstor.storage.layer.adapter.drbd;

import com.linbit.ImplementationError;
import com.linbit.drbd.md.AlStripesException;
import com.linbit.drbd.md.MaxAlSizeException;
import com.linbit.drbd.md.MaxSizeException;
import com.linbit.drbd.md.MetaData;
import com.linbit.drbd.md.MinAlSizeException;
import com.linbit.drbd.md.MinSizeException;
import com.linbit.drbd.md.PeerCountException;
import com.linbit.extproc.ExtCmdFailedException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceData;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.Resource.RscFlags;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.ResourceType;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.Volume;
import com.linbit.linstor.Volume.VlmFlags;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.devmgr.DeviceHandler2;
import com.linbit.linstor.drbdstate.DrbdConnection;
import com.linbit.linstor.drbdstate.DrbdResource;
import com.linbit.linstor.drbdstate.DrbdStateStore;
import com.linbit.linstor.drbdstate.DrbdStateTracker;
import com.linbit.linstor.drbdstate.DrbdVolume;
import com.linbit.linstor.drbdstate.DrbdVolume.DiskState;
import com.linbit.linstor.drbdstate.NoInitialStateException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.storage.layer.ResourceLayer;
import com.linbit.linstor.storage.layer.adapter.drbd.helper.ReadyForPrimaryNotifier;
import com.linbit.linstor.storage.layer.adapter.drbd.utils.ConfFileBuilder;
import com.linbit.linstor.storage.layer.adapter.drbd.utils.DrbdAdm;
import com.linbit.linstor.storage.layer.adapter.drbd.utils.MdSuperblockBuffer;
import com.linbit.linstor.storage.layer.exceptions.ResourceException;
import com.linbit.linstor.storage.layer.exceptions.VolumeException;
import com.linbit.linstor.storage.utils.ResourceUtils;
import com.linbit.linstor.storage.utils.VolumeUtils;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.utils.AccessUtils;

import static com.linbit.linstor.storage.utils.VolumeUtils.getBackingVolume;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Singleton
public class DrbdLayer implements ResourceLayer
{
    private static final String DRBD_CONFIG_SUFFIX = ".res";
    private static final String DRBD_CONFIG_TMP_SUFFIX = ".res_tmp";

    private static final long HAS_VALID_STATE_FOR_PRIMARY_TIMEOUT = 2000;

    private final AccessContext workerCtx;
    private final Provider<NotificationListener> notificationListenerProvider;
    private final DrbdAdm drbdUtils;
    private final DrbdStateStore drbdState;
    private final ErrorReporter errorReporter;
    private final WhitelistProps whitelistProps;
    private final CtrlStltSerializer interComSerializer;
    private final ControllerPeerConnector controllerPeerConnector;
    private final Provider<DeviceHandler2> resourceProcessorProvider;
    private final Provider<TransactionMgr> transMgrProvider;

    // Number of activity log stripes for DRBD meta data; this should be replaced with a property of the
    // resource definition, a property of the volume definition, or otherwise a system-wide default
    public static final int FIXME_AL_STRIPES = 1;

    // Number of activity log stripes; this should be replaced with a property of the resource definition,
    // a property of the volume definition, or or otherwise a system-wide default
    public static final long FIXME_AL_STRIPE_SIZE = 32;

    @Inject
    public DrbdLayer(
        @DeviceManagerContext AccessContext workerCtxRef,
        Provider<NotificationListener> notificationListenerProviderRef,
        DrbdAdm drbdUtilsRef,
        DrbdStateStore drbdStateRef,
        ErrorReporter errorReporterRef,
        WhitelistProps whiltelistPropsRef,
        CtrlStltSerializer interComSerializerRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        Provider<DeviceHandler2> resourceProcessorRef,
        Provider<TransactionMgr> transactionMgrProviderRef
    )
    {
        workerCtx = workerCtxRef;
        notificationListenerProvider = notificationListenerProviderRef;
        drbdUtils = drbdUtilsRef;
        drbdState = drbdStateRef;
        errorReporter = errorReporterRef;
        whitelistProps = whiltelistPropsRef;
        interComSerializer = interComSerializerRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        resourceProcessorProvider = resourceProcessorRef;
        transMgrProvider = transactionMgrProviderRef;
    }

    @Override
    public String getName()
    {
        return this.getClass().getSimpleName();
    }

    @Override
    public void prepare(List<Resource> rscs, List<Snapshot> snapshots)
        throws StorageException, AccessDeniedException, SQLException
    {
        try
        {
            for (Resource rsc : rscs)
            {
                Resource parent = rsc.getParentResource(workerCtx);
                Resource defaultRsc = getDefaultResource(parent);

                if (defaultRsc.isCreatePrimary())
                {
                    ((ResourceData) rsc).setCreatePrimary();
                }

                // update resource's layer data
                if (rsc.getLayerData(workerCtx) == null)
                {
                    rsc.setLayerData(
                        workerCtx,
                        new DrbdRscDataStlt(
                            rsc.getNodeId(),
                            defaultRsc.isDiskless(workerCtx),
                            defaultRsc.disklessForPeers(workerCtx)
                        )
                    );
                }

                String peerSlotsProp = rsc.getProps(workerCtx).getProp(ApiConsts.KEY_PEER_SLOTS);
                // Property is checked when the API sets it; if it still throws for whatever reason, it is logged
                // as an unexpected exception in dispatchResource()
                short peerSlots = peerSlotsProp == null ?
                    InternalApiConsts.DEFAULT_PEER_SLOTS : Short.parseShort(peerSlotsProp);
                ResourceDefinition rscDfn = rsc.getDefinition();

                // currently there are no drbdVolumeLayerData to update

                // update resource definition's layer data
                if (rscDfn.getLayerData(workerCtx, DrbdRscDfnDataStlt.class) == null)
                {
                    rscDfn.setLayerData(
                        workerCtx,
                        new DrbdRscDfnDataStlt(
                            rscDfn.getPort(workerCtx),
                            rscDfn.getTransportType(workerCtx),
                            rscDfn.getSecret(workerCtx),
                            transMgrProvider
                        )
                    );
                }

                // update volume definitions' layer data
                rscDfn.streamVolumeDfn(workerCtx).forEach(
                    vlmDfn ->
                    {
                        try
                        {
                            if (vlmDfn.getLayerData(workerCtx, DrbdVlmDfnDataStlt.class) == null)
                            {
                                vlmDfn.setLayerData(
                                    workerCtx,
                                    new DrbdVlmDfnDataStlt(
                                        vlmDfn.getMinorNr(workerCtx),
                                        peerSlots,
                                        transMgrProvider
                                    )
                                );
                            }
                        }
                        catch (AccessDeniedException exc)
                        {
                            throw new ImplementationError(exc);
                        }
                    }
                );

            }
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private Resource getDefaultResource(Resource defaultRsc) throws AccessDeniedException
    {
        while (defaultRsc.getType() != ResourceType.DEFAULT)
        {
            defaultRsc = defaultRsc.getParentResource(workerCtx);
        }
        return defaultRsc;
    }

    @Override
    public void updateGrossSize(Volume drbdVlm, Volume parentVlm) throws AccessDeniedException, SQLException
    {
        try
        {
            String peerSlotsProp = drbdVlm.getResource().getProps(workerCtx).getProp(ApiConsts.KEY_PEER_SLOTS);
            // Property is checked when the API sets it; if it still throws for whatever reason, it is logged
            // as an unexpected exception in dispatchResource()
            short peerSlots = peerSlotsProp == null ?
                InternalApiConsts.DEFAULT_PEER_SLOTS : Short.parseShort(peerSlotsProp);

            long netSize = parentVlm.getAllocatedSize(workerCtx);
            long grossSize = new MetaData().getGrossSize(
                netSize,
                peerSlots,
                DrbdLayer.FIXME_AL_STRIPES,
                DrbdLayer.FIXME_AL_STRIPE_SIZE
            );

            drbdVlm.setUsableSize(workerCtx, netSize);
            drbdVlm.setAllocatedSize(workerCtx, grossSize);
        }
        catch (InvalidKeyException | IllegalArgumentException | MinSizeException | MaxSizeException |
            MinAlSizeException | MaxAlSizeException | AlStripesException | PeerCountException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    @Override
    public void clearCache()
    {
        // no-op
    }

    @Override
    public void process(Resource rsc, Collection<Snapshot> snapshots, ApiCallRcImpl apiCallRc)
        throws StorageException, ResourceException, VolumeException, AccessDeniedException, SQLException
    {
        ResourceName resourceName = rsc.getDefinition().getName();
        if (rsc.getProps(workerCtx).map().containsKey(ApiConsts.KEY_RSC_ROLLBACK_TARGET))
        {
            /*
             *  snapshot rollback:
             *  - delete drbd
             *  - rollback snapshot
             *  - start drbd
             */
            deleteDrbd(rsc);
            processChild(rsc, snapshots, apiCallRc);
            adjustDrbd(rsc, snapshots, apiCallRc, true);
        }
        else
        if (
            rsc.getDefinition().isDown(workerCtx) ||
            rsc.getStateFlags().isSet(workerCtx, RscFlags.DELETE)
        )
        {
            deleteDrbd(rsc);
            addDeletedMsg(resourceName, apiCallRc);

            processChild(rsc, snapshots, apiCallRc);

            //TODO: remove this .delete call once not only default-resources are
            // iterated in the deviceHandler but also (drbd-) typed
            rsc.delete(workerCtx);
        }
        else
        {
            adjustDrbd(rsc, snapshots, apiCallRc, false);
            addAdjustedMsg(resourceName, apiCallRc);
        }
    }

    private void addDeletedMsg(ResourceName resourceName, ApiCallRcImpl apiCallRc)
    {
        apiCallRc.addEntry(
            ApiCallRcImpl.simpleEntry(
                ApiConsts.MASK_RSC | ApiConsts.DELETED,
                "Resource '" +  resourceName + "' [DRBD] deleted."
            )
        );
    }

    private void addAdjustedMsg(ResourceName resourceName, ApiCallRcImpl apiCallRc)
    {
        apiCallRc.addEntry(
            ApiCallRcImpl.simpleEntry(
                ApiConsts.MASK_RSC | ApiConsts.MODIFIED,
                "Resource '" +  resourceName + "' [DRBD] adjusted."
            )
        );
    }

    private void processChild(Resource rsc, Collection<Snapshot> snapshots, ApiCallRcImpl apiCallRc)
        throws AccessDeniedException, StorageException, ResourceException, VolumeException, SQLException
    {
        if (!rsc.isDiskless(workerCtx))
        {
            resourceProcessorProvider.get().process(ResourceUtils.getSingleChild(rsc, workerCtx), snapshots, apiCallRc);
        }
    }

    /**
     * Deletes a given DRBD resource, by calling {@code drbdadm down <resource-name>} and deleting
     * the resource specific .res file
     * {@link Resource#delete(AccessContext)} is also called on the given resource
     *
     * @param drbdRsc
     * @throws StorageException
     * @throws SQLException
     * @throws AccessDeniedException
     */
    private void deleteDrbd(Resource drbdRsc) throws StorageException
    {
        ResourceName rscName = drbdRsc.getDefinition().getName();

        try
        {
            errorReporter.logTrace("Shutting down drbd resource %s", rscName);
            drbdUtils.down(rscName);
            Path resFile = asResourceFile(rscName, false);
            errorReporter.logTrace("Deleting res file: %s ", resFile);
            Files.deleteIfExists(resFile);
            notificationListenerProvider.get().notifyResourceDeleted(drbdRsc);

            // TODO: delete all drbd-volumes once the API is split into
            // "notifyVolumeDeleted(Volume)" and "notifyStorageVolumeDeleted(Volume, long)"
        }
        catch (ExtCmdFailedException cmdExc)
        {
            throw new StorageException(
                "Shutdown of the DRBD resource '" + rscName.displayValue + " failed",
                getAbortMsg(rscName),
                "The external command for stopping the DRBD resource failed",
                "- Check whether the required software is installed\n" +
                    "- Check whether the application's search path includes the location\n" +
                    "  of the external software\n" +
                    "- Check whether the application has execute permission for the external command\n",
                    null,
                    cmdExc
            );
        }
        catch (IOException exc)
        {
            throw new StorageException("IOException while removing resource file", exc);
        }
    }

    /**
     * Adjusts (creates or modifies) a given DRBD resource
     * @param drbdRsc
     * @param snapshots
     * @param apiCallRc
     * @param childAlreadyProcessed
     * @throws SQLException
     * @throws StorageException
     * @throws AccessDeniedException
     * @throws VolumeException
     * @throws ResourceException
     */
    private void adjustDrbd(
        Resource drbdRsc,
        Collection<Snapshot> snapshots,
        ApiCallRcImpl apiCallRc,
        boolean childAlreadyProcessed
    )
        throws AccessDeniedException, StorageException, SQLException,
            ResourceException, VolumeException
    {
        DrbdRscDataStlt rscLinState = (DrbdRscDataStlt) drbdRsc.getLayerData(workerCtx);

        rscLinState.requiresAdjust = isAdjustRequired(drbdRsc);

        if (rscLinState.requiresAdjust)
        {
            /*
             *  we have to split here into several steps:
             *  - first we have to detach all volumes marked for deletion and delete the DRBD-volumes
             *  - suspend IO if required by a snapshot
             *  - call the underlying layer's process method
             *  - create metaData for new volumes
             *  -- check which volumes are new
             *  -- render all res files
             *  -- create-md only for new volumes (create-md needs already valid .res files)
             *  - adjust all remaining and newly created volumes
             *  - resume IO if allowed by all snapshots
             */

            List<Volume> checkMetaData = detachVolumesIfNecessary(drbdRsc);

            adjustSuspendIo(drbdRsc, snapshots);

            if (!childAlreadyProcessed)
            {
                processChild(drbdRsc, snapshots, apiCallRc);
            }

            updateResourceToCurrentDrbdState(drbdRsc);

            // hasMetaData needs to be run after child-resource processed
            List<Volume> createMetaData = new ArrayList<>();
            for (Volume drbdVlm : checkMetaData)
            {
                if (!hasMetaData(drbdVlm))
                {
                    createMetaData.add(drbdVlm);
                }
            }

            regenerateResFile(drbdRsc);

            // createMetaData needs rendered resFile
            for (Volume drbdVlm : createMetaData)
            {
                VolumeDefinition drbdVlmDfn = drbdVlm.getVolumeDefinition();

                DrbdVlmDataStlt vlmState = (DrbdVlmDataStlt) drbdVlm.getLayerData(workerCtx);
                createMetaData(drbdRsc, drbdVlmDfn, vlmState);
            }

            try
            {
                drbdUtils.adjust(
                    drbdRsc.getDefinition().getName(),
                    false,
                    false,
                    false,
                    null
                );
                rscLinState.requiresAdjust = false;

                condInitialOrSkipSync(drbdRsc, rscLinState);
            }
            catch (ExtCmdFailedException exc)
            {
                throw new ResourceException(
                    String.format("Failed to adjust DRBD resource %s", drbdRsc.getDefinition().getName()),
                    exc
                );
            }
        }
    }

    private boolean isAdjustRequired(Resource drbdRsc)
    {
        return true; // TODO could be improved :)
    }

    private List<Volume> detachVolumesIfNecessary(Resource drbdRsc)
        throws AccessDeniedException, SQLException, StorageException
    {
        List<Volume> checkMetaData = new ArrayList<>();
        if (!drbdRsc.isDiskless(workerCtx))
        {
            // using a dedicated list to prevent concurrentModificationException
            List<Volume> volumesToDetach = new ArrayList<>();

            Iterator<Volume> vlmsIt = drbdRsc.iterateVolumes();
            while (vlmsIt.hasNext())
            {
                Volume drbdVlm = vlmsIt.next();
                if (drbdVlm.getFlags().isSet(workerCtx, VlmFlags.DELETE))
                {
                    volumesToDetach.add(drbdVlm);
                }
                else
                {
                    checkMetaData.add(drbdVlm);
                }
            }
            for (Volume drbdVlm : volumesToDetach)
            {
                detachDrbdVolume(drbdVlm);
            }
        }
        return checkMetaData;
    }

    private void detachDrbdVolume(Volume drbdVlm) throws AccessDeniedException, SQLException, StorageException
    {
        ResourceName rscName = drbdVlm.getResourceDefinition().getName();
        VolumeNumber vlmNr = drbdVlm.getVolumeDefinition().getVolumeNumber();

        errorReporter.logTrace("Detaching volume %s/%d", rscName, vlmNr.value);
        try
        {
            drbdUtils.detach(rscName, vlmNr);
        }
        catch (ExtCmdFailedException exc)
        {
            throw new StorageException(
                String.format(
                    "Failed to detach DRBD volume %s/%d",
                    drbdVlm.getResourceDefinition().getName(),
                    drbdVlm.getVolumeDefinition().getVolumeNumber().value
                ),
                exc
            );
        }
        // only deletes the drbd-volume, not the storage volume
        drbdVlm.delete(workerCtx);
    }

    private void adjustSuspendIo(Resource drbdRsc, Collection<Snapshot> snapshots)
        throws ResourceException, AccessDeniedException
    {
        ResourceName rscName = drbdRsc.getDefinition().getName();

        DrbdRscDataStlt rscLinState = (DrbdRscDataStlt) drbdRsc.getLayerData(workerCtx);

        boolean shouldSuspend = snapshots.stream()
            .anyMatch(snap ->
                AccessUtils.execPrivileged(() -> snap.getSuspendResource(workerCtx))
            );

        if (!rscLinState.isSuspended && shouldSuspend)
        {
            try
            {
                errorReporter.logTrace("Suspending DRBD-IO for resource '%s'", rscName.displayValue);
                drbdUtils.suspendIo(rscName);
            }
            catch (ExtCmdFailedException exc)
            {
                throw new ResourceException(
                    "Suspend of the DRBD resource '" + rscName.displayValue + " failed",
                    getAbortMsg(rscName),
                    "The external command for suspending the DRBD resource failed",
                    null,
                    null,
                    exc
                );
            }
        }
        else
        if (rscLinState.isSuspended && !shouldSuspend)
        {
            try
            {
                errorReporter.logTrace("Resuming DRBD-IO for resource '%s'", rscName.displayValue);
                drbdUtils.resumeIo(rscName);
            }
            catch (ExtCmdFailedException exc)
            {
                throw new ResourceException(
                    "Resume of the DRBD resource '" + rscName.displayValue + " failed",
                    getAbortMsg(rscName),
                    "The external command for resuming the DRBD resource failed",
                    null,
                    null,
                    exc
                );
            }
        }
    }

    private boolean hasMetaData(Volume drbdVlm)
        throws StorageException, AccessDeniedException, SQLException, VolumeException
    {
        Volume backingVlm = getBackingVolume(workerCtx, drbdVlm);
        if (backingVlm == null)
        {
            throw new ImplementationError(
                // error in controller / LayeredResourceHandler
                String.format(
                    "DRBD volume %s/%d has no backing volume",
                    drbdVlm.getResourceDefinition().getName(),
                    drbdVlm.getVolumeDefinition().getVolumeNumber().value
                )
            );
        }

        DrbdVlmDataStlt vlmState = (DrbdVlmDataStlt) drbdVlm.getLayerData(workerCtx);

        VolumeDefinition drbdVlmDfn = drbdVlm.getVolumeDefinition();
        DrbdVlmDfnDataStlt vlmDfnState = drbdVlmDfn.getLayerData(
            workerCtx,
            DrbdVlmDfnDataStlt.class
        );

        String backingDiskPath = backingVlm.getDevicePath(workerCtx);
        if (backingDiskPath == null)
        {
            throw new VolumeException(
                String.format(
                    "Drbd volume %s/%d's backing volume (%s) has no backing disk!",
                    drbdVlm.getResourceDefinition().getName(),
                    drbdVlm.getVolumeDefinition().getVolumeNumber().value,
                    backingVlm.toString()
                )
            );
        }
        drbdVlm.setBackingDiskPath(workerCtx, backingDiskPath);

        MdSuperblockBuffer mdUtils = new MdSuperblockBuffer();
        try
        {
            mdUtils.readObject(backingDiskPath);
        }
        catch (IOException exc)
        {
            throw new VolumeException(
                String.format(
                    "Failed to access DRBD super-block of volume %s/%d",
                    drbdVlm.getResourceDefinition().getName(),
                    drbdVlm.getVolumeDefinition().getVolumeNumber().value
                ),
                exc
            );
        }

        boolean hasMetaData;

        if (vlmState.checkMetaData)
        {
            if (mdUtils.hasMetaData())
            {
                boolean isMetaDataCorrupt;
                try
                {
                    isMetaDataCorrupt = !drbdUtils.hasMetaData(
                        backingDiskPath,
                        vlmDfnState.getMinorNr().value,
                        "internal"
                    );
                }
                catch (ExtCmdFailedException exc)
                {
                    throw new VolumeException(
                        String.format(
                            "Failed to check DRBD meta-data integrety of volume %s/%d",
                            drbdVlm.getResourceDefinition().getName(),
                            drbdVlm.getVolumeDefinition().getVolumeNumber().value
                        ),
                        exc
                    );
                }
                if (isMetaDataCorrupt)
                {
                    throw new VolumeException(
                        "Corrupted drbd-metadata",
                        null,
                        "Linstor has found existing DRBD meta data, " +
                            "but drbdmeta could not read them",
                        "Check if the DRBD-utils version match the DRBD kernel version. ",
                        null
                    );
                }
                else
                {
                    hasMetaData = true;
                }
            }
            else
            {
                hasMetaData = false;
            }
        }
        else
        {
            hasMetaData = true; // just dont create new meta-data if "checkMetaData" is disabled
        }
        return hasMetaData;
    }

    private void createMetaData(Resource rsc, VolumeDefinition drbdVlmDfn, DrbdVlmDataStlt vlmState)
        throws AccessDeniedException, StorageException, ImplementationError, VolumeException
    {
        VolumeNumber vlmNr = drbdVlmDfn.getVolumeNumber();
        try
        {
            drbdUtils.createMd(
                rsc.getDefinition().getName(),
                vlmNr,
                vlmState.peerSlots
            );
            vlmState.metaDataIsNew = true;

            if (VolumeUtils.isVolumeThinlyBacked(workerCtx, rsc.getVolume(vlmNr)))
            {
                ResourceDefinition rscDfn = rsc.getDefinition();

                String currentGi = null;
                try
                {
                    currentGi = drbdVlmDfn.getProps(workerCtx).getProp(ApiConsts.KEY_DRBD_CURRENT_GI);
                }
                catch (InvalidKeyException invKeyExc)
                {
                    throw new ImplementationError(
                        "API constant contains an invalid key",
                        invKeyExc
                    );
                }
                if (currentGi == null)
                {
                    throw new StorageException(
                        "Meta data creation for resource '" + rscDfn.getName().displayValue + "' volume " +
                        vlmNr.value + " failed",
                        getAbortMsg(rscDfn.getName(), vlmNr),
                        "Volume " + vlmNr.value + " of the resource uses a thin provisioning storage driver,\n" +
                        "but no initial value for the DRBD current generation is set on the volume definition",
                        "- Ensure that the initial DRBD current generation is set on the volume definition\n" +
                        "or\n" +
                        "- Recreate the volume definition",
                        "The key of the initial DRBD current generation property is:\n" +
                        ApiConsts.KEY_DRBD_CURRENT_GI,
                        null
                    );
                }
                drbdUtils.setGi(
                    rsc.getNodeId(),
                    drbdVlmDfn.getMinorNr(workerCtx),
                    rsc.getVolume(vlmNr).getBackingDiskPath(workerCtx),
                    currentGi,
                    null,
                    true
                );
            }
        }
        catch (ExtCmdFailedException exc)
        {
            throw new VolumeException(
                String.format(
                    "Failed to create meta-data for DRBD volume %s/%d",
                    rsc.getDefinition().getName(),
                    drbdVlmDfn.getVolumeNumber().value
                ),
                exc
            );
        }
    }

    private void updateResourceToCurrentDrbdState(Resource rsc)
        throws AccessDeniedException, SQLException, StorageException
    {
        try
        {
            errorReporter.logTrace(
                "Synchronizing Linstor-state with DRBD-state for resource %s",
                rsc.getDefinition().getName()
            );
            DrbdRscDataStlt rscState  = (DrbdRscDataStlt) rsc.getLayerData(workerCtx);

            DrbdResource drbdRscState = drbdState.getDrbdResource(rsc.getDefinition().getName().displayValue);

            if (drbdRscState == null)
            {
                rscState.exists = false;
            }
            else
            {
                rscState.exists = true;

                // FIXME: Temporary fix: If the NIC selection property on a storage pool is changed retrospectively,
                //        then rewriting the DRBD resource configuration file and 'drbdadm adjust' is required,
                //        but there is not yet a mechanism to notify the device handler to perform an adjust action.
                rscState.requiresAdjust = true;

                { // check drbdRole
                    DrbdResource.Role rscRole = drbdRscState.getRole();
                    if (rscRole == DrbdResource.Role.UNKNOWN)
                    {
                        rscState.requiresAdjust = true;
                    }
                    else
                    if (rscRole == DrbdResource.Role.PRIMARY)
                    {
                        rscState.isPrimary = true;
                    }
                }

                { // check drbd connections
                    rsc.getDefinition().streamResource(workerCtx)
                        .filter(otherRsc -> !otherRsc.equals(rsc))
                        .forEach(
                            otherRsc ->
                                {
                                    DrbdConnection drbdConn = drbdRscState.getConnection(
                                        otherRsc.getAssignedNode().getName().displayValue
                                    );
                                    if (drbdConn != null)
                                    {
                                        DrbdConnection.State connState = drbdConn.getState();
                                        switch (connState)
                                        {
                                            case STANDALONE:
                                                // fall-through
                                            case DISCONNECTING:
                                                // fall-through
                                            case UNCONNECTED:
                                                // fall-through
                                            case TIMEOUT:
                                                // fall-through
                                            case BROKEN_PIPE:
                                                // fall-through
                                            case NETWORK_FAILURE:
                                                // fall-through
                                            case PROTOCOL_ERROR:
                                                // fall-through
                                            case TEAR_DOWN:
                                                // fall-through
                                            case UNKNOWN:
                                                // fall-through
                                                rscState.requiresAdjust = true;
                                                break;
                                            case CONNECTING:
                                                break;
                                            case CONNECTED:
                                                break;
                                            default:
                                                throw new ImplementationError(
                                                    "Missing switch case for enumeration value '" +
                                                    connState.name() + "'",
                                                    null
                                                );
                                        }
                                    }
                                    else
                                    {
                                        // Missing connection
                                        rscState.requiresAdjust = true;
                                    }
                                }
                        );
                }

                String peerSlotsProp = rsc.getProps(workerCtx).getProp(ApiConsts.KEY_PEER_SLOTS);
                // Property is checked when the API sets it; if it still throws for whatever reason, it is logged as an
                // unexpected exception in dispatchResource()
                short peerSlots = peerSlotsProp == null ?
                    InternalApiConsts.DEFAULT_PEER_SLOTS : Short.parseShort(peerSlotsProp);

                Map<VolumeNumber, DrbdVolume> drbdVolumes = drbdRscState.getVolumesMap();

                Iterator<Volume> vlmIter = rsc.iterateVolumes();
                while (vlmIter.hasNext())
                {
                    Volume vlm = vlmIter.next();
                    if (!vlm.getFlags().isSet(workerCtx, VlmFlags.DELETE))
                    {
                        Volume backingVlm;
                        try
                        {
                            backingVlm = getBackingVolume(workerCtx, vlm);
                        }
                        catch (StorageException exc)
                        {
                            throw new ImplementationError(exc);
                        }

                        DrbdVlmDataStlt vlmState = (DrbdVlmDataStlt) vlm.getLayerData(workerCtx);

                        { // check drbd-volume
                            DrbdVolume drbdVlmState = drbdVolumes.remove(vlm.getVolumeDefinition().getVolumeNumber());
                            if (drbdVlmState != null)
                            {
                                vlmState.exists = true;
                                DiskState diskState = drbdVlmState.getDiskState();
                                vlmState.diskState = diskState.toString();
                                switch (diskState)
                                {
                                    case DISKLESS:
                                        if (!drbdVlmState.isClient())
                                        {
                                            vlmState.failed = true;
                                            rscState.requiresAdjust = true;
                                        }
                                        else
                                        {
                                            vlmState.checkMetaData = false;
                                        }
                                        break;
                                    case DETACHING:
                                        // TODO: May be a transition from storage to client
                                        // fall-through
                                    case FAILED:
                                        vlmState.failed = true;
                                        // fall-through
                                    case NEGOTIATING:
                                        // fall-through
                                    case UNKNOWN:
                                        // The local disk state should not be unknown,
                                        // try adjusting anyways
                                        rscState.requiresAdjust = true;
                                        break;
                                    case UP_TO_DATE:
                                        // fall-through
                                    case CONSISTENT:
                                        // fall-through
                                    case INCONSISTENT:
                                        // fall-through
                                    case OUTDATED:
                                        vlmState.hasMetaData = true;
                                        // No additional check for existing meta data is required
                                        vlmState.checkMetaData = false;
                                        // fall-through
                                    case ATTACHING:
                                        vlmState.hasDisk = true;
                                        break;
                                    default:
                                        throw new ImplementationError(
                                            "Missing switch case for enumeration value '" +
                                            diskState.name() + "'",
                                            null
                                        );
                                }
                            }
                            else
                            {
                                // Missing volume, adjust the resource
                                rscState.requiresAdjust = true;
                            }
                        }

                        vlmState.metaDataIsNew = false;
                        if (rsc.getStateFlags().isSet(workerCtx, RscFlags.DISKLESS))
                        {
                            vlmState.checkMetaData = false;
                        }
                        else
                        {
                            vlmState.peerSlots = peerSlots;
                            vlmState.alStripes = FIXME_AL_STRIPES;
                            vlmState.alStripeSize = FIXME_AL_STRIPE_SIZE;
                        }
                        rscState.putVlmState(vlm.getVolumeDefinition().getVolumeNumber(), vlmState);
                    }
                }
                if (!drbdVolumes.isEmpty())
                {
                    // The DRBD resource has additional unknown volumes,
                    // adjust the resource
                    rscState.requiresAdjust = true;
                }

                rscState.isSuspended =
                    drbdRscState.getSuspendedUser() == null ?
                        false :
                        drbdRscState.getSuspendedUser();
            }
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError("Invalid hardcoded key", exc);
        }
        catch (IllegalArgumentException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (NoInitialStateException exc)
        {
            throw new StorageException("Need initial DRBD state", exc);
        }
    }

    private void regenerateResFile(Resource localRsc) throws AccessDeniedException, StorageException
    {
        ResourceName rscName = localRsc.getDefinition().getName();
        Path resFile = asResourceFile(rscName, false);
        Path tmpResFile = asResourceFile(rscName, true);

        List<Resource> peerResources = localRsc.getDefinition().streamResource(workerCtx)
            .filter(otherRsc -> otherRsc.getType().equals(ResourceType.DRBD) && !otherRsc.equals(localRsc))
            .collect(Collectors.toList());

        String content = new ConfFileBuilder(
            errorReporter,
            workerCtx,
            localRsc,
            peerResources,
            whitelistProps
        ).build();

        try (FileOutputStream resFileOut = new FileOutputStream(tmpResFile.toFile()))
        {
            resFileOut.write(content.getBytes());
        }
        catch (IOException ioExc)
        {
            String ioErrorMsg = ioExc.getMessage();
            if (ioErrorMsg == null)
            {
                ioErrorMsg = "The runtime environment or operating system did not provide a description of " +
                    "the I/O error";
            }
            throw new StorageException(
                "Creation of the DRBD configuration file for resource '" + rscName.displayValue +
                    "' failed due to an I/O error",
                getAbortMsg(rscName),
                "Creation of the DRBD configuration file failed due to an I/O error",
                "- Check whether enough free space is available for the creation of the file\n" +
                    "- Check whether the application has write access to the target directory\n" +
                    "- Check whether the storage is operating flawlessly",
                "The error reported by the runtime environment or operating system is:\n" + ioErrorMsg,
                ioExc
            );
        }

        try
        {
            drbdUtils.checkResFile(rscName, tmpResFile, resFile);
        }
        catch (ExtCmdFailedException exc)
        {
            String errMsg = exc.getMessage();
            throw new StorageException(
                "Generated resource file for resource '" + rscName.displayValue + "' is invalid.",
                getAbortMsg(rscName),
                "Verification of resource file failed",
                null,
                "The error reported by the runtime environment or operating system is:\n" + errMsg,
                exc
            );
        }

        try
        {
            Files.move(
                tmpResFile,
                resFile,
                StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE
            );
        }
        catch (IOException ioExc)
        {
            String ioErrorMsg = ioExc.getMessage();
            throw new StorageException(
                "Unable to move temporary DRBD resource file '" + tmpResFile.toString() + "' to resource directory.",
                getAbortMsg(rscName),
                "Unable to move temporary DRBD resource file due to an I/O error",
                "- Check whether enough free space is available for moving the file\n" +
                    "- Check whether the application has write access to the target directory\n" +
                    "- Check whether the storage is operating flawlessly",
                "The error reported by the runtime environment or operating system is:\n" + ioErrorMsg,
                ioExc
            );
        }
    }

    private void condInitialOrSkipSync(Resource rsc, DrbdRscDataStlt rscState)
        throws AccessDeniedException, StorageException
    {
        try
        {
            ResourceDefinition rscDfn = rsc.getDefinition();
            ResourceName rscName = rscDfn.getName();

            if (rscDfn.getProps(workerCtx).getProp(InternalApiConsts.PROP_PRIMARY_SET) == null &&
                rsc.getStateFlags().isUnset(workerCtx, Resource.RscFlags.DISKLESS))
            {
                boolean alreadyInitialized = !allVlmsMetaDataNew(rscState);
                errorReporter.logTrace("Requesting primary on %s; already initialized: %b",
                    rscName.getDisplayName(), alreadyInitialized);
                // Send a primary request even when volumes have already been initialized so that the controller can
                // save DrbdPrimarySetOn so that subsequently added nodes do not request to be primary
                sendRequestPrimaryResource(
                    rscDfn.getName().getDisplayName(),
                    rsc.getUuid().toString(),
                    alreadyInitialized
                );
            }
            else
            if (rsc.isCreatePrimary() && !rscState.isPrimary)
            {
                // First, skip the resync on all thinly provisioned volumes
                boolean haveFatVlm = false;
                Iterator<Volume> vlmIter = rsc.iterateVolumes();
                while (vlmIter.hasNext())
                {
                    Volume vlm = vlmIter.next();
                    if (!VolumeUtils.isVolumeThinlyBacked(workerCtx, vlm))
                    {
                        haveFatVlm = true;
                    }
                }

                // Set the resource primary (--force) to trigger an initial sync of all
                // fat provisioned volumes
                ((ResourceData) rsc).unsetCreatePrimary();
                ((ResourceData) getDefaultResource(rsc)).unsetCreatePrimary();
                if (haveFatVlm)
                {
                    errorReporter.logTrace("Setting resource primary on %s", rscName.getDisplayName());
                    setResourcePrimary(rsc);
                }
            }
        }
        catch (InvalidKeyException invalidKeyExc)
        {
            throw new ImplementationError("Invalid hardcoded property key", invalidKeyExc);
        }
    }

    private boolean allVlmsMetaDataNew(DrbdRscDataStlt rscState)
    {
        return rscState.streamVolumeStates().allMatch(vlmState -> vlmState.metaDataIsNew);
    }

    private void setResourcePrimary(Resource rsc) throws StorageException
    {
        ResourceName rscName = rsc.getDefinition().getName();
        try
        {
            waitForValidStateForPrimary(rscName);

            drbdUtils.primary(rscName, true, false);
            // setting to secondary because of two reasons:
            // * bug in drbdsetup: cannot down a primary resource
            // * let the user choose which satellite should be primary (or let it be handled by auto-promote)
            drbdUtils.secondary(rscName);
        }
        catch (ExtCmdFailedException cmdExc)
        {
            throw new StorageException(
                "Starting the initial resync of the DRBD resource '" + rscName.getDisplayName() + " failed",
                getAbortMsg(rscName),
                "The external command for changing the DRBD resource's role failed",
                "- Check whether the required software is installed\n" +
                "- Check whether the application's search path includes the location\n" +
                "  of the external software\n" +
                "- Check whether the application has execute permission for the external command\n",
                null,
                cmdExc
            );
        }
    }

    private void waitForValidStateForPrimary(ResourceName rscName) throws StorageException
    {
        try
        {
            final Object syncObj = new Object();
            synchronized (syncObj)
            {
                ReadyForPrimaryNotifier resourceObserver = new ReadyForPrimaryNotifier(rscName.displayValue, syncObj);
                drbdState.addObserver(resourceObserver, DrbdStateTracker.OBS_DISK);
                if (!resourceObserver.hasValidStateForPrimary(drbdState.getDrbdResource(rscName.displayValue)))
                {
                    syncObj.wait(HAS_VALID_STATE_FOR_PRIMARY_TIMEOUT);
                }
                if (!resourceObserver.hasValidStateForPrimary(drbdState.getDrbdResource(rscName.displayValue)))
                {
                    throw new StorageException(
                        "Device did not get ready within " + HAS_VALID_STATE_FOR_PRIMARY_TIMEOUT + "ms"
                    );
                }
                drbdState.removeObserver(resourceObserver);
            }
        }
        catch (NoInitialStateException exc)
        {
            throw new StorageException("No initial drbd state", exc);
        }
        catch (InterruptedException exc)
        {
            throw new StorageException("Interrupted", exc);
        }
    }

    private void sendRequestPrimaryResource(
        final String rscName,
        final String rscUuid,
        boolean alreadyInitialized
    )
    {
        byte[] data = interComSerializer
            .onewayBuilder(InternalApiConsts.API_REQUEST_PRIMARY_RSC)
            .primaryRequest(rscName, rscUuid, alreadyInitialized)
            .build();

        controllerPeerConnector.getControllerPeer().sendMessage(data);
    }

    /*
     * DELETE method and its utilities
     */

    private Path asResourceFile(ResourceName rscName, boolean temp)
    {
        return Paths.get(
            CoreModule.CONFIG_PATH,
            rscName.displayValue + (temp ? DRBD_CONFIG_TMP_SUFFIX : DRBD_CONFIG_SUFFIX)
        );
    }

    private String getAbortMsg(ResourceName rscName)
    {
        return "Operations on resource '" + rscName.displayValue + "' were aborted";
    }

    private String getAbortMsg(ResourceName rscName, VolumeNumber vlmNr)
    {
        return "Operations on volume " + vlmNr.value + " of resource '" + rscName.displayValue + "' were aborted";
    }

    @Override
    public void setLocalNodeProps(Props localNodePropsRef)
    {
        // ignored
    }
}
