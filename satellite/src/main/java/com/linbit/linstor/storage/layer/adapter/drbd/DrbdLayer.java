package com.linbit.linstor.storage.layer.adapter.drbd;

import com.linbit.ImplementationError;
import com.linbit.drbd.md.AlStripesException;
import com.linbit.drbd.md.MaxAlSizeException;
import com.linbit.drbd.md.MaxSizeException;
import com.linbit.drbd.md.MetaData;
import com.linbit.drbd.md.MinAlSizeException;
import com.linbit.drbd.md.MinSizeException;
import com.linbit.drbd.md.PeerCountException;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.extproc.ExtCmdFailedException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.devmgr.DeviceHandler;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.drbdstate.DrbdConnection;
import com.linbit.linstor.drbdstate.DrbdEventPublisher;
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
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscObject.DrbdRscFlags;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.layer.DeviceLayer;
import com.linbit.linstor.storage.layer.adapter.drbd.helper.ReadyForPrimaryNotifier;
import com.linbit.linstor.storage.layer.adapter.drbd.utils.ConfFileBuilder;
import com.linbit.linstor.storage.layer.adapter.drbd.utils.DrbdAdm;
import com.linbit.linstor.storage.layer.adapter.drbd.utils.MdSuperblockBuffer;
import com.linbit.linstor.storage.layer.exceptions.ResourceException;
import com.linbit.linstor.storage.layer.exceptions.VolumeException;
import com.linbit.linstor.storage.utils.MkfsUtils;
import com.linbit.linstor.storage.utils.VolumeUtils;
import com.linbit.linstor.utils.layer.DrbdLayerUtils;
import com.linbit.utils.AccessUtils;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Singleton
public class DrbdLayer implements DeviceLayer
{
    public static final String DRBD_DEVICE_PATH_FORMAT = "/dev/drbd%d";
    private static final String DRBD_CONFIG_SUFFIX = ".res";
    private static final String DRBD_CONFIG_TMP_SUFFIX = ".res_tmp";

    private static final long HAS_VALID_STATE_FOR_PRIMARY_TIMEOUT = 2000;

    private final AccessContext workerCtx;
    private final DrbdAdm drbdUtils;
    private final DrbdStateStore drbdState;
    private final DrbdEventPublisher drbdEventPublisher;
    private final ErrorReporter errorReporter;
    private final WhitelistProps whitelistProps;
    private final CtrlStltSerializer interComSerializer;
    private final ControllerPeerConnector controllerPeerConnector;
    private final Provider<DeviceHandler> resourceProcessorProvider;
    private final ExtCmdFactory extCmdFactory;

    // Number of activity log stripes for DRBD meta data; this should be replaced with a property of the
    // resource definition, a property of the volume definition, or otherwise a system-wide default
    public static final int FIXME_AL_STRIPES = 1;

    // Number of activity log stripes; this should be replaced with a property of the resource definition,
    // a property of the volume definition, or or otherwise a system-wide default
    public static final long FIXME_AL_STRIPE_SIZE = 32;

    @Inject
    public DrbdLayer(
        @DeviceManagerContext AccessContext workerCtxRef,
        DrbdAdm drbdUtilsRef,
        DrbdStateStore drbdStateRef,
        DrbdEventPublisher drbdEventPublisherRef,
        ErrorReporter errorReporterRef,
        WhitelistProps whiltelistPropsRef,
        CtrlStltSerializer interComSerializerRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        Provider<DeviceHandler> resourceProcessorRef,
        ExtCmdFactory extCmdFactoryRef
    )
    {
        workerCtx = workerCtxRef;
        drbdUtils = drbdUtilsRef;
        drbdState = drbdStateRef;
        drbdEventPublisher = drbdEventPublisherRef;
        errorReporter = errorReporterRef;
        whitelistProps = whiltelistPropsRef;
        interComSerializer = interComSerializerRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        resourceProcessorProvider = resourceProcessorRef;
        extCmdFactory = extCmdFactoryRef;
    }

    @Override
    public String getName()
    {
        return this.getClass().getSimpleName();
    }

    @Override
    public void prepare(
        Set<AbsRscLayerObject<Resource>> rscDataList,
        Set<AbsRscLayerObject<Snapshot>> affectedSnapshots
    )
        throws StorageException, AccessDeniedException, DatabaseException
    {
        // no-op
    }

    @Override
    public void resourceFinished(AbsRscLayerObject<Resource> layerDataRef)
    {
        /*
         * Although the corresponding events2 event will also trigger the "resource created"
         * linstor event, we still trigger it here in case the resource already existed before
         * we did anything (migration).
         *
         * If we do not do that, the controller will wait for the resource-ready event, which should
         * be triggered by the events2. However, that events2 will not come, as we already received it
         * at startup of the satellite.
         */
        DrbdResource drbdResource;
        try
        {
            drbdResource = drbdState.getDrbdResource(layerDataRef.getSuffixedResourceName());
            if (drbdResource != null)
            {
                drbdEventPublisher.resourceCreated(drbdResource);
            }
        }
        catch (NoInitialStateException exc)
        {
            // we should not have been called
            throw new ImplementationError(exc);
        }
    }

    @Override
    public void updateGrossSize(VlmProviderObject<Resource> vlmData)
        throws AccessDeniedException, DatabaseException
    {
        try
        {
            DrbdVlmData<Resource> drbdVlmData = (DrbdVlmData<Resource>) vlmData;
            String peerSlotsProp = vlmData.getVolume().getAbsResource()
                .getProps(workerCtx).getProp(ApiConsts.KEY_PEER_SLOTS);
            // Property is checked when the API sets it; if it still throws for whatever reason, it is logged
            // as an unexpected exception in dispatchResource()
            short peerSlots = peerSlotsProp == null ?
                InternalApiConsts.DEFAULT_PEER_SLOTS : Short.parseShort(peerSlotsProp);

            long netSize = drbdVlmData.getUsableSize();

            boolean isDiskless = drbdVlmData.getRscLayerObject().getAbsResource().getStateFlags()
                .isSet(workerCtx, Resource.Flags.DRBD_DISKLESS);
            if (!isDiskless)
            {
                if (drbdVlmData.isUsingExternalMetaData())
                {
                    long extMdSize = new MetaData().getExternalMdSize(
                        netSize,
                        peerSlots,
                        DrbdLayer.FIXME_AL_STRIPES,
                        DrbdLayer.FIXME_AL_STRIPE_SIZE
                    );
                    drbdVlmData.setAllocatedSize(netSize + extMdSize); // rough estimation

                    drbdVlmData.getChildBySuffix(DrbdRscData.SUFFIX_DATA).setUsableSize(netSize);
                    VlmProviderObject<Resource> metaChild = drbdVlmData.getChildBySuffix(DrbdRscData.SUFFIX_META);
                    if (metaChild != null)
                    {
                        // is null if we are nvme-traget while the drbd-ext-metadata stays on the initiator side
                        metaChild.setUsableSize(extMdSize);
                    }
                }
                else
                {
                    long grossSize = new MetaData().getGrossSize(
                        netSize,
                        peerSlots,
                        DrbdLayer.FIXME_AL_STRIPES,
                        DrbdLayer.FIXME_AL_STRIPE_SIZE
                    );
                    drbdVlmData.setAllocatedSize(grossSize);
                    drbdVlmData.getChildBySuffix(DrbdRscData.SUFFIX_DATA).setUsableSize(grossSize);
                }
            }
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
    public LayerProcessResult process(
        AbsRscLayerObject<Resource> rscLayerData,
        List<Snapshot> snapshotList,
        ApiCallRcImpl apiCallRc
    )
        throws StorageException, ResourceException, VolumeException, AccessDeniedException, DatabaseException
    {
        DrbdRscData<Resource> drbdRscData = (DrbdRscData<Resource>) rscLayerData;

        Resource rsc = rscLayerData.getAbsResource();
        if (rsc.getProps(workerCtx).map().containsKey(ApiConsts.KEY_RSC_ROLLBACK_TARGET))
        {
            /*
             *  snapshot rollback:
             *  - delete drbd
             *  - rollback snapshot
             *  - start drbd
             */
            deleteDrbd(drbdRscData);
            if (processChild(drbdRscData, snapshotList, apiCallRc))
            {
                adjustDrbd(drbdRscData, snapshotList, apiCallRc, true);

                // this should not be executed if adjusting the drbd resource fails
                copyResFileToBackup(drbdRscData);
            }
        }
        else
        if (
            drbdRscData.getRscDfnLayerObject().isDown() ||
                rsc.getStateFlags().isSet(workerCtx, Resource.Flags.DELETE)
        )
        {
            deleteDrbd(drbdRscData);
            addDeletedMsg(drbdRscData, apiCallRc);

            processChild(drbdRscData, snapshotList, apiCallRc);

            // this should not be executed if deleting the drbd resource fails
            deleteBackupResFile(drbdRscData);
        }
        else
        {
            if (adjustDrbd(drbdRscData, snapshotList, apiCallRc, false))
            {
                addAdjustedMsg(drbdRscData, apiCallRc);

                // this should not be executed if adjusting the drbd resource fails
                copyResFileToBackup(drbdRscData);
            }
            else
            {
                addAbortedMsg(drbdRscData, apiCallRc);
            }
        }
        return LayerProcessResult.NO_DEVICES_PROVIDED; // TODO: make this depend on whether the local
        // resource is currently primary or not.
    }

    private void addDeletedMsg(DrbdRscData<Resource> drbdRscData, ApiCallRcImpl apiCallRc)
    {
        apiCallRc.addEntry(
            ApiCallRcImpl.simpleEntry(
                ApiConsts.MASK_RSC | ApiConsts.DELETED,
                "Resource '" +  drbdRscData.getSuffixedResourceName() + "' [DRBD] deleted."
            )
        );
    }

    private void addAdjustedMsg(DrbdRscData<Resource> drbdRscData, ApiCallRcImpl apiCallRc)
    {
        apiCallRc.addEntry(
            ApiCallRcImpl.simpleEntry(
                ApiConsts.MASK_RSC | ApiConsts.MODIFIED,
                "Resource '" +  drbdRscData.getSuffixedResourceName() + "' [DRBD] adjusted."
            )
        );
    }

    private void addAbortedMsg(DrbdRscData<Resource> drbdRscData, ApiCallRcImpl apiCallRc)
    {
        apiCallRc.addEntry(
            ApiCallRcImpl.simpleEntry(
                ApiConsts.MASK_RSC,
                "Resource '" + drbdRscData.getSuffixedResourceName() + "' [DRBD] not adjusted "
            ).setCause(
                "This happened most likely because the layer below did not provide a device to work with."
            )
        );
    }

    private boolean processChild(
        DrbdRscData<Resource> drbdRscData,
        List<Snapshot> snapshotList,
        ApiCallRcImpl apiCallRc
    )
        throws AccessDeniedException, StorageException, ResourceException, VolumeException, DatabaseException
    {
        boolean isDiskless = drbdRscData.getAbsResource().isDrbdDiskless(workerCtx);
        boolean isDiskRemoving = drbdRscData.getAbsResource().getStateFlags()
            .isSet(workerCtx, Resource.Flags.DISK_REMOVING);

        boolean contProcess = isDiskless;

        if (!isDiskless || isDiskRemoving)
        {
            AbsRscLayerObject<Resource> dataChild = drbdRscData.getChildBySuffix(DrbdRscData.SUFFIX_DATA);
            LayerProcessResult dataResult = resourceProcessorProvider.get().process(
                dataChild,
                snapshotList,
                apiCallRc
            );
            LayerProcessResult metaResult = null;

            AbsRscLayerObject<Resource> metaChild = drbdRscData.getChildBySuffix(DrbdRscData.SUFFIX_META);
            if (metaChild != null)
            {
                metaResult = resourceProcessorProvider.get().process(
                    metaChild,
                    snapshotList,
                    apiCallRc
                );
            }

            if (
                dataResult == LayerProcessResult.SUCCESS &&
                    (metaResult == null || metaResult == LayerProcessResult.SUCCESS)
            )
            {
                contProcess = true;
            }
        }
        return contProcess;
    }

    /**
     * Deletes a given DRBD resource, by calling {@code drbdadm down <resource-name>} and deleting
     * the resource specific .res file
     * {@link Resource#delete(AccessContext)} is also called on the given resource
     *
     * @param drbdRsc
     * @param rscNameSuffix
     * @throws StorageException
     * @throws DatabaseException
     * @throws AccessDeniedException
     */
    private void deleteDrbd(DrbdRscData<Resource> drbdRscData) throws StorageException
    {
        String suffixedRscName = drbdRscData.getSuffixedResourceName();
        try
        {
            errorReporter.logTrace("Shutting down drbd resource %s", suffixedRscName);
            drbdUtils.down(drbdRscData);
            Path resFile = asResourceFile(drbdRscData, false);
            errorReporter.logTrace("Deleting res file: %s ", resFile);
            Files.deleteIfExists(resFile);

            drbdRscData.setExists(false);
            for (DrbdVlmData<Resource> drbdVlmData : drbdRscData.getVlmLayerObjects().values())
            {
                drbdVlmData.setExists(false);
            }
        }
        catch (ExtCmdFailedException cmdExc)
        {
            throw new StorageException(
                "Shutdown of the DRBD resource '" + suffixedRscName + " failed",
                getAbortMsg(drbdRscData),
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
     */
    private boolean adjustDrbd(
        DrbdRscData<Resource> drbdRscData,
        List<Snapshot> snapshotList,
        ApiCallRcImpl apiCallRc,
        boolean childAlreadyProcessed
    )
        throws AccessDeniedException, StorageException, DatabaseException,
            ResourceException, VolumeException
    {
        boolean contProcess = true;
        updateRequiresAdjust(drbdRscData);

        if (drbdRscData.isAdjustRequired())
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
            updateResourceToCurrentDrbdState(drbdRscData);

            List<DrbdVlmData<Resource>> checkMetaData = detachVolumesIfNecessary(drbdRscData);

            adjustSuspendIo(drbdRscData, snapshotList);

            if (!childAlreadyProcessed)
            {
                contProcess = processChild(drbdRscData, snapshotList, apiCallRc);
            }

            if (contProcess)
            {
                // hasMetaData needs to be run after child-resource processed
                List<DrbdVlmData<Resource>> createMetaData = new ArrayList<>();
                if (!drbdRscData.getAbsResource().isDrbdDiskless(workerCtx))
                {
                    // do not try to create meta data while the resource is diskless....
                    for (DrbdVlmData<Resource> drbdVlmData : checkMetaData)
                    {
                        if (!hasMetaData(drbdVlmData))
                        {
                            createMetaData.add(drbdVlmData);
                        }
                    }
                }

                regenerateResFile(drbdRscData);

                // createMetaData needs rendered resFile
                for (DrbdVlmData<Resource> drbdVlmData : createMetaData)
                {
                    createMetaData(drbdVlmData);
                }

                try
                {
                    for (DrbdVlmData<Resource> drbdVlmData : drbdRscData.getVlmLayerObjects().values())
                    {
                        if (needsResize(drbdVlmData))
                        {
                            drbdUtils.resize(
                                drbdVlmData,
                                // TODO: not sure if we should "--assume-clean" if data device is only partially
                                // thinly backed
                                VolumeUtils.isVolumeThinlyBacked(drbdVlmData, false)
                            );
                        }
                    }

                    if (!drbdRscData.getAbsResource().isDrbdDiskless(workerCtx))
                    {
                        for (DrbdRscData<Resource> otherRsc : drbdRscData.getRscDfnLayerObject().getDrbdRscDataList())
                        {
                            if (!otherRsc.equals(drbdRscData) && // skip local rsc
                                !otherRsc.getAbsResource().isDrbdDiskless(workerCtx) && // skip remote diskless resources
                                    otherRsc.getAbsResource().getStateFlags().isSet(workerCtx, Resource.Flags.DELETE)
                            )
                            {
                                /*
                                 * If a peer is getting deleted, we issue a forget-peer (which requires
                                 * a del-peer) so that the bitmap of that peer is reset to day0
                                 */
                                ExtCmdFailedException delPeerExc = null;
                                try
                                {
                                    /*
                                     * Race condition:
                                     * If two linstor-resources are deleted concurrently, and one is much
                                     * faster than the other, the slower will get an "unknown connection"
                                     * from the drbd-utils when executing the del-peer command.
                                     * In that case, we will still try the forget-peer.
                                     * If the forget-peer command succeeds, ignore the exception of the failed
                                     * del-peer command.
                                     * If the forget-peer command also failed we ignore that exception and
                                     * re-throw the del-peer's exception as there could be a different reason
                                     * for the del-peer to have failed than this race-condition
                                     */
                                    drbdUtils.deletePeer(otherRsc);
                                }
                                catch (ExtCmdFailedException exc)
                                {
                                    delPeerExc = exc;
                                }
                                try
                                {
                                    drbdUtils.forgetPeer(otherRsc);
                                }
                                catch (ExtCmdFailedException forgetPeerExc)
                                {
                                    throw delPeerExc != null ? delPeerExc : forgetPeerExc;
                                }
                            }
                        }
                    }

                    drbdUtils.adjust(
                        drbdRscData,
                        false,
                        false,
                        false
                    );
                    drbdRscData.setAdjustRequired(false);

                    // set device paths
                    for (DrbdVlmData<Resource> drbdVlmData : drbdRscData.getVlmLayerObjects().values())
                    {
                        drbdVlmData.setDevicePath(generateDevicePath(drbdVlmData));
                    }
                    condInitialOrSkipSync(drbdRscData);
                }
                catch (ExtCmdFailedException exc)
                {
                    throw new ResourceException(
                        String.format("Failed to adjust DRBD resource %s", drbdRscData.getSuffixedResourceName()),
                        exc
                    );
                }
            }
        }
        return contProcess;
    }

    private boolean needsResize(DrbdVlmData<Resource> drbdVlmData) throws AccessDeniedException
    {
        // A resize should not be called on a resize without a disk
        // there was a bug in pre 0.9.2 versions where diskless would be chosen for the resize command
        boolean isResizeFlagSet = ((Volume) drbdVlmData.getVolume()).getFlags()
            .isSet(workerCtx, Volume.Flags.DRBD_RESIZE);
        return isResizeFlagSet && drbdVlmData.hasDisk();
    }

    private String generateDevicePath(DrbdVlmData<Resource> drbdVlmData)
    {
        return String.format(DRBD_DEVICE_PATH_FORMAT, drbdVlmData.getVlmDfnLayerObject().getMinorNr().value);
    }

    private void updateRequiresAdjust(DrbdRscData<?> drbdRscData)
    {
        drbdRscData.setAdjustRequired(true); // TODO: could be improved :)
    }

    private List<DrbdVlmData<Resource>> detachVolumesIfNecessary(DrbdRscData<Resource> drbdRscData)
        throws AccessDeniedException, StorageException
    {
        List<DrbdVlmData<Resource>> checkMetaData = new ArrayList<>();
        Resource rsc = drbdRscData.getAbsResource();
        if (!rsc.isDrbdDiskless(workerCtx) ||
            rsc.getStateFlags().isSet(workerCtx, Resource.Flags.DISK_REMOVING)
        )
        {
            // using a dedicated list to prevent concurrentModificationException
            List<DrbdVlmData<Resource>> volumesToDelete = new ArrayList<>();
            List<DrbdVlmData<Resource>> volumesToMakeDiskless = new ArrayList<>();

            for (DrbdVlmData<Resource> drbdVlmData : drbdRscData.getVlmLayerObjects().values())
            {
                if (((Volume) drbdVlmData.getVolume()).getFlags().isSet(workerCtx, Volume.Flags.DELETE))
                {
                    if (drbdVlmData.hasDisk() && !drbdVlmData.hasFailed())
                    {
                        volumesToDelete.add(drbdVlmData);
                    }
                }
                else if (rsc.getStateFlags().isSet(workerCtx, Resource.Flags.DISK_REMOVING))
                {
                    if (drbdVlmData.hasDisk() && !drbdVlmData.hasFailed())
                    {
                        volumesToMakeDiskless.add(drbdVlmData);
                    }
                }
                else
                {
                    checkMetaData.add(drbdVlmData);
                }
            }
            for (DrbdVlmData<Resource> drbdVlmData : volumesToDelete)
            {
                detachDrbdVolume(drbdVlmData, false);
                drbdVlmData.setExists(false);
            }
            for (DrbdVlmData<Resource> drbdVlmData : volumesToMakeDiskless)
            {
                detachDrbdVolume(drbdVlmData, true);
                drbdVlmData.setExists(false);
            }
        }
        return checkMetaData;
    }

    private void detachDrbdVolume(DrbdVlmData<Resource> drbdVlmData, boolean diskless) throws StorageException
    {
        errorReporter.logTrace(
            "Detaching volume %s/%d",
            drbdVlmData.getRscLayerObject().getSuffixedResourceName(),
            drbdVlmData.getVlmNr().value
        );
        try
        {
            drbdUtils.detach(drbdVlmData, diskless);
            drbdVlmData.setHasDisk(false);
        }
        catch (ExtCmdFailedException exc)
        {
            throw new StorageException(
                String.format(
                    "Failed to detach DRBD volume %s/%d",
                    drbdVlmData.getRscLayerObject().getSuffixedResourceName(),
                    drbdVlmData.getVlmNr().value
                ),
                exc
            );
        }
    }

    private void adjustSuspendIo(
        DrbdRscData<Resource> drbdRscData,
        List<Snapshot> snapshotList
    )
        throws ResourceException
    {
        boolean shouldSuspend = drbdRscData.exists() && drbdRscData.getSuspendIo();

        if (!drbdRscData.isSuspended() && shouldSuspend)
        {
            try
            {
                errorReporter.logTrace("Suspending DRBD-IO for resource '%s'", drbdRscData.getSuffixedResourceName());
                drbdUtils.suspendIo(drbdRscData);
            }
            catch (ExtCmdFailedException exc)
            {
                throw new ResourceException(
                    "Suspend of the DRBD resource '" + drbdRscData.getSuffixedResourceName() + " failed",
                    getAbortMsg(drbdRscData),
                    "The external command for suspending the DRBD resource failed",
                    null,
                    null,
                    exc
                );
            }
        }
        else
        if (drbdRscData.isSuspended() && !shouldSuspend)
        {
            try
            {
                errorReporter.logTrace("Resuming DRBD-IO for resource '%s'", drbdRscData.getSuffixedResourceName());
                drbdUtils.resumeIo(drbdRscData);
            }
            catch (ExtCmdFailedException exc)
            {
                throw new ResourceException(
                    "Resume of the DRBD resource '" + drbdRscData.getSuffixedResourceName() + " failed",
                    getAbortMsg(drbdRscData),
                    "The external command for resuming the DRBD resource failed",
                    null,
                    null,
                    exc
                );
            }
        }
    }

    private boolean hasMetaData(DrbdVlmData<Resource> drbdVlmData)
        throws VolumeException
    {
        String metaDiskPath = drbdVlmData.getMetaDiskPath();
        if (metaDiskPath == null)
        {
            // internal meta data
            metaDiskPath = drbdVlmData.getBackingDevice();
        }

        MdSuperblockBuffer mdUtils = new MdSuperblockBuffer();
        try
        {
            mdUtils.readObject(metaDiskPath);
        }
        catch (IOException exc)
        {
            throw new VolumeException(
                String.format(
                    "Failed to access DRBD super-block of volume %s/%d",
                    drbdVlmData.getRscLayerObject().getSuffixedResourceName(),
                    drbdVlmData.getVlmNr().value
                ),
                exc
            );
        }

        boolean hasMetaData;

        if (drbdVlmData.checkMetaData() ||
            // when adding a disk, DRBD believes that it is diskless but we still need to create metadata
            !drbdVlmData.hasDisk())
        {
            if (mdUtils.hasMetaData())
            {
                boolean isMetaDataCorrupt;
                try
                {
                    isMetaDataCorrupt = !drbdUtils.hasMetaData(
                        metaDiskPath,
                        drbdVlmData.getVlmDfnLayerObject().getMinorNr().value,
                        "internal"
                    );
                }
                catch (ExtCmdFailedException exc)
                {
                    throw new VolumeException(
                        String.format(
                            "Failed to check DRBD meta-data integrety of volume %s/%d",
                            drbdVlmData.getRscLayerObject().getSuffixedResourceName(),
                            drbdVlmData.getVlmNr().value
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

    private void createMetaData(DrbdVlmData<Resource> drbdVlmData)
        throws AccessDeniedException, StorageException, ImplementationError, VolumeException
    {
        try
        {
            drbdUtils.createMd(
                drbdVlmData,
                drbdVlmData.getRscLayerObject().getPeerSlots()
            );
            drbdVlmData.setMetaDataIsNew(true);

            boolean skipInitSync = VolumeUtils.isVolumeThinlyBacked(drbdVlmData, true);
            if (!skipInitSync)
            {
                skipInitSync = VolumeUtils.getStorageDevices(
                    drbdVlmData.getChildBySuffix(DrbdRscData.SUFFIX_DATA)
                )
                    .stream()
                    .map(VlmProviderObject::getProviderKind)
                    .allMatch(kind -> kind == DeviceProviderKind.ZFS || kind == DeviceProviderKind.ZFS_THIN);
            }

            if (skipInitSync)
            {
                String currentGi = null;
                try
                {
                    currentGi = drbdVlmData.getVlmDfnLayerObject().getVolumeDefinition()
                        .getProps(workerCtx).getProp(ApiConsts.KEY_DRBD_CURRENT_GI);
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
                    int vlmNr = drbdVlmData.getVlmNr().value;
                    throw new StorageException(
                        "Meta data creation for resource '" +
                        drbdVlmData.getRscLayerObject().getSuffixedResourceName() + "' volume " + vlmNr + " failed",
                        getAbortMsg(drbdVlmData),
                        "Volume " + vlmNr + " of the resource uses a thin provisioning storage driver,\n" +
                        "but no initial value for the DRBD current generation is set on the volume definition",
                        "- Ensure that the initial DRBD current generation is set on the volume definition\n" +
                        "or\n" +
                        "- Recreate the volume definition",
                        "The key of the initial DRBD current generation property is:\n" +
                        ApiConsts.KEY_DRBD_CURRENT_GI,
                        null
                    );
                }

                String metaDiskPath = drbdVlmData.getMetaDiskPath();
                boolean internal = false;
                if (metaDiskPath == null)
                {
                    // internal metadata
                    metaDiskPath = drbdVlmData.getBackingDevice();
                    internal = true;
                }
                drbdUtils.setGi(
                    drbdVlmData.getRscLayerObject().getNodeId(),
                    drbdVlmData.getVlmDfnLayerObject().getMinorNr(),
                    metaDiskPath,
                    currentGi,
                    null,
                    !drbdVlmData.getRscLayerObject().getFlags().isSet(workerCtx, DrbdRscFlags.INITIALIZED),
                    internal
                );
            }
        }
        catch (ExtCmdFailedException exc)
        {
            throw new VolumeException(
                String.format(
                    "Failed to create meta-data for DRBD volume %s/%d",
                    drbdVlmData.getRscLayerObject().getSuffixedResourceName(),
                    drbdVlmData.getVlmNr().value
                ),
                exc
            );
        }
    }

    private void updateResourceToCurrentDrbdState(DrbdRscData<Resource> drbdRscData)
        throws AccessDeniedException, StorageException
    {
        try
        {
            errorReporter.logTrace(
                "Synchronizing Linstor-state with DRBD-state for resource %s",
                drbdRscData.getSuffixedResourceName()
            );
            fillResourceState(drbdRscData);

            DrbdResource drbdRscState = drbdState.getDrbdResource(drbdRscData.getSuffixedResourceName());
            if (drbdRscState == null)
            {
                drbdRscData.setExists(false);
            }
            else
            {
                drbdRscData.setExists(true);

                { // check drbdRole
                    DrbdResource.Role rscRole = drbdRscState.getRole();
                    if (rscRole == DrbdResource.Role.UNKNOWN)
                    {
                        drbdRscData.setAdjustRequired(true);
                    }
                    else
                    if (rscRole == DrbdResource.Role.PRIMARY)
                    {
                        drbdRscData.setPrimary(true);
                    }
                }

                { // check drbd connections
                    Resource localResource = drbdRscData.getAbsResource();
                    localResource.getDefinition().streamResource(workerCtx)
                        .filter(otherRsc -> !otherRsc.equals(localResource))
                        .forEach(
                            otherRsc ->
                                {
                                    DrbdConnection drbdConn = drbdRscState.getConnection(
                                        otherRsc.getNode().getName().displayValue
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
                                                drbdRscData.setAdjustRequired(true);
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
                                        drbdRscData.setAdjustRequired(true);
                                    }
                                }
                        );
                }

                Map<VolumeNumber, DrbdVolume> drbdVolumes = drbdRscState.getVolumesMap();

                for (DrbdVlmData<Resource> drbdVlmData : drbdRscData.getVlmLayerObjects().values())
                {
                    { // check drbd-volume
                        DrbdVolume drbdVlmState = drbdVolumes.remove(drbdVlmData.getVlmNr());
                        if (drbdVlmState != null)
                        {
                            drbdVlmData.setExists(true);
                            DiskState diskState = drbdVlmState.getDiskState();

                            /*
                             *  The following line is commented out to prevent confusion
                             *  The problem is that this will be filled when the resource changes (thats nice)
                             *  but it will not be updated when an events2 event occurs.
                             *  Even if we can update this field upon an events2, we would then have to
                             *  update the whole DrbdVlmData, for which there is currently no mechanism
                             *  (apart from the EventSystem, but that does not allow such complex data
                             *  as layerData).
                             *
                             *  That means that the EventSystem converts events2 diskChange events as usual
                             *  (this sets on controller the volume_states accordingly), but
                             *  drbdVlmData.getDiskState() will stay the same for a long time. To prevent
                             *  this divergence, we simply do not set the diskstate here (until we might rework
                             *  the EventSystem somehow)
                             */
                            // drbdVlmData.setDiskState(diskState.toString());
                            switch (diskState)
                            {
                                case DISKLESS:
                                    if (!drbdVlmState.isClient())
                                    {
                                        drbdVlmData.setFailed(true);
                                        drbdRscData.setAdjustRequired(true);
                                    }
                                    else
                                    {
                                        drbdVlmData.setCheckMetaData(false);
                                    }
                                    break;
                                case DETACHING:
                                    // TODO: May be a transition from storage to client
                                    // fall-through
                                case FAILED:
                                    drbdVlmData.setFailed(true);
                                    // fall-through
                                case NEGOTIATING:
                                    // fall-through
                                case UNKNOWN:
                                    // The local disk state should not be unknown,
                                    // try adjusting anyways
                                    drbdRscData.setAdjustRequired(true);
                                    break;
                                case UP_TO_DATE:
                                    // fall-through
                                case CONSISTENT:
                                    // fall-through
                                case INCONSISTENT:
                                    // fall-through
                                case OUTDATED:
                                    drbdVlmData.setHasMetaData(true);
                                    // No additional check for existing meta data is required
                                    drbdVlmData.setCheckMetaData(false);
                                    // fall-through
                                case ATTACHING:
                                    drbdVlmData.setHasDisk(true);
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
                            drbdRscData.setAdjustRequired(true);
                        }
                    }

                    drbdVlmData.setMetaDataIsNew(false);
                }
                if (!drbdVolumes.isEmpty())
                {
                    // The DRBD resource has additional unknown volumes,
                    // adjust the resource
                    drbdRscData.setAdjustRequired(true);
                }

                drbdRscData.setSuspended(
                    drbdRscState.getSuspendedUser() == null ?
                        false :
                        drbdRscState.getSuspendedUser()
                );
            }
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

    private void fillResourceState(DrbdRscData<Resource> drbdRscData)
        throws AccessDeniedException
    {
        Resource localResource = drbdRscData.getAbsResource();

        // FIXME: Temporary fix: If the NIC selection property on a storage pool is changed retrospectively,
        //        then rewriting the DRBD resource configuration file and 'drbdadm adjust' is required,
        //        but there is not yet a mechanism to notify the device handler to perform an adjust action.
        drbdRscData.setAdjustRequired(true);

        boolean isRscDisklessFlagSet = localResource.getStateFlags().isSet(workerCtx, Resource.Flags.DRBD_DISKLESS);

        Iterator<DrbdVlmData<Resource>> drbdVlmDataIter = drbdRscData.getVlmLayerObjects().values().iterator();
        while (drbdVlmDataIter.hasNext())
        {
            DrbdVlmData<Resource> drbdVlmData = drbdVlmDataIter.next();

            if (isRscDisklessFlagSet)
            {
                drbdVlmData.setCheckMetaData(false);
            }
        }
    }

    private void regenerateResFile(DrbdRscData<Resource> drbdRscData)
        throws AccessDeniedException, StorageException
    {
        Path resFile = asResourceFile(drbdRscData, false);
        Path tmpResFile = asResourceFile(drbdRscData, true);

        List<DrbdRscData<Resource>> drbdPeerRscDataList = drbdRscData.getRscDfnLayerObject()
            .getDrbdRscDataList().stream()
            .filter(otherRscData -> !otherRscData.equals(drbdRscData) &&
                AccessUtils.execPrivileged(() -> DrbdLayerUtils.isDrbdResourceExpected(workerCtx, otherRscData))
            )
            .collect(Collectors.toList());

        String content = new ConfFileBuilder(
            errorReporter,
            workerCtx,
            drbdRscData,
            drbdPeerRscDataList,
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
                "Creation of the DRBD configuration file for resource '" + drbdRscData.getSuffixedResourceName() +
                    "' failed due to an I/O error",
                getAbortMsg(drbdRscData),
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
            drbdUtils.checkResFile(tmpResFile, resFile);
        }
        catch (ExtCmdFailedException exc)
        {
            String errMsg = exc.getMessage();
            throw new StorageException(
                "Generated resource file for resource '" + drbdRscData.getSuffixedResourceName() + "' is invalid.",
                getAbortMsg(drbdRscData),
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
                getAbortMsg(drbdRscData),
                "Unable to move temporary DRBD resource file due to an I/O error",
                "- Check whether enough free space is available for moving the file\n" +
                    "- Check whether the application has write access to the target directory\n" +
                    "- Check whether the storage is operating flawlessly",
                "The error reported by the runtime environment or operating system is:\n" + ioErrorMsg,
                ioExc
            );
        }
    }

    private void copyResFileToBackup(DrbdRscData<Resource> drbdRscData) throws StorageException
    {
        Path resFile = asResourceFile(drbdRscData, false);
        Path backupFile = asBackupResourceFile(drbdRscData);
        try
        {
            Files.copy(resFile, backupFile, StandardCopyOption.REPLACE_EXISTING);
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
                "Failed to create a backup of the resource file of resource '" + drbdRscData.getSuffixedResourceName() +
                    "'",
                getAbortMsg(drbdRscData),
                null,
                "- Check whether enough free space is available for the creation of the file\n" +
                    "- Check whether the application has write access to the target directory\n" +
                    "- Check whether the storage is operating flawlessly",
                "The error reported by the runtime environment or operating system is:\n" + ioErrorMsg,
                ioExc
            );
        }
    }

    private void deleteBackupResFile(DrbdRscData<Resource> drbdRscDataRef) throws StorageException
    {
        Path resFile = asBackupResourceFile(drbdRscDataRef);
        errorReporter.logTrace("Deleting res file from backup: %s ", resFile);
        try
        {
            Files.deleteIfExists(resFile);
        }
        catch (IOException exc)
        {
            throw new StorageException("IOException while removing resource file from backup", exc);
        }
    }

    private void condInitialOrSkipSync(DrbdRscData<Resource> drbdRscData)
        throws AccessDeniedException, StorageException
    {
        try
        {
            Resource rsc = drbdRscData.getAbsResource();
            ResourceDefinition rscDfn = rsc.getDefinition();

            if (rscDfn.getProps(workerCtx).getProp(InternalApiConsts.PROP_PRIMARY_SET) == null &&
                    !rsc.getStateFlags().isSet(workerCtx, Resource.Flags.DRBD_DISKLESS)
            )
            {
                boolean alreadyInitialized = !allVlmsMetaDataNew(drbdRscData);
                errorReporter.logTrace(
                    "Requesting primary on %s; already initialized: %b",
                    drbdRscData.getSuffixedResourceName(),
                    alreadyInitialized
                );
                // Send a primary request even when volumes have already been initialized so that the controller can
                // save DrbdPrimarySetOn so that subsequently added nodes do not request to be primary
                sendRequestPrimaryResource(
                    rscDfn.getName().getDisplayName(), // intentionally not suffixedRscName
                    rsc.getUuid().toString(),
                    alreadyInitialized
                );
            }
            else
            if (rsc.isCreatePrimary() && !drbdRscData.isPrimary())
            {
                // First, skip the resync on all thinly provisioned volumes
                boolean haveFatVlm = false;
                for (DrbdVlmData<Resource> drbdVlmData : drbdRscData.getVlmLayerObjects().values())
                {
                    if (!VolumeUtils.isVolumeThinlyBacked(drbdVlmData, false))
                    {
                        haveFatVlm = true;
                        break;
                    }
                }

                // Set the resource primary (--force) to trigger an initial sync of all
                // fat provisioned volumes
                rsc.unsetCreatePrimary();
                if (haveFatVlm)
                {
                    errorReporter.logTrace("Setting resource primary on %s", drbdRscData.getSuffixedResourceName());
                    setResourcePrimary(drbdRscData);
                }

                MkfsUtils.makeFileSystemOnMarked(errorReporter, extCmdFactory, workerCtx, rsc);
            }
        }
        catch (InvalidKeyException invalidKeyExc)
        {
            throw new ImplementationError("Invalid hardcoded property key", invalidKeyExc);
        }
    }

    private boolean allVlmsMetaDataNew(DrbdRscData<Resource> rscState)
    {
        return rscState.getVlmLayerObjects().values().stream().allMatch(DrbdVlmData::isMetaDataNew);
    }

    private void setResourcePrimary(DrbdRscData<Resource> drbdRscData) throws StorageException
    {
        try
        {
            waitForValidStateForPrimary(drbdRscData);

            drbdUtils.primary(drbdRscData, true, false);
            // setting to secondary because of two reasons:
            // * bug in drbdsetup: cannot down a primary resource
            // * let the user choose which satellite should be primary (or let it be handled by auto-promote)
            drbdUtils.secondary(drbdRscData);
        }
        catch (ExtCmdFailedException cmdExc)
        {
            throw new StorageException(
                "Starting the initial resync of the DRBD resource '" + drbdRscData.getSuffixedResourceName() +
                    " failed",
                getAbortMsg(drbdRscData),
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

    private void waitForValidStateForPrimary(DrbdRscData<Resource> drbdRscData) throws StorageException
    {
        try
        {
            final Object syncObj = new Object();
            synchronized (syncObj)
            {
                String rscNameStr = drbdRscData.getSuffixedResourceName();
                ReadyForPrimaryNotifier resourceObserver = new ReadyForPrimaryNotifier(rscNameStr, syncObj);
                drbdState.addObserver(resourceObserver, DrbdStateTracker.OBS_DISK);
                if (!resourceObserver.hasValidStateForPrimary(drbdState.getDrbdResource(rscNameStr)))
                {
                    syncObj.wait(HAS_VALID_STATE_FOR_PRIMARY_TIMEOUT);
                }
                if (!resourceObserver.hasValidStateForPrimary(drbdState.getDrbdResource(rscNameStr)))
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

    private Path asResourceFile(DrbdRscData<Resource> drbdRscData, boolean temp)
    {
        return Paths.get(
            CoreModule.CONFIG_PATH,
            drbdRscData.getSuffixedResourceName() + (temp ? DRBD_CONFIG_TMP_SUFFIX : DRBD_CONFIG_SUFFIX)
        );
    }

    private Path asBackupResourceFile(DrbdRscData<Resource> drbdRscData)
    {
        return Paths.get(
            CoreModule.BACKUP_PATH,
            drbdRscData.getSuffixedResourceName() + DRBD_CONFIG_SUFFIX
        );
    }

    private String getAbortMsg(DrbdRscData<Resource> drbdRscData)
    {
        return "Operations on resource '" + drbdRscData.getSuffixedResourceName() + "' were aborted";
    }

    private String getAbortMsg(DrbdVlmData<Resource> drbdVlmData)
    {
        return "Operations on volume " + drbdVlmData.getVlmNr().value + " of resource '" +
            drbdVlmData.getRscLayerObject().getSuffixedResourceName() + "' were aborted";
    }

    @Override
    public void setLocalNodeProps(Props localNodePropsRef)
    {
        // ignored
    }
}
