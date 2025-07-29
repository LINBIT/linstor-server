package com.linbit.linstor.layer.drbd;

import com.linbit.ImplementationError;
import com.linbit.Platform;
import com.linbit.PlatformStlt;
import com.linbit.drbd.DrbdVersion;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.extproc.ExtCmdFailedException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.SysBlockUtils;
import com.linbit.linstor.core.devmgr.DeviceHandler;
import com.linbit.linstor.core.devmgr.exceptions.ResourceException;
import com.linbit.linstor.core.devmgr.exceptions.VolumeException;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.pojos.LocalPropsChangePojo;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.DeviceLayer;
import com.linbit.linstor.layer.drbd.drbdstate.DiskState;
import com.linbit.linstor.layer.drbd.drbdstate.DrbdConnection;
import com.linbit.linstor.layer.drbd.drbdstate.DrbdEventPublisher;
import com.linbit.linstor.layer.drbd.drbdstate.DrbdResource;
import com.linbit.linstor.layer.drbd.drbdstate.DrbdStateStore;
import com.linbit.linstor.layer.drbd.drbdstate.DrbdStateTracker;
import com.linbit.linstor.layer.drbd.drbdstate.DrbdVolume;
import com.linbit.linstor.layer.drbd.drbdstate.NoInitialStateException;
import com.linbit.linstor.layer.drbd.helper.ReadyForPrimaryNotifier;
import com.linbit.linstor.layer.drbd.utils.ConfFileBuilder;
import com.linbit.linstor.layer.drbd.utils.DrbdAdm;
import com.linbit.linstor.layer.drbd.utils.MdSuperblockBuffer;
import com.linbit.linstor.layer.drbd.utils.WindowsFirewall;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject.Size;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscObject.DrbdRscFlags;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.storage.utils.MkfsUtils;
import com.linbit.linstor.storage.utils.VolumeUtils;
import com.linbit.linstor.utils.layer.DrbdLayerUtils;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.utils.AccessUtils;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Singleton
public class DrbdLayer implements DeviceLayer
{
    public static final String DRBD_DEVICE_PATH_FORMAT = "/dev/drbd%d";
    private static final String DRBD_CONFIG_SUFFIX = ".res";
    private static final String DRBD_CONFIG_TMP_SUFFIX = ".res_tmp";

    private static final long HAS_VALID_STATE_FOR_PRIMARY_TIMEOUT = 2000;

    private static final String DRBD_NEW_GI = "0000000000000004";

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
    private final StltConfigAccessor stltCfgAccessor;
    private final DrbdVersion drbdVersion;
    private final WindowsFirewall windowsFirewall;
    private final PlatformStlt platformStlt;

    @Nullable private static String drbdSetupStatusOutput;

    /**
     * This is a list of resource names (lowercase) that needs to be adjusted, in the devmanager run
     * If null, adjust all resources as before this command was available
     */
    @Nullable private static List<String> adjustResourcesList;

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
        ExtCmdFactory extCmdFactoryRef,
        StltConfigAccessor stltCfgAccessorRef,
        DrbdVersion drbdVersionRef,
        WindowsFirewall windowsFirewallRef,
        PlatformStlt platformStltRef
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
        stltCfgAccessor = stltCfgAccessorRef;
        drbdVersion = drbdVersionRef;
        windowsFirewall = windowsFirewallRef;
        platformStlt = platformStltRef;
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
        if (drbdSetupStatusOutput == null)
        {
            drbdSetupStatusOutput = drbdUtils.drbdSetupStatus();
        }

        List<String> notGeneratedResFiles = regenerateAllResFile(rscDataList);

        if (drbdVersion.getUtilsVsn().greaterOrEqual(new ExtToolsInfo.Version(9, 32, 0)))
        {
            try
            {
                adjustResourcesList = drbdUtils.listAdjustable();
                if (adjustResourcesList != null)
                {
                    adjustResourcesList.addAll(notGeneratedResFiles);
                }
            }
            catch (ExtCmdFailedException extCmdExc)
            {
                adjustResourcesList = null;
                errorReporter.reportError(extCmdExc);
            }
        }
    }

    @Override
    public boolean resourceFinished(AbsRscLayerObject<Resource> layerDataRef)
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
        boolean resourceReadySent = false;
        DrbdResource drbdResource;
        try
        {
            drbdResource = drbdState.getDrbdResource(layerDataRef.getSuffixedResourceName());
            // drbdResource might be null if we are over an nvme-initiator
            if (drbdResource != null)
            {
                drbdEventPublisher.resourceCreated(drbdResource);
                resourceReadySent = true;
            }
        }
        catch (NoInitialStateException exc)
        {
            // we should not have been called
            throw new ImplementationError(exc);
        }
        return resourceReadySent;
    }

    @Override
    public void clearCache()
    {
        drbdSetupStatusOutput = null;
        adjustResourcesList = null;
    }

    @Override
    public boolean isDiscGranFeasible(AbsRscLayerObject<Resource> rscLayerObjectRef) throws AccessDeniedException
    {
        return !Platform.isWindows() && !rscLayerObjectRef.getAbsResource().isDrbdDiskless(workerCtx);
    }

    @Override
    public void processResource(
        AbsRscLayerObject<Resource> rscLayerData,
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
            deleteDrbd(drbdRscData, apiCallRc);
            if (processChild(drbdRscData, apiCallRc))
            {
                adjustDrbd(drbdRscData, apiCallRc, true, null);

                // this should not be executed if adjusting the drbd resource fails
                copyResFileToBackup(drbdRscData);
            }
        }
        else
        {
            if (shouldDrbdDeviceBeDeleted(drbdRscData))
            {
                deleteDrbd(drbdRscData, apiCallRc);

                processChild(drbdRscData, apiCallRc);

                // this should not be executed if deleting the drbd resource fails
                deleteBackupResFile(drbdRscData);
            }
            else
            {
                if (adjustDrbd(drbdRscData, apiCallRc, false, drbdSetupStatusOutput))
                {
                    addAdjustedMsg(drbdRscData, apiCallRc);

                    // this should not be executed if adjusting the drbd resource fails
                    copyResFileToBackup(drbdRscData);
                }
                else
                {
                    addNotAdjustedMsg(drbdRscData, apiCallRc);
                }
            }
        }
    }

    private boolean shouldDrbdDeviceBeDeleted(DrbdRscData<Resource> drbdRscData)
        throws AccessDeniedException
    {
        boolean ret = drbdRscData.getRscDfnLayerObject().isDown();
        if (!ret)
        {
            Resource rsc = drbdRscData.getAbsResource();
            StateFlags<Flags> rscFlags = rsc.getStateFlags();
            ret = rscFlags.isSomeSet(
                workerCtx,
                Resource.Flags.DELETE,
                Resource.Flags.DRBD_DELETE,
                Resource.Flags.INACTIVE
            );
            if (!ret)
            {
                Iterator<Volume> vlmIt = rsc.iterateVolumes();
                while (vlmIt.hasNext())
                {
                    Volume vlm = vlmIt.next();
                    if (vlm.getFlags().isSet(workerCtx, Volume.Flags.CLONING))
                    {
                        ret = true;
                        break;
                    }
                }
            }
        }
        return ret;
    }

    private void addDeletedMsg(DrbdRscData<Resource> drbdRscData, ApiCallRcImpl apiCallRc)
    {
        final String msg = "Resource '" +  drbdRscData.getSuffixedResourceName() + "' [DRBD] deleted.";
        apiCallRc.addEntry(
            ApiCallRcImpl.simpleEntry(
                ApiConsts.MASK_RSC | ApiConsts.DELETED,
                msg
            )
        );
        errorReporter.logInfo(msg);
    }

    private void addAdjustedMsg(DrbdRscData<Resource> drbdRscData, ApiCallRcImpl apiCallRc)
    {
        final String msg = "Resource '" +  drbdRscData.getSuffixedResourceName() + "' [DRBD] adjusted.";
        apiCallRc.addEntry(
            ApiCallRcImpl.simpleEntry(
                ApiConsts.MASK_RSC | ApiConsts.MODIFIED,
                msg
            )
        );
        errorReporter.logInfo(msg);
    }

    private void addNotAdjustedMsg(DrbdRscData<Resource> drbdRscData, ApiCallRcImpl apiCallRc)
    {
        final String msg = "Resource '" + drbdRscData.getSuffixedResourceName() + "' [DRBD] not adjusted ";
        apiCallRc.addEntry(
            ApiCallRcImpl.simpleEntry(
                ApiConsts.MASK_RSC,
                msg
            ).setCause(
                "This happened most likely because the layer below did not provide a device to work with."
            )
        );
        errorReporter.logInfo(msg);
    }

    private boolean processChild(
        DrbdRscData<Resource> drbdRscData,
        ApiCallRcImpl apiCallRc
    )
        throws AccessDeniedException, StorageException, ResourceException, VolumeException, DatabaseException
    {
        boolean isDiskless = drbdRscData.getAbsResource().isDrbdDiskless(workerCtx);
        StateFlags<Flags> rscFlags = drbdRscData.getAbsResource().getStateFlags();
        boolean isDiskRemoving = rscFlags.isSet(workerCtx, Resource.Flags.DISK_REMOVING);

        boolean contProcess = isDiskless;

        boolean processChildren = !isDiskless || isDiskRemoving;
        // do not process children when ONLY DRBD_DELETE flag is set (DELETE flag is still unset)
        processChildren &= (!rscFlags.isSet(workerCtx, Resource.Flags.DRBD_DELETE) ||
            rscFlags.isSet(workerCtx, Resource.Flags.DELETE));

        if (processChildren)
        {
            AbsRscLayerObject<Resource> dataChild = drbdRscData.getChildBySuffix(RscLayerSuffixes.SUFFIX_DATA);
            resourceProcessorProvider.get().processResource(dataChild, apiCallRc);

            AbsRscLayerObject<Resource> metaChild = drbdRscData.getChildBySuffix(RscLayerSuffixes.SUFFIX_DRBD_META);
            if (metaChild != null)
            {
                resourceProcessorProvider.get().processResource(metaChild, apiCallRc);
            }

            contProcess = true;
        }
        return contProcess;
    }

    /**
     * Deletes a given DRBD resource, by calling {@code drbdadm down <resource-name>} and deleting
     * the resource specific .res file
     * {@link Resource#delete(AccessContext)} is also called on the given resource
     *
     * @param drbdRscData
     * @throws StorageException
     * @throws DatabaseException
     * @throws AccessDeniedException
     */
    private void deleteDrbd(DrbdRscData<Resource> drbdRscData, ApiCallRcImpl apiCallRc) throws
        StorageException, AccessDeniedException, DatabaseException
    {
        String suffixedRscName = drbdRscData.getSuffixedResourceName();
        try
        {
            /*
             * If the resource is INACTIVE, this method is also called; every time
             * the rscDfn changes.
             * If the DRBD resource is already down, no need to re-issue "drbdsetup down $rscName"
             */
            updateResourceToCurrentDrbdState(drbdRscData);
            if (drbdRscData.exists())
            {
                errorReporter.logTrace("Shutting down drbd resource %s", suffixedRscName);
                drbdUtils.down(drbdRscData);
                if (Platform.isWindows())
                {
                    for (TcpPortNumber port : drbdRscData.getTcpPortList())
                    {
                        windowsFirewall.closePort(port.value);
                    }
                }
                addDeletedMsg(drbdRscData, apiCallRc);
            }
            Path resFile = asResourceFile(drbdRscData, false, false);
            errorReporter.logTrace("Ensuring .res file is deleted: %s ", resFile);
            Files.deleteIfExists(resFile);
            drbdRscData.setResFileExists(false);

            drbdRscData.setExists(false);
            for (DrbdVlmData<Resource> drbdVlmData : drbdRscData.getVlmLayerObjects().values())
            {
                drbdVlmData.setExists(false);
                drbdVlmData.setDevicePath(null);

                // in case we want to undelete this resource... but the metadata got already wiped
                drbdVlmData.setCheckMetaData(true);
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
        ApiCallRcImpl apiCallRc,
        boolean childAlreadyProcessed,
        @Nullable String drbdSetupStatus
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
             *  - call the underlying layer's process method
             *  - create metaData for new volumes
             *  -- check which volumes are new
             *  -- render all res files
             *  -- create-md only for new volumes (create-md needs already valid .res files)
             *  - adjust all remaining and newly created volumes
             *  - resume IO if allowed by all snapshots
             */

            if (Platform.isWindows())
            {
                for (TcpPortNumber port : drbdRscData.getTcpPortList())
                {
                    windowsFirewall.openPort(port.value);
                }
            }

            updateResourceToCurrentDrbdState(drbdRscData);

            List<DrbdVlmData<Resource>> checkMetaData = detachVolumesIfNecessary(drbdRscData);

            shrinkVolumesIfNecessary(drbdRscData);

            final boolean skipDisk = drbdRscData.isSkipDiskEnabled(workerCtx, stltCfgAccessor.getReadonlyProps());

            if (!childAlreadyProcessed && !skipDisk)
            {
                contProcess = processChild(drbdRscData, apiCallRc);
            }

            if (contProcess)
            {
                for (DrbdVlmData<Resource> drbdVlmData : drbdRscData.getVlmLayerObjects().values())
                {
                    // only continue if either both flags (RESIZE + DRBD_RESIZE) are set or none of them
                    contProcess &= areBothResizeFlagsSet(drbdVlmData);
                }

                if (contProcess)
                {
                    for (DrbdRscData<Resource> peer : drbdRscData.getRscDfnLayerObject().getDrbdRscDataList())
                    {
                        if (!drbdRscData.equals(peer))
                        {
                            for (DrbdVlmData<Resource> peerVlm : peer.getVlmLayerObjects().values())
                            {
                                if (isFlagSet(peerVlm, Volume.Flags.DRBD_RESIZE) &&
                                    !isFlagSet(peerVlm, Volume.Flags.RESIZE))
                                {
                                    // if a peer is currently shrinking, don't do anything
                                    contProcess = false;
                                    break;
                                }
                            }
                        }
                    }
                }
            }

            if (contProcess)
            {
                // hasMetaData needs to be run after child-resource processed
                List<DrbdVlmData<Resource>> createMetaData = new ArrayList<>();
                if (!drbdRscData.getAbsResource().isDrbdDiskless(workerCtx) && !skipDisk)
                {
                    // do not try to create meta data while the resource is diskless or skipDisk is enabled
                    for (DrbdVlmData<Resource> drbdVlmData : checkMetaData)
                    {
                        if (
                            !hasMetaData(drbdVlmData) || needsNewMetaData(drbdVlmData, drbdSetupStatus)
                        )
                        {
                            createMetaData.add(drbdVlmData);
                        }
                    }
                }

                // The .res file might not have been generated in the prepare method since it was
                // missing information from the child-layers. Now that we have processed them, we
                // need to make sure the .res file exists in all circumstances.
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
                        if (needsResize(drbdVlmData) && drbdVlmData.getSizeState().equals(Size.TOO_SMALL))
                        {
                            drbdUtils.resize(
                                drbdVlmData,
                                // TODO: not sure if we should "--assume-clean" if data device is only partially
                                // thinly backed
                                VolumeUtils.isVolumeThinlyBacked(drbdVlmData, false),
                                null
                            );
                            errorReporter.logInfo("DRBD resized %s/%d",
                                drbdRscData.getSuffixedResourceName(), drbdVlmData.getVlmNr().getValue());
                        }
                    }

                    for (DrbdRscData<Resource> otherRsc : drbdRscData.getRscDfnLayerObject().getDrbdRscDataList())
                    {
                        StateFlags<Flags> otherRscFlags = otherRsc.getAbsResource().getStateFlags();
                        if (!otherRsc.equals(drbdRscData) && // skip local rsc
                            /* also call forget-peer for diskless-peers */
                            otherRscFlags.isSomeSet(workerCtx, Resource.Flags.DELETE, Resource.Flags.DRBD_DELETE))
                        {
                            /*
                             * If a peer is getting deleted, we issue a forget-peer (which requires
                             * a del-peer) so that the bitmap of that peer is reset to day0
                             *
                             * This gets important if a new node is created with a never seen node-id but we
                             * simply ran out of unused peer-slots (as those are already bound to old node-ids)
                             */
                            ExtCmdFailedException delPeerExc = null;
                            try
                            {
                                /*
                                 * "drbdadm del-peer ..." only updates the kernel (using "drbdsetup") but does not need
                                 * metadata - can therefore also be used for diskless peers as well as during skipDisk
                                 */

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
                            if (!drbdRscData.getAbsResource().isDrbdDiskless(workerCtx))
                            {
                                if (!skipDisk)
                                {
                                    try
                                    {
                                        drbdUtils.forgetPeer(otherRsc);
                                    }
                                    catch (ExtCmdFailedException forgetPeerExc)
                                    {
                                        if (!otherRsc.getAbsResource().isDrbdDiskless(workerCtx))
                                        {
                                            /*
                                             * let us check our current version of the events2 stream.
                                             * if the peer we just tried to delete does not exist, we should be fine
                                             */
                                            try
                                            {
                                                DrbdResource drbdRscState = drbdState.getDrbdResource(
                                                    drbdRscData.getSuffixedResourceName()
                                                );
                                                if (drbdRscState != null)
                                                {
                                                    // we might not even have started this resource -> no peer we could
                                                    // forget about
                                                    DrbdConnection peerConnection = drbdRscState.getConnection(
                                                        otherRsc.getAbsResource().getNode().getName().displayValue
                                                    );
                                                    if (peerConnection != null)
                                                    {
                                                        throw delPeerExc != null ? delPeerExc : forgetPeerExc;
                                                    }
                                                    else
                                                    {
                                                        // ignore the exceptions, the peer does not seem to exist
                                                        // anymore
                                                        errorReporter.logDebug(
                                                            "del-peer and forget-peer failed, but we also failed " +
                                                                "to find the specific peer. noop"
                                                        );
                                                    }
                                                }
                                            }
                                            catch (NoInitialStateException exc)
                                            {
                                                throw new ImplementationError(exc);
                                            }
                                        }
                                        else
                                        {
                                            /*
                                             * instead of throwing any exception when the peer resource is diskless
                                             * (which might happen with older DRBD versions), we simply log a warning to
                                             * leave some trace of this behavior
                                             */
                                            errorReporter.logDebug(
                                                "Ignoring error caused by forget-peer for a diskless resource"
                                            );
                                        }
                                    }
                                }
                                else
                                {
                                    /*
                                     * we cannot call "drbdadm forget-peer" right now as that would also need to update
                                     * metadata. Since skipDisk is currently enabled, we cannot access the metadata
                                     * right now.
                                     * Instead, we store the node-id in ApiConsts.NAMESPC_STLT+ "/" +
                                     * InternalApiConsts.KEY_BACKUP_NODE_IDS_TO_RESET property so that a "forget-peer"
                                     * is executed later while we can confirm the deletion of the peer-resource in the
                                     * meantime
                                     */
                                    Props rscProps = drbdRscData.getAbsResource().getProps(workerCtx);
                                    try
                                    {
                                        String oldIds = rscProps.getPropWithDefault(
                                            InternalApiConsts.KEY_DRBD_NODE_IDS_TO_RESET,
                                            ApiConsts.NAMESPC_STLT + "/" + InternalApiConsts.NAMESPC_DRBD,
                                            ""
                                        );
                                        String updatedIds = oldIds.isBlank() ?
                                            "" :
                                            oldIds + InternalApiConsts.KEY_BACKUP_NODE_ID_SEPERATOR;
                                        updatedIds += Integer.toString(otherRsc.getNodeId().value);
                                        rscProps.setProp(
                                            InternalApiConsts.KEY_DRBD_NODE_IDS_TO_RESET,
                                            updatedIds,
                                            ApiConsts.NAMESPC_STLT + "/" + InternalApiConsts.NAMESPC_DRBD
                                        );
                                    }
                                    catch (InvalidKeyException | InvalidValueException exc)
                                    {
                                        throw new ImplementationError(exc);
                                    }
                                }
                            }
                        }
                    }

                    try
                    {
                        drbdUtils.adjust(
                            drbdRscData,
                            false,
                            skipDisk,
                            false
                        );
                    }
                    catch (ExtCmdFailedException extCmdExc)
                    {
                        restoreBackupResFile(drbdRscData);
                        throw extCmdExc;
                    }

                    if (
                        drbdRscData.getAbsResource().getStateFlags()
                            .isSet(workerCtx, Resource.Flags.RESTORE_FROM_SNAPSHOT) &&
                            !DrbdLayerUtils.isForceInitialSyncSet(workerCtx, drbdRscData)
                    )
                    {
                        /*
                         * If forceInitialSync is set, we already created new metadata, so no resetting needed.
                         *
                         * Basically a similar scenario as "we have the current peer and ALL other peers were removed".
                         * See delete-case's forget-peer comment
                         */
                        String ids = drbdRscData.getAbsResource().getResourceDefinition().getProps(workerCtx).getProp(
                            InternalApiConsts.KEY_BACKUP_NODE_IDS_TO_RESET,
                            ApiConsts.NAMESPC_BACKUP_SHIPPING
                        );
                        forgetPeersCleanup(drbdRscData, ids);
                    }

                    recoverAfterSkipdisk(drbdRscData);

                    drbdRscData.setAdjustRequired(false);

                    // set device paths
                    for (DrbdVlmData<Resource> drbdVlmData : drbdRscData.getVlmLayerObjects().values())
                    {
                        StateFlags<Volume.Flags> vlmFlags = ((Volume) drbdVlmData.getVolume()).getFlags();
                        if (vlmFlags.isSomeSet(
                            workerCtx,
                            Volume.Flags.DELETE,
                            Volume.Flags.DRBD_DELETE,
                            Volume.Flags.CLONING
                        ))
                        {
                            // `drbdadm adjust` just deleted that volume or an exception was thrown.
                            drbdVlmData.setExists(false);
                        }
                        else
                        {
                            drbdVlmData.setExists(true);
                            drbdVlmData.setDevicePath(generateDevicePath(drbdVlmData));
                            drbdVlmData.setSizeState(Size.AS_EXPECTED);
                        }
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

    private void recoverAfterSkipdisk(DrbdRscData<Resource> drbdRscData)
        throws ExtCmdFailedException, AccessDeniedException, DatabaseException, InvalidKeyException
    {
        Props rscProps = drbdRscData.getAbsResource().getProps(workerCtx);
        String ids = rscProps
            .getProp(
                InternalApiConsts.KEY_DRBD_NODE_IDS_TO_RESET,
                ApiConsts.NAMESPC_STLT + "/" + InternalApiConsts.NAMESPC_DRBD
            );
        if (ids != null && !ids.isBlank())
        {
            forgetPeersCleanup(drbdRscData, ids);
        }

        rscProps.removeProp(
            InternalApiConsts.KEY_DRBD_NODE_IDS_TO_RESET,
            ApiConsts.NAMESPC_STLT + "/" + InternalApiConsts.NAMESPC_DRBD
        );
    }

    private void forgetPeersCleanup(DrbdRscData<Resource> drbdRscData, String ids)
        throws ExtCmdFailedException, AccessDeniedException
    {
        List<String> nodeIds = new ArrayList<>();
        if (ids != null && !ids.isEmpty())
        {
            nodeIds.addAll(
                Arrays.asList(ids.split(InternalApiConsts.KEY_BACKUP_NODE_ID_SEPERATOR))
            );
        }

        for (DrbdRscData<Resource> rscData : drbdRscData.getRscDfnLayerObject().getDrbdRscDataList())
        {
            nodeIds.remove("" + rscData.getNodeId().value);
        }
        for (String strId : nodeIds)
        {
            int nodeId = Integer.parseInt(strId);
            if (drbdRscData.getNodeId().value != nodeId)
            {
                try
                {
                    drbdUtils.forgetPeer(drbdRscData.getSuffixedResourceName(), nodeId);
                }
                catch (ExtCmdFailedException exc)
                {
                    errorReporter.logDebug("ignoring error in forget-peer %d after restoring", nodeId);
                }
            }
        }

        drbdUtils.adjust(
            drbdRscData,
            false,
            false,
            false
        );
    }

    private boolean needsNewMetaData(DrbdVlmData<Resource> drbdVlmData, @Nullable String drbdSetupStatusOut)
        throws AccessDeniedException, StorageException
    {
        boolean isRscUp = drbdSetupStatusOut != null ?
            drbdSetupStatusOut.contains(drbdVlmData.getRscLayerObject().getSuffixedResourceName() + " role:") :
            drbdUtils.drbdSetupStatusRscIsUp(drbdVlmData.getRscLayerObject().getSuffixedResourceName());
        StateFlags<DrbdRscFlags> flags = drbdVlmData.getRscLayerObject().getFlags();
        return flags.isUnset(workerCtx, DrbdRscFlags.INITIALIZED) &&
            flags.isSet(workerCtx, DrbdRscFlags.FORCE_NEW_METADATA) &&
            !isRscUp;
    }

    private boolean areBothResizeFlagsSet(DrbdVlmData<Resource> drbdVlmData) throws AccessDeniedException
    {
        StateFlags<Volume.Flags> vlmFlags = ((Volume) drbdVlmData.getVolume()).getFlags();
        return vlmFlags.isSet(workerCtx, Volume.Flags.RESIZE, Volume.Flags.DRBD_RESIZE) ||
            vlmFlags.isUnset(workerCtx, Volume.Flags.RESIZE, Volume.Flags.DRBD_RESIZE);
    }

    private boolean isFlagSet(DrbdVlmData<Resource> drbdVlmData, Volume.Flags... flagsRef) throws AccessDeniedException
    {
        StateFlags<Volume.Flags> vlmFlags = ((Volume) drbdVlmData.getVolume()).getFlags();
        return vlmFlags.isSet(workerCtx, flagsRef);
    }

    private boolean needsResize(DrbdVlmData<Resource> drbdVlmData) throws AccessDeniedException, StorageException
    {
        // A resize should not be called on a resize without a disk
        // there was a bug in pre 0.9.2 versions where diskless would be chosen for the resize command
        boolean isResizeFlagSet = ((Volume) drbdVlmData.getVolume()).getFlags()
            .isSet(workerCtx, Volume.Flags.DRBD_RESIZE);
        boolean needsResize = isResizeFlagSet && drbdVlmData.hasDisk();

        /* No way to query DRBD size on Windows. The block
         * device only exists when it is Primary.
         */

        if (needsResize && Platform.isLinux())
        {
            long sizeInSectors = SysBlockUtils.getDrbdSizeInSectors(
                extCmdFactory,
                drbdVlmData.getVlmDfnLayerObject().getMinorNr().value
            );
            long actualSizeInKib = sizeInSectors / 2;
            if (drbdVlmData.getUsableSize() != actualSizeInKib)
            {
                if (drbdVlmData.getUsableSize() > actualSizeInKib)
                {
                    drbdVlmData.setSizeState(Size.TOO_SMALL);
                }
                else
                {
                    drbdVlmData.setSizeState(Size.TOO_LARGE);
                }
            }
            else
            {
                drbdVlmData.setSizeState(Size.AS_EXPECTED);
                needsResize = false;
            }
        }

        return needsResize;
    }

    private String generateDevicePath(DrbdVlmData<Resource> drbdVlmData)
    {
        return String.format(DRBD_DEVICE_PATH_FORMAT, drbdVlmData.getVlmDfnLayerObject().getMinorNr().value);
    }

    private void updateRequiresAdjust(DrbdRscData<?> drbdRscData)
    {
        drbdRscData.setAdjustRequired(
            adjustResourcesList == null || adjustResourcesList.contains(
                drbdRscData.getResourceName().displayValue.toLowerCase()));
    }

    private List<DrbdVlmData<Resource>> detachVolumesIfNecessary(DrbdRscData<Resource> drbdRscData)
        throws AccessDeniedException, StorageException, DatabaseException
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
                if (((Volume) drbdVlmData.getVolume()).getFlags().isSomeSet(
                    workerCtx,
                    Volume.Flags.DELETE,
                    Volume.Flags.DRBD_DELETE,
                    Volume.Flags.CLONING
                ))
                {
                    if (drbdVlmData.hasDisk() && !drbdVlmData.hasFailed())
                    {
                        volumesToDelete.add(drbdVlmData);
                    }
                }
                else
                if (rsc.getStateFlags().isSet(workerCtx, Resource.Flags.DISK_REMOVING))
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

    private void shrinkVolumesIfNecessary(DrbdRscData<Resource> drbdRscData)
        throws AccessDeniedException, StorageException, ResourceException
    {
        try
        {
            if (
                !drbdRscData.getAbsResource().getStateFlags()
                    .isSet(workerCtx, Resource.Flags.DRBD_DISKLESS)
            )
            {
                for (DrbdVlmData<Resource> drbdVlmData : drbdRscData.getVlmLayerObjects().values())
                {
                    if (needsResize(drbdVlmData) && drbdVlmData.getSizeState().equals(Size.TOO_LARGE))
                    {
                        drbdUtils.resize(
                            drbdVlmData,
                            false, // we dont need to --assume-clean when shrinking...
                            drbdVlmData.getUsableSize()
                        );
                        // DO NOT set size.AS_EXPECTED as we most likely want to grow a little
                        // bit again once the layers below finished shrinking
                    }
                }
            }
        }
        catch (ExtCmdFailedException exc)
        {
            throw new ResourceException(
                String.format("Failed to shrink DRBD resource %s", drbdRscData.getSuffixedResourceName()),
                exc
            );
        }
    }

    @Override
    public boolean isSuspendIoSupported()
    {
        return true;
    }

    @Override
    public void resumeIo(AbsRscLayerObject<Resource> rscDataRef) throws ExtCmdFailedException
    {
        drbdUtils.resumeIo((DrbdRscData<Resource>) rscDataRef);
    }

    @Override
    public void suspendIo(AbsRscLayerObject<Resource> rscDataRef) throws ExtCmdFailedException
    {
        drbdUtils.suspendIo((DrbdRscData<Resource>) rscDataRef);
    }

    @Override
    public void updateSuspendState(AbsRscLayerObject<Resource> rscDataRef) throws StorageException, DatabaseException
    {
        try
        {
            DrbdResource drbdRscState = drbdState.getDrbdResource(rscDataRef.getSuffixedResourceName());
            rscDataRef.setIsSuspended(
                drbdRscState != null && drbdRscState.getSuspendedUser() != null &&
                    drbdRscState.getSuspendedUser()
            );
        }
        catch (NoInitialStateException exc)
        {
            throw new StorageException("Need initial DRBD state", exc);
        }
    }

    private boolean hasExternalMd(DrbdVlmData<Resource> drbdVlmData)
    {
        return drbdVlmData.getMetaDiskPath() != null;
    }

    private boolean hasExternalMd(DrbdRscData<?> drbdRscDataRef)
    {
        return drbdRscDataRef.getChildren().size() > 1;
    }

    private boolean hasMetaData(DrbdVlmData<Resource> drbdVlmData)
        throws VolumeException, AccessDeniedException
    {
        String metaDiskPath = drbdVlmData.getMetaDiskPath();
        boolean externalMd = hasExternalMd(drbdVlmData);
        if (!externalMd)
        {
            // internal meta data
            metaDiskPath = drbdVlmData.getDataDevice();
        }

        MdSuperblockBuffer mdUtils = new MdSuperblockBuffer();
        try
        {
            mdUtils.readObject(extCmdFactory, metaDiskPath, externalMd);
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
                        externalMd ? "flex-external" : "internal"
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
        errorReporter.logTrace("Found metadata: %s", hasMetaData);
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
            errorReporter.logInfo("DRBD meta data created for %s/%d",
                drbdVlmData.getRscLayerObject().getSuffixedResourceName(),
                drbdVlmData.getVlmNr().getValue());
            drbdVlmData.setMetaDataIsNew(true);

            if (DrbdLayerUtils.skipInitSync(workerCtx, drbdVlmData))
            {
                String currentGi = getCurrentGiFromVlmDfnProp(drbdVlmData);

                String metaDiskPath = drbdVlmData.getMetaDiskPath();
                boolean internal = false;
                if (metaDiskPath == null)
                {
                    // internal metadata
                    metaDiskPath = drbdVlmData.getDataDevice();
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
                errorReporter.logInfo("DRBD skipping initial sync for %s/%s",
                    drbdVlmData.getRscLayerObject().getSuffixedResourceName(),
                    drbdVlmData.getVlmNr().getValue());
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

    private String getCurrentGiFromVlmDfnProp(DrbdVlmData<Resource> drbdVlmData)
        throws AccessDeniedException, ImplementationError, StorageException
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
        return currentGi;
    }

    private void updateResourceToCurrentDrbdState(DrbdRscData<Resource> drbdRscData)
        throws AccessDeniedException, StorageException, DatabaseException
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
                for (DrbdVlmData<Resource> drbdVlmData : drbdRscData.getVlmLayerObjects().values())
                {
                    drbdVlmData.setExists(false);
                }
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

                { // check promotion stuff, not used on Satellite yet
                    drbdRscData.setPromotionScore(drbdRscState.getPromotionScore());
                    drbdRscData.setMayPromote(drbdRscState.mayPromote());
                }

                { // check drbd connections
                    Resource localResource = drbdRscData.getAbsResource();
                    localResource.getResourceDefinition().streamResource(workerCtx)
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
                                        // if we are unintentionally diskless, we should check our metadata as soon as
                                        // we have our disk again
                                        drbdVlmData.setCheckMetaData(true);
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

                                    drbdVlmData.setFailed(false);
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
                            drbdVlmData.setExists(false);
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

                drbdRscData.setIsSuspended(
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

    private String readResFile(Path resFilePath) throws IOException
    {
        return Files.readString(resFilePath);
    }

    private List<String> regenerateAllResFile(Set<AbsRscLayerObject<Resource>> rscDataList)
    {
        List<String> notGeneratedList = new ArrayList<>();
        for (AbsRscLayerObject<Resource> rscLayerObject : rscDataList)
        {
            DrbdRscData<Resource> drbdRscData = (DrbdRscData<Resource>)rscLayerObject;
            try
            {
                if (drbdRscData.isResFileReady(workerCtx) &&
                    !drbdRscData.getAbsResource().getStateFlags().isSomeSet(
                        workerCtx, Flags.DRBD_DELETE, Flags.DELETE, Flags.INACTIVE))
                {
                    regenerateResFile(drbdRscData);
                }
                else
                {
                    notGeneratedList.add(drbdRscData.getResourceName().displayValue.toLowerCase());
                }
            }
            catch (AccessDeniedException|StorageException exc)
            {
                errorReporter.reportError(exc);
                notGeneratedList.add(drbdRscData.getResourceName().displayValue.toLowerCase());
            }
        }
        return notGeneratedList;
    }

    /**
     * Writes a new resfile if the content really changed.
     *
     * @param drbdRscData
     * @return True if a new res file was written otherwise false.
     * @throws AccessDeniedException
     * @throws StorageException
     */
    private boolean regenerateResFile(DrbdRscData<Resource> drbdRscData)
        throws AccessDeniedException, StorageException
    {
        boolean fileWritten = false;
        Path resFile = asResourceFile(drbdRscData, false, false);
        Path tmpResFile = asResourceFile(drbdRscData, true, false);

        List<DrbdRscData<Resource>> drbdPeerRscDataList = drbdRscData.getRscDfnLayerObject()
            .getDrbdRscDataList().stream()
            .filter(otherRscData -> !otherRscData.equals(drbdRscData) &&
                AccessUtils.execPrivileged(() -> DrbdLayerUtils.isDrbdResourceExpected(workerCtx, otherRscData)) &&
                AccessUtils.execPrivileged(
                    () -> !otherRscData.getAbsResource().getStateFlags().isSet(workerCtx, Resource.Flags.INACTIVE)
                )
            )
            .collect(Collectors.toList());

        String content = new ConfFileBuilder(
            errorReporter,
            workerCtx,
            drbdRscData,
            drbdPeerRscDataList,
            whitelistProps,
            stltCfgAccessor.getReadonlyProps(),
            drbdVersion
        ).build();

        String onDiskContent = "";
        if (drbdRscData.resFileExists())
        {
            try
            {
                onDiskContent = readResFile(resFile);
            }
            catch (NoSuchFileException nsfe)
            {
                errorReporter.logWarning("Expected resource file %s did not exist. Rewriting...", resFile.toString());
                drbdRscData.setResFileExists(false);
            }
            catch (IOException exc)
            {
                errorReporter.reportError(exc);
            }
        }

        if (!onDiskContent.equals(content))
        {
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
                Files.move(
                    tmpResFile,
                    resFile,
                    StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE
                );
                drbdRscData.setResFileExists(true);
                fileWritten = true;
            }
            catch (IOException ioExc)
            {
                String ioErrorMsg = ioExc.getMessage();
                throw new StorageException(
                    "Unable to move temporary DRBD resource file '" + tmpResFile + "' to resource directory.",
                    getAbortMsg(drbdRscData),
                    "Unable to move temporary DRBD resource file due to an I/O error",
                    "- Check whether enough free space is available for moving the file\n" +
                        "- Check whether the application has write access to the target directory\n" +
                        "- Check whether the storage is operating flawlessly",
                    "The error reported by the runtime environment or operating system is:\n" + ioErrorMsg,
                    ioExc
                );
            }
            errorReporter.logInfo("DRBD regenerated resource file: %s", resFile);
        }
        return fileWritten;
    }

    private void copyResFile(Path srcPath, Path dstPath, String errMsg, String errCause)
        throws StorageException
    {
        try
        {
            Files.copy(srcPath, dstPath, StandardCopyOption.REPLACE_EXISTING);
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
                errMsg,
                errCause,
                null,
                "- Check whether enough free space is available for the creation of the file\n" +
                    "- Check whether the application has write access to the target directory\n" +
                    "- Check whether the storage is operating flawlessly",
                "The error reported by the runtime environment or operating system is:\n" + ioErrorMsg,
                ioExc
            );
        }
    }

    private void copyResFileToBackup(DrbdRscData<Resource> drbdRscData) throws StorageException
    {
        Path resFile = asResourceFile(drbdRscData, false, false);
        Path backupFile = asBackupResourceFile(drbdRscData);
        String rscName = drbdRscData.getSuffixedResourceName();
        copyResFile(
            resFile,
            backupFile,
            String.format("Failed to create a backup of the resource file of resource '%s'", rscName),
            getAbortMsg(drbdRscData)
        );
    }

    private void restoreBackupResFile(DrbdRscData<Resource> drbdRscData) throws StorageException
    {
        String rscName = drbdRscData.getSuffixedResourceName();
        errorReporter.logError("Restoring resource file from backup: %s", rscName);
        Path backupFile = asBackupResourceFile(drbdRscData);
        Path resFile = asResourceFile(drbdRscData, false, false);
        copyResFile(
            backupFile,
            resFile,
            String.format("Failed to restore resource file from backup of resource '%s'", rscName),
            getAbortMsg(drbdRscData)
        );
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
            ResourceDefinition rscDfn = rsc.getResourceDefinition();

            if (rscDfn.getProps(workerCtx).getProp(InternalApiConsts.PROP_PRIMARY_SET) == null &&
                    !rsc.getStateFlags().isSet(workerCtx, Resource.Flags.DRBD_DISKLESS)
            )
            {
                boolean alreadyInitialized;
                try
                {
                    alreadyInitialized = !allVlmsMetaDataNew(drbdRscData);
                }
                catch (ExtCmdFailedException exc)
                {
                    throw new StorageException("Could not check if metadata is new", exc);
                }

                errorReporter.logTrace(
                    "Requesting primary on %s; already initialized: %b",
                    drbdRscData.getSuffixedResourceName(),
                    alreadyInitialized
                );
                // Send a primary request even when volumes have already been initialized so that the controller can
                // save DrbdPrimarySetOn so that subsequently added nodes do not request to be primary
                sendRequestPrimaryResource(
                    rscDfn.getName().getDisplayName(), // intentionally not suffixedRscName
                    rsc.getUuid(),
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
                    if (
                        !VolumeUtils.isVolumeThinlyBacked(drbdVlmData, false) ||
                            DrbdLayerUtils.isForceInitialSyncSet(workerCtx, drbdRscData)
                    )
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
                    setResourceUpToDate(drbdRscData);
                }


                /*
                 * since we just created this resource, becoming briefly primary should not be an issue.
                 * primary needs to be done with --force since we might have configured quorum, but did not give DRBD
                 * enough time to connect to peers.
                 *
                 * we need to be primary even if autoPromote is deactivated to create the filesystem
                 */
                try (var ignored = drbdUtils.primaryAutoClose(drbdRscData, true, false))
                {
                    MkfsUtils.makeFileSystemOnMarked(errorReporter, extCmdFactory, workerCtx, rsc);
                }
                catch (ExtCmdFailedException exc)
                {
                    throw new StorageException("Failed to become secondary again after creating filesystem", exc);
                }
            }
        }
        catch (InvalidKeyException invalidKeyExc)
        {
            throw new ImplementationError("Invalid hardcoded property key", invalidKeyExc);
        }

    }

    private boolean allVlmsMetaDataNew(DrbdRscData<Resource> rscState)
        throws AccessDeniedException, StorageException, ImplementationError, ExtCmdFailedException
    {
        boolean allNew = true;
        for (DrbdVlmData<Resource> drbdVlmData : rscState.getVlmLayerObjects().values())
        {
            boolean isMetadataNew = false;
            if (drbdVlmData.isMetaDataNew())
            {
                isMetadataNew = true;
            }
            else
            {
                String currentGiFromVlmDfn = getCurrentGiFromVlmDfnProp(drbdVlmData);
                String allGisFromMetaData;
                {
                    String metaDiskPath = drbdVlmData.getMetaDiskPath();
                    boolean externalMd = metaDiskPath != null;
                    if (!externalMd)
                    {
                        // internal meta data
                        metaDiskPath = drbdVlmData.getDataDevice();
                    }
                    allGisFromMetaData = drbdUtils.getCurrentGID(
                        metaDiskPath,
                        drbdVlmData.getVlmDfnLayerObject().getMinorNr().value,
                        externalMd ? "flex-external" : "internal"
                    );
                }
                String currentGiFromMetaData = allGisFromMetaData.split(":")[0];
                isMetadataNew = currentGiFromVlmDfn.equalsIgnoreCase(currentGiFromMetaData) ||
                    currentGiFromMetaData.equals(DRBD_NEW_GI);
            }

            if (!isMetadataNew)
            {
                allNew = false;
                break;
            }
        }
        return allNew;
    }

    private void setResourceUpToDate(DrbdRscData<Resource> drbdRscData) throws StorageException
    {
        waitForValidStateForPrimary(drbdRscData);

        try (var ignored = drbdUtils.primaryAutoClose(drbdRscData, true, false))
        {
            // setting to secondary because of two reasons:
            // * bug in drbdsetup: cannot down a primary resource
            // * let the user choose which satellite should be primary (or let it be handled by auto-promote)
            assert true; // "fix" checkstyle empty statement
        }
        catch (ExtCmdFailedException | StorageException cmdExc)
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
        final UUID rscUuid,
        boolean alreadyInitialized
    )
    {
        byte[] data = interComSerializer
            .onewayBuilder(InternalApiConsts.API_REQUEST_PRIMARY_RSC)
            .primaryRequest(rscName, rscUuid, alreadyInitialized)
            .build();

        controllerPeerConnector.getControllerPeer().sendMessage(data, InternalApiConsts.API_REQUEST_PRIMARY_RSC);
    }

    /*
     * DELETE method and its utilities
     */

    private Path asResourceFile(DrbdRscData<Resource> drbdRscData, boolean temp, boolean cygwinFormat)
    {
        String prefix;

        if (cygwinFormat)
        {
            prefix = platformStlt.sysRootCygwin();
        }
        else
        {
            prefix = platformStlt.sysRoot();
        }

        return Paths.get(
            prefix + LinStor.CONFIG_PATH,
            drbdRscData.getSuffixedResourceName() + (temp ? DRBD_CONFIG_TMP_SUFFIX : DRBD_CONFIG_SUFFIX)
        );
    }

    private Path asBackupResourceFile(DrbdRscData<Resource> drbdRscData)
    {
        return Paths.get(
            platformStlt.sysRoot() + LinStor.BACKUP_PATH,
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
    public @Nullable LocalPropsChangePojo setLocalNodeProps(Props localNodePropsRef)
    {
        // ignored
        return null;
    }

    @Override
    public boolean isDeleteFlagSet(AbsRscLayerObject<?> rscDataRef) throws AccessDeniedException
    {
        if (!(rscDataRef instanceof DrbdRscData))
        {
            throw new ImplementationError(
                "method called with unexpected rscData: " + rscDataRef.getClass().getCanonicalName()
            );
        }
        boolean isDeleteFlagSet;
        AbsResource<?> absRsc = rscDataRef.getAbsResource();
        if (absRsc instanceof Resource)
        {
            @SuppressWarnings("unchecked")
            DrbdRscData<Resource> drbdRscData = (DrbdRscData<Resource>) rscDataRef;
            isDeleteFlagSet = shouldDrbdDeviceBeDeleted(drbdRscData);
        }
        else
        {
            isDeleteFlagSet = false; // DRBD does not manage snapshots
        }

        return isDeleteFlagSet;
    }

    @Override
    public CloneSupportResult getCloneSupport(
        AbsRscLayerObject<?> sourceRscDataRef,
        AbsRscLayerObject<?> targetRscDataRef
    )
    {
        final CloneSupportResult ret;
        final boolean isSourceDrbd = sourceRscDataRef.getLayerKind().equals(DeviceLayerKind.DRBD);
        final boolean isTargetDrbd = targetRscDataRef.getLayerKind().equals(DeviceLayerKind.DRBD);
        if (isSourceDrbd)
        {
            final boolean hasSourceInternalMd = !hasExternalMd((DrbdRscData<?>) sourceRscDataRef);
            if (isTargetDrbd)
            {
                // passthrough is an option, but only if both agree on internal or external MD.
                final boolean hasTargetInternalMd = !hasExternalMd((DrbdRscData<?>) targetRscDataRef);

                if (hasSourceInternalMd == hasTargetInternalMd)
                {
                    ret = CloneSupportResult.PASSTHROUGH;
                }
                else
                {
                    ret = CloneSupportResult.TRUE;
                }
            }
            else
            {
                if (hasSourceInternalMd)
                {
                    ret = CloneSupportResult.TRUE;
                }
                else
                {
                    // if source has external MD and target has no DRBD at all, we can use PASSTHROUGH
                    ret = CloneSupportResult.PASSTHROUGH;
                }
            }
        }
        else
        {
            if (isTargetDrbd)
            {
                ret = CloneSupportResult.TRUE;
            }
            else
            {
                throw new ImplementationError("Neither source, nor target is a DRBD resource");
            }
        }
        return ret;
    }

    @Override
    public void openDeviceForClone(VlmProviderObject<?> vlmRef, String targetRscNameRef) throws StorageException
    {
        throw new ImplementationError("not implemented yet");
    }

    private void cloneLastKnownBlockDeviceFile(int srcMinorNr, int tgtMinorNr) throws IOException
    {
        String srcPath = String.format("/var/lib/drbd/drbd-minor-%d.lkbd", srcMinorNr);
        String tgtPath = String.format("/var/lib/drbd/drbd-minor-%d.lkbd", tgtMinorNr);

        Files.copy(Path.of(srcPath), Path.of(tgtPath), StandardCopyOption.REPLACE_EXISTING);
    }

    /**
     * If we clone from ZFS -> LVM, the LVM volume will be larger and the dd drbd metadata will not be at the device end
     * Therefore we have to make sure the metadata gets moved to the end.
     * DRBD keeps a .lkbd file in /var/lib/drbd that stores the last known metadata path/offset, we have to clone this
     * file to the new resource and afterward call drbdmeta check-resize (which will move the metadata to the end).
     * @param vlmSrc
     * @param vlmTgt
     * @param clonedPath
     * @throws StorageException
     */
    @Override
    public void processAfterClone(VlmProviderObject<?> vlmSrc, VlmProviderObject<?> vlmTgt, String clonedPath)
        throws StorageException
    {
        DrbdVlmData<Resource> drbdVlmData = (DrbdVlmData<Resource>) vlmTgt;
        try
        {
            AbsRscLayerObject<?> srcLayerData = vlmSrc.getRscLayerObject().getAbsResource().getLayerData(workerCtx);
            var optSrcDrbdLayer = LayerRscUtils.getRscDataByLayer(srcLayerData, DeviceLayerKind.DRBD)
                .stream().findFirst();
            if (optSrcDrbdLayer.isPresent())
            {
                var srcDrbdLayer = optSrcDrbdLayer.get();
                DrbdVlmData<Resource> drbdSrcVlmData = (DrbdVlmData<Resource>) srcDrbdLayer.getVlmProviderObject(
                    drbdVlmData.getVlmNr());
                if (!hasExternalMd(drbdSrcVlmData))
                {
                    // only internal metadata needs to be possibly moved.
                    String metaDiskPath = drbdVlmData.getDataDevice() != null ?
                        drbdVlmData.getDataDevice() : clonedPath;
                    if (metaDiskPath != null)
                    {
                        int srcMinorNr = drbdSrcVlmData.getVlmDfnLayerObject().getMinorNr().value;
                        int tgtMinorNr = drbdVlmData.getVlmDfnLayerObject().getMinorNr().value;
                        cloneLastKnownBlockDeviceFile(srcMinorNr, tgtMinorNr);
                        drbdUtils.drbdMetaCheckResize(metaDiskPath, tgtMinorNr);
                    }
                    else
                    {
                        errorReporter.logInfo("drbdmeta-check-resize: Skipping because MD device path is null.");
                    }
                }
            }
        }
        catch (IOException ioExc)
        {
            throw new StorageException("clone DRBD last known block device file failed.", ioExc);
        }
        catch (ExtCmdFailedException|AccessDeniedException exc)
        {
            throw new StorageException("drbdmeta check-resize failed", exc);
        }
    }

    @Override
    public DeviceLayerKind getKind()
    {
        return DeviceLayerKind.DRBD;
    }
}
