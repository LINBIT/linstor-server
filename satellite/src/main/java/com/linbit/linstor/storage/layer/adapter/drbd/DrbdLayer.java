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
import com.linbit.linstor.Volume.VlmFlags;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.ResourceType;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.drbdstate.DrbdConnection;
import com.linbit.linstor.drbdstate.DrbdResource;
import com.linbit.linstor.drbdstate.DrbdStateStore;
import com.linbit.linstor.drbdstate.DrbdVolume;
import com.linbit.linstor.drbdstate.DrbdVolume.DiskState;
import com.linbit.linstor.drbdstate.NoInitialStateException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.layer.DeviceLayer;
import com.linbit.linstor.storage.layer.adapter.drbd.utils.ConfFileBuilder;
import com.linbit.linstor.storage.layer.adapter.drbd.utils.DrbdAdm;
import com.linbit.linstor.storage.layer.adapter.drbd.utils.MdSuperblockBuffer;
import com.linbit.linstor.storage.utils.VolumeUtils;
import com.linbit.utils.AccessUtils;

import static com.linbit.linstor.storage.utils.VolumeUtils.getBackingVolume;

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
import java.util.TreeMap;
import java.util.stream.Collectors;

public class DrbdLayer implements DeviceLayer
{
    private static final String DRBD_CONFIG_SUFFIX = ".res";
    private static final String DRBD_CONFIG_TMP_SUFFIX = ".res_tmp";

    private final AccessContext workerCtx;
    private final NotificationListener notificationListener;
    private final DrbdAdm drbdUtils;
    private final DrbdStateStore drbdState;
    private final ErrorReporter errorReporter;
    private final WhitelistProps whitelistProps;
    private final CtrlStltSerializer interComSerializer;
    private final ControllerPeerConnector controllerPeerConnector;

    // Number of activity log stripes for DRBD meta data; this should be replaced with a property of the
    // resource definition, a property of the volume definition, or otherwise a system-wide default
    private static final int FIXME_AL_STRIPES = 1;

    // Number of activity log stripes; this should be replaced with a property of the resource definition,
    // a property of the volume definition, or or otherwise a system-wide default
    private static final long FIXME_AL_STRIPE_SIZE = 32;

    public DrbdLayer(
        @SystemContext AccessContext workerCtxRef,
        NotificationListener notificationListenerRef,
        DrbdAdm drbdUtilsRef,
        DrbdStateStore drbdStateRef,
        ErrorReporter errorReporterRef,
        WhitelistProps whiltelistPropsRef,
        CtrlStltSerializer interComSerializerRef,
        ControllerPeerConnector controllerPeerConnectorRef
    )
    {
        workerCtx = workerCtxRef;
        notificationListener = notificationListenerRef;
        drbdUtils = drbdUtilsRef;
        drbdState = drbdStateRef;
        errorReporter = errorReporterRef;
        whitelistProps = whiltelistPropsRef;
        interComSerializer = interComSerializerRef;
        controllerPeerConnector = controllerPeerConnectorRef;
    }

    @Override
    public Map<Resource, StorageException> adjustTopDown(
        Collection<Resource> resources,
        Collection<Snapshot> snapshots
    )
        throws StorageException
    {
        Map<Resource, StorageException> exceptions = new TreeMap<>();
        for (Resource rsc : resources)
        {
            ResourceName rscName = rsc.getDefinition().getName();
            try
            {
                if (rsc.getStateFlags().isSet(workerCtx, RscFlags.DELETE))
                {
                    try
                    {
                        // delete whole resource
                        errorReporter.logTrace("Shutting down drbd resource %s", rscName);
                        drbdUtils.down(rscName);
                        Path resFile = asResourceFile(rscName, false);
                        errorReporter.logTrace("Deleting res file: %s ", resFile);
                        Files.deleteIfExists(resFile);
                        notificationListener.notifyResourceDeleted(rsc);
                    }
                    catch (ExtCmdFailedException cmdExc)
                    {
                        exceptions.put(
                            rsc,
                            new StorageException(
                                "Shutdown of the DRBD resource '" + rscName.displayValue + " failed",
                                getAbortMsg(rscName),
                                "The external command for stopping the DRBD resource failed",
                                "- Check whether the required software is installed\n" +
                                "- Check whether the application's search path includes the location\n" +
                                "  of the external software\n" +
                                "- Check whether the application has execute permission for the external command\n",
                                null,
                                cmdExc
                            )
                        );
                    }
                    catch (IOException exc)
                    {
                        exceptions.put(rsc, new StorageException("IOException while removing resource file", exc));
                    }
                }
                else
                {
                    // detatch volumes that should be deleted so that their backing disk can be removed
                    // by the storage layer.
                    List<Volume> volumesToDetatch = rsc.streamVolumes().filter(
                        this::hasVolumeDeleteFlagSet
                    )
                    .collect(Collectors.toList());

                    for (Volume vlm : volumesToDetatch)
                    {
                        try
                        {
                            VolumeNumber vlmNr = vlm.getVolumeDefinition().getVolumeNumber();
                            errorReporter.logTrace("Detaching volume %s/%d", rscName, vlmNr.value);
                            drbdUtils.detach(rscName, vlmNr);
                            vlm.delete(workerCtx); // only deletes the drbd-volume, not the storage volume
                        }
                        catch (ExtCmdFailedException exc)
                        {
                            exceptions.put(
                                rsc,
                                new StorageException(
                                    "Detaching volume '" + vlm.toString() + "' failed.",
                                    exc
                                )
                            );
                        }
                        catch (SQLException exc)
                        {
                            throw new ImplementationError(exc);
                        }
                    }
                }
                // no adjust - that will be done in adjustBottomUp

                // TODO: suspend IO for snapshots
            }
            catch (AccessDeniedException accDeniedExc)
            {
                throw new ImplementationError(accDeniedExc);
            }
        }
        return exceptions;
    }

    private boolean hasVolumeDeleteFlagSet(Volume vlm)
    {
        return AccessUtils.execPrivileged(() -> vlm.getFlags().isSet(workerCtx, VlmFlags.DELETE));
    }

    @Override
    public Map<Resource, StorageException> adjustBottomUp(
        Collection<Resource> resources,
        Collection<Snapshot> snapshots
    )
        throws StorageException
    {
        Map<Resource, StorageException> exceptions = new TreeMap<>();
        try
        {
            updateToCurrentDrbdStates(resources);

            for (Resource rsc : resources)
            {
                if (rsc.getStateFlags().isSet(workerCtx, RscFlags.DELETE))
                {
                    // resource was already deleted in adjustTopDown - nothing to do here
                }
                else
                {
                    DrbdRscDataStlt rscState = (DrbdRscDataStlt) rsc.getLayerData(workerCtx);

                    rscState.requiresAdjust = true;

                    try
                    {
                        List<Volume> createMetaData = new ArrayList<>();

                        Iterator<Volume> vlmsIt = rsc.iterateVolumes();
                        while (vlmsIt.hasNext())
                        {
                            Volume drbdVlm = vlmsIt.next();
                            Volume backingVlm = getBackingVolume(workerCtx, drbdVlm);

                            DrbdVlmDataStlt vlmState = (DrbdVlmDataStlt) drbdVlm.getLayerData(workerCtx);

                            VolumeDefinition drbdVlmDfn = drbdVlm.getVolumeDefinition();
                            DrbdVlmDfnDataStlt vlmDfnState = drbdVlmDfn.getLayerData(
                                workerCtx,
                                DrbdVlmDfnDataStlt.class
                            );

                            String backingDiskPath = backingVlm.getDevicePath(workerCtx);
                            if (backingDiskPath == null)
                            {
                                if (!backingVlm.getFlags().isSet(workerCtx, VlmFlags.DELETE))
                                {
                                    rscState.requiresAdjust = false;
                                    rscState.failed = true;
                                    break;
                                }
                            }
                            else
                            {
                                drbdVlm.setBackingDiskPath(workerCtx, backingDiskPath);

                                MdSuperblockBuffer mdUtils = new MdSuperblockBuffer();
                                mdUtils.readObject(backingDiskPath);

                                if (vlmState.checkMetaData)
                                {
                                    if (mdUtils.hasMetaData())
                                    {
                                        if (
                                            !drbdUtils.hasMetaData(
                                                backingDiskPath,
                                                vlmDfnState.getMinorNr().value,
                                                "internal"
                                            )
                                        )
                                        {
                                            throw new StorageException(
                                                "Corrupted drbd-metadata",
                                                null,
                                                "Linstor has found existing DRBD meta data, " +
                                                    "but drbdmeta could not read them",
                                                "Check if the DRBD-utils version match the DRBD kernel version. ",
                                                null
                                            );
                                        }
                                    }
                                    else
                                    {
                                        createMetaData.add(drbdVlm);
                                    }
                                }
                            }
                        }

                        if (!rscState.failed)
                        {
                            if (rscState.requiresAdjust)
                            {
                                regenerateResFile(rsc);

                                for (Volume drbdVlm : createMetaData)
                                {
                                    VolumeDefinition drbdVlmDfn = drbdVlm.getVolumeDefinition();

                                    DrbdVlmDataStlt vlmState = (DrbdVlmDataStlt) drbdVlm.getLayerData(workerCtx);
                                    drbdUtils.createMd(
                                        rsc.getDefinition().getName(),
                                        drbdVlmDfn.getVolumeNumber(),
                                        vlmState.peerSlots
                                    );
                                    vlmState.metaDataIsNew = true;
                                }

                                drbdUtils.adjust(
                                    rsc.getDefinition().getName(),
                                    false,
                                    false,
                                    false,
                                    null
                                );

                                condInitialOrSkipSync(rsc, rscState);
                            }
                        }
                    }
                    catch (ExtCmdFailedException | IOException exc)
                    {
                        exceptions.put(rsc, new StorageException("External command failed", exc));
                    }
                    catch (StorageException exc)
                    {
                        exceptions.put(rsc, exc);
                    }
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError("DRBD-Layer has not enough privileges", exc);
        }
        catch (SQLException sqlExc)
        {
            throw new ImplementationError(sqlExc);
        }
        catch (NoInitialStateException drbdStateExc)
        {
            throw new StorageException("DRBD state tracking is unavailable", drbdStateExc);
        }
        return exceptions;
    }

    private void updateToCurrentDrbdStates(Collection<Resource> resources)
        throws AccessDeniedException, SQLException, NoInitialStateException
    {
        try
        {
            errorReporter.logTrace("Synchronizing Linstor-state with DRBD-state");
            for (Resource rsc : resources)
            {
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
                        vlmState.allocatedSize = backingVlm.getUsableSize(workerCtx);
                        vlmState.peerSlots = peerSlots;
                        vlmState.alStripes =  FIXME_AL_STRIPES;
                        vlmState.alStripeSize =  FIXME_AL_STRIPE_SIZE;
                        vlmState.usableSize = new MetaData().getNetSize(
                            vlmState.allocatedSize,
                            vlmState.peerSlots,
                            vlmState.alStripes,
                            vlmState.alStripeSize
                        );
                        if (rsc.getStateFlags().isSet(workerCtx, RscFlags.DISKLESS))
                        {
                            vlmState.checkMetaData = false;
                        }

                        rscState.putVlmState(vlm.getVolumeDefinition().getVolumeNumber(), vlmState);
                    }
                    if (!drbdVolumes.isEmpty())
                    {
                        // The DRBD resource has additional unknown volumes,
                        // adjust the resource
                        rscState.requiresAdjust = true;
                    }
                }
            }
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError("Invalid hardcoded key", exc);
        }
        catch (IllegalArgumentException | MinSizeException | MaxSizeException | MinAlSizeException |
            MaxAlSizeException | AlStripesException | PeerCountException exc)
        {
            throw new ImplementationError(exc);
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
                    VolumeNumber vlmNr = vlm.getVolumeDefinition().getVolumeNumber();
                    if (!VolumeUtils.isVolumeThinlyBacked(workerCtx, vlm))
                    {
                        haveFatVlm = true;
                    }
                }

                // Set the resource primary (--force) to trigger an initial sync of all
                // fat provisioned volumes
                ((ResourceData) rsc).unsetCreatePrimary();
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

    @Override
    public void setLocalNodeProps(Props localNodePropsRef)
    {
        // ignored
    }
}
