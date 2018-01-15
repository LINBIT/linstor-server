package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.drbd.DrbdAdm;
import com.linbit.drbd.md.MdException;
import com.linbit.drbd.md.MetaData;
import com.linbit.drbd.md.MetaDataApi;
import com.linbit.extproc.ExtCmdFailedException;
import com.linbit.linstor.ConfFile;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.MinorNumber;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceData;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.SatelliteCoreServices;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ProtoInterComSerializer;
import com.linbit.linstor.drbdstate.DrbdConnection;
import com.linbit.linstor.drbdstate.DrbdResource;
import com.linbit.linstor.drbdstate.DrbdStateTracker;
import com.linbit.linstor.drbdstate.DrbdVolume;
import com.linbit.linstor.drbdstate.NoInitialStateException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.IllegalMessageStateException;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageDriver;
import com.linbit.linstor.storage.StorageException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import org.slf4j.event.Level;

/* TODO
 *
 * createResource() -- handling of resources known to LINSTOR vs. rogue resources not known to LINSTOR
 *                     required restructuring
 * vlmState.skip -- needs a better definition
 * rscState should possibly contain all vlmStates
 * vlmState should probably know whether the volume is a LINSTOR volume or a volume only seen by DRBD
 */

class DrbdDeviceHandler implements DeviceHandler
{
    private Satellite stlt;
    private AccessContext wrkCtx;
    private SatelliteCoreServices coreSvcs;
    private DrbdStateTracker drbdState;
    private DrbdAdm drbdUtils;
    private ErrorReporter errLog;
    private MetaDataApi drbdMd;

    // Number of peer slots for DRBD meta data; this should be replaced with a property of the
    // resource definition or otherwise a system-wide default
    private final short FIXME_PEERS = 7;

    // Number of activity log stripes for DRBD meta data; this should be replaced with a property of the
    // resource definition, a property of the volume definition, or otherwise a system-wide default
    private final int FIXME_STRIPES = 1;

    // Number of activity log stripes; this should be replaced with a property of the resource definition,
    // a property of the volume definition, or or otherwise a system-wide default
    private final long FIXME_STRIPE_SIZE = 32;

    // Path to the DRBD configuration files; this should be replaced by some meaningful constant or possibly
    // a value configurable in the cluster configuration
    private final String FIXME_CONFIG_PATH = "/etc/drbd.d";

    // DRBD configuration file suffix; this should be replaced by a meaningful constant
    private final String DRBD_CONFIG_SUFFIX = ".res";

    DrbdDeviceHandler(Satellite stltRef, AccessContext wrkCtxRef, SatelliteCoreServices coreSvcsRef)
    {
        stlt = stltRef;
        wrkCtx = wrkCtxRef;
        coreSvcs = coreSvcsRef;
        drbdUtils = new DrbdAdm(FileSystems.getDefault().getPath(FIXME_CONFIG_PATH), coreSvcs);
        errLog = coreSvcsRef.getErrorReporter();
        drbdState = coreSvcsRef.getDrbdStateTracker();
        drbdMd = new MetaData();
    }

    /**
     * Entry point for the DeviceManager
     *
     * @param rsc The resource to perform changes on
     */
    @Override
    public void dispatchResource(Resource rsc)
    {
        errLog.logTrace(
            "DrbdDeviceHandler: dispatchRsc() - Begin actions: Resource '" +
            rsc.getDefinition().getName().displayValue + "'"
        );
        ResourceDefinition rscDfn = rsc.getDefinition();
        ResourceName rscName = rscDfn.getName();
        Node localNode = rsc.getAssignedNode();
        NodeName localNodeName = localNode.getName();
        try
        {
            // Volatile state information of the resource and its volumes
            ResourceState rscState = new ResourceState();
            Map<VolumeNumber, VolumeState> vlmStateMap = new TreeMap<>();
            {
                Iterator<Volume> vlmIter = rsc.iterateVolumes();
                while (vlmIter.hasNext())
                {
                    Volume vlm = vlmIter.next();
                    VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
                    VolumeNumber vlmNr = vlmDfn.getVolumeNumber();


                    VolumeState vlmState = new VolumeState(vlmNr, vlmDfn.getVolumeSize(wrkCtx));
                    vlmState.markedForDelete = vlm.getFlags().isSet(wrkCtx, Volume.VlmFlags.DELETE) ||
                        vlmDfn.getFlags().isSet(wrkCtx, VolumeDefinition.VlmDfnFlags.DELETE);
                    vlmState.storVlmName = rscDfn.getName().displayValue + "_" +
                        String.format("%05d", vlmNr.value);
                    vlmState.minorNr = vlmDfn.getMinorNr(wrkCtx);
                    vlmStateMap.put(vlmNr, vlmState);
                }
            }

            // Evaluate resource & volumes state by checking the DRBD state
            evaluateDrbdResource(rsc, rscDfn, rscState, vlmStateMap);

            debugPrintState(rscName, rscState, vlmStateMap);

            try
            {
                if (rsc.getStateFlags().isSet(wrkCtx, Resource.RscFlags.DELETE) ||
                    rscDfn.getFlags().isSet(wrkCtx, ResourceDefinition.RscDfnFlags.DELETE))
                {
                    deleteResource(rsc, rscDfn, localNode, rscState, vlmStateMap);
                }
                else if (rsc.isCreatePrimary() &&
                        !rscState.isPrimary)
                {
                    // set primary
                    errLog.logTrace("Setting resource primary on %s", rscName.getDisplayName());
                    setResourcePrimary(rsc);
                    ((ResourceData)rsc).unsetCreatePrimary();
                }
                else
                {
                    createResource(localNode, localNodeName, rscName, rsc, rscDfn, rscState, vlmStateMap);

                    if (rscDfn.getProps(wrkCtx).getProp(InternalApiConsts.PROP_PRIMARY_SET) == null)
                    {
                        errLog.logTrace("Requesting primary on %s", rscName.getDisplayName());
                        sendRequestPrimaryResource(
                            localNode.getPeer(wrkCtx),
                            rscDfn.getName().getDisplayName(),
                            rsc.getUuid().toString()
                        );
                    }
                }
            }
            catch (ResourceException rscExc)
            {
                errLog.reportProblem(Level.ERROR, rscExc, null, null, null);
            }
            catch (Exception exc)
            {
                // FIXME: Narrow exception handling
                errLog.reportError(exc);
            }
        }
        catch (AccessDeniedException accExc)
        {
            errLog.reportError(
                Level.ERROR,
                new ImplementationError(
                    "Worker access context not authorized to perform a required operation",
                    accExc
                )
            );
        }
        catch (NoInitialStateException drbdStateExc)
        {
            errLog.reportProblem(
                Level.ERROR,
                new LinStorException(
                    "DRBD state tracking is unavailable, operations on resource '" + rscName.displayValue +
                    "' were aborted.",
                    getAbortMsg(rscName),
                    "DRBD state tracking is unavailable",
                    "Operations will continue automatically when DRBD state tracking is recovered",
                    null,
                    null
                ),
                null, null,
                "This error was generated while attempting to evaluate the current state of the resource"
            );
        }
        catch (Exception exc)
        {
            errLog.reportError(
                Level.ERROR,
                new ImplementationError(
                    "Unhandled exception",
                    exc
                )
            );
        }

        errLog.logTrace(
            "DrbdDeviceHandler: dispatchRsc() - End actions: Resource '" +
            rsc.getDefinition().getName().displayValue + "'"
        );
    }

    private void sendRequestPrimaryResource(
        final Peer ctrlPeer,
        final String rscName,
        final String rscUuid
    )
    {
        byte[] data = stlt.getInterComSerializer().builder(InternalApiConsts.API_REQUEST_PRIMARY_RSC, 1)
                .primaryRequest(rscName, rscUuid)
                .build();

        if (data != null)
        {
            try
            {
                Message netComMsg = ctrlPeer.createMessage();
                netComMsg.setData(data);
                ctrlPeer.sendMessage(netComMsg);
            }
            catch (IllegalMessageStateException illStateExc)
            {
                throw new ImplementationError(
                    "Attempt to send a NetCom message that has an illegal state",
                    illStateExc
                );
            }
        }
    }

    private void evaluateDelDrbdResource(
        Resource rsc,
        ResourceDefinition rscDfn,
        ResourceState rscState,
        Map<VolumeNumber, VolumeState> vlmStateMap
    )
        throws NoInitialStateException, AccessDeniedException
    {
        // Check whether the resource is known to DRBD
        DrbdResource drbdRsc = drbdState.getDrbdResource(rscDfn.getName().displayValue);
        if (drbdRsc != null)
        {
            rscState.isPresent = true;
            evaluateDrbdRole(rscState, drbdRsc);
        }
        else
        {
            rscState.isPresent = false;
            rscState.isPrimary = false;
        }
    }

    private void evaluateDrbdResource(
        Resource rsc,
        ResourceDefinition rscDfn,
        ResourceState rscState,
        Map<VolumeNumber, VolumeState> vlmStateMap
    )
        throws NoInitialStateException, AccessDeniedException
    {
        // Evaluate resource & volumes state by checking the DRBD state
        DrbdResource drbdRsc = drbdState.getDrbdResource(rscDfn.getName().displayValue);
        if (drbdRsc != null)
        {
            rscState.isPresent = true;
            evaluateDrbdRole(rscState, drbdRsc);
            evaluateDrbdConnections(rsc, rscDfn, rscState, vlmStateMap, drbdRsc);
            evaluateDrbdVolumes(rscState, vlmStateMap, drbdRsc);
        }
        else
        {
            rscState.requiresAdjust = true;
        }
    }

    private void evaluateDrbdRole(
        ResourceState rscState,
        DrbdResource drbdRsc
    )
    {
        DrbdResource.Role rscRole = drbdRsc.getRole();
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

    private void evaluateDrbdConnections(
        Resource rsc,
        ResourceDefinition rscDfn,
        ResourceState rscState,
        Map<VolumeNumber, VolumeState> vlmStateMap,
        DrbdResource drbdRsc
    )
        throws NoInitialStateException, AccessDeniedException
    {
        // Check connections to peer resources
        // TODO: Check for missing connections
        // TODO: Check for connections that should not be there
        Iterator<Resource> peerRscIter = rscDfn.iterateResource(wrkCtx);
        while (!rscState.requiresAdjust && peerRscIter.hasNext())
        {
            Resource peerRsc = peerRscIter.next();

            // Skip the local resource
            if (peerRsc != rsc)
            {
                Node peerNode = rsc.getAssignedNode();
                NodeName peerNodeName = peerNode.getName();
                DrbdConnection drbdConn = drbdRsc.getConnection(peerNodeName.displayValue);
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
        }
    }

    private void evaluateDrbdVolumes(
        ResourceState rscState,
        Map<VolumeNumber, VolumeState> vlmStateMap,
        DrbdResource drbdRsc
    )
    {
        Map<VolumeNumber, DrbdVolume> volumes = drbdRsc.getVolumesMap();
        for (VolumeState vlmState : vlmStateMap.values())
        {
            DrbdVolume drbdVlm = volumes.remove(vlmState.vlmNr);
            if (drbdVlm != null)
            {
                vlmState.isPresent = true;
                DrbdVolume.DiskState diskState = drbdVlm.getDiskState();
                switch (diskState)
                {
                    case DISKLESS:
                        // TODO: Volume may be a client volume
                        // fall-through
                    case DETACHING:
                        // TODO: May be a transition from storage to client
                        // fall-through
                    case FAILED:
                        vlmState.diskFailed = true;
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
        if (!volumes.isEmpty())
        {
            // The DRBD resource has additional unknown volumes,
            // adjust the resource
            rscState.requiresAdjust = true;
        }
    }

    private void getVolumeStorageDriver(
        ResourceName rscName,
        Node localNode,
        Volume vlm,
        VolumeDefinition vlmDfn,
        VolumeState vlmState,
        Props nodeProps,
        Props rscProps,
        Props rscDfnProps
    )
        throws AccessDeniedException, VolumeException
    {
        Props vlmProps = vlm.getProps(wrkCtx);
        Props vlmDfnProps = vlmDfn.getProps(wrkCtx);

        PriorityProps vlmPrioProps = new PriorityProps(
            vlmProps, rscProps, vlmDfnProps, rscDfnProps, nodeProps
        );

        String spNameStr = null;
        try
        {
            spNameStr = vlmPrioProps.getProp(ApiConsts.KEY_STOR_POOL_NAME);
            if (spNameStr == null)
            {
                spNameStr = Controller.DEFAULT_STOR_POOL_NAME;
            }

            StorPoolName spName = new StorPoolName(spNameStr);
            StorageDriver driver = null;
            StorPool storagePool = localNode.getStorPool(wrkCtx, spName);
            if (storagePool != null)
            {
                driver = storagePool.getDriver(wrkCtx);
                if (driver != null)
                {
                    vlmState.driver = driver;
                    vlmState.storPoolName = spName;
                }
                else
                {
                    errLog.logTrace(
                        "Cannot find storage pool '" + spName.displayValue + "' for volume " +
                        vlmState.vlmNr.toString() + " on the local node '" + localNode.getName() + "'"
                    );
                }
            }
        }
        catch (InvalidKeyException keyExc)
        {
            // Thrown if KEY_STOR_POOL_NAME is invalid
            throw new ImplementationError(
                "The builtin name constant for storage pools contains an invalid string",
                keyExc
            );
        }
        catch (InvalidNameException nameExc)
        {
            // Thrown if the name of the storage pool that is selected somewhere in the hierarchy
            // is invalid
            String detailsMsg = null;
            if (spNameStr != null)
            {
                detailsMsg = "The faulty storage pool name is '" + spNameStr + "'";
            }
            throw new VolumeException(
                "An invalid storage pool name is specified for volume " + vlmState.vlmNr.value + " of resource '"
                + rscName.displayValue,
                getAbortMsg(rscName, vlmState.vlmNr),
                "An invalid storage pool name was specified for the volume",
                "Correct the property that selects the storage pool for this volume.\n" +
                "Note that the property may be set on the volume or may be inherited from other objects " +
                "such as the corresponding resource definition or the node to which the resource is " +
                "assigned.",
                detailsMsg,
                nameExc
            );
        }
    }

    private void evaluateStorageVolume(
        ResourceName rscName,
        Resource rsc,
        ResourceDefinition rscDfn,
        Node localNode,
        NodeName localNodeName,
        VolumeState vlmState,
        Props nodeProps,
        Props rscProps,
        Props rscDfnProps
    )
        throws AccessDeniedException, VolumeException, MdException
    {
        // Evaluate volumes state by checking for the presence of backend-storage
        Volume vlm = rsc.getVolume(vlmState.vlmNr);
        VolumeDefinition vlmDfn = rscDfn.getVolumeDfn(wrkCtx, vlmState.vlmNr);
        errLog.logTrace(
            "Evaluating storage volume for resource '" + rscDfn.getName().displayValue + "' " +
            "volume " + vlmState.vlmNr.toString()
        );

        if (!vlmState.hasDisk)
        {
            if (!vlmState.driverKnown)
            {
                getVolumeStorageDriver(rscName, localNode, vlm, vlmDfn, vlmState, nodeProps, rscProps, rscDfnProps);
            }
            if (vlmState.driver != null)
            {
                long netSize = vlmDfn.getVolumeSize(wrkCtx);
                long expectedSize = drbdMd.getGrossSize(
                    netSize, FIXME_PEERS, FIXME_STRIPES, FIXME_STRIPE_SIZE
                );
                try
                {
                    vlmState.driver.checkVolume(vlmState.storVlmName, expectedSize);
                    vlmState.hasDisk = true;
                    errLog.logTrace(
                        "Existing storage volume found for resource '" +
                        rscDfn.getName().displayValue + "' " + "volume " + vlmState.vlmNr.toString()
                    );
                }
                catch (StorageException ignored)
                {
                    // FIXME: The driver should return a boolean indicating whether the volume exists
                    //        and throw an exception only if the check failed, but not to indicate
                    //        that the volume does not exist
                    errLog.logTrace(
                        "Storage volume for resource '" + rscDfn.getName().displayValue + "' " +
                        "volume " + vlmState.vlmNr.toString() + " does not exist"
                    );
                }
            }
        }
    }

    private void createStorageVolume(
        ResourceDefinition rscDfn,
        VolumeState vlmState
    )
        throws MdException, VolumeException
    {
        if (vlmState.driver != null)
        {
            try
            {
                vlmState.grossSize = drbdMd.getGrossSize(
                    vlmState.netSize, FIXME_PEERS, FIXME_STRIPES, FIXME_STRIPE_SIZE
                );
                vlmState.driver.createVolume(vlmState.storVlmName, vlmState.grossSize);
            }
            catch (StorageException storExc)
            {
                throw new VolumeException(
                    "Storage volume creation failed for resource '" + rscDfn.getName().displayValue + "' volume " +
                    vlmState.vlmNr.value,
                    getAbortMsg(rscDfn.getName(), vlmState.vlmNr),
                    "Creation of the storage volume failed",
                    "- Check whether there is sufficient space in the storage pool selected for the volume\n" +
                    "- Check whether the storage pool is operating flawlessly\n",
                    null,
                    storExc
                );
            }
        }
        else
        {
            String message = "Storage volume creation failed for resource '" + rscDfn.getName().displayValue +
                "' volume " + vlmState.vlmNr;
            errLog.reportProblem(
                Level.ERROR,
                new StorageException(
                    message,
                    message,
                    "The selected storage pool driver for the volume is unavailable",
                    null,
                    null,
                    null
                ),
                null, null, null
            );
        }
    }

    private void deleteStorageVolume(
        ResourceDefinition rscDfn,
        VolumeState vlmState
    )
        throws VolumeException
    {
        if (vlmState.driver != null)
        {
            try
            {
                vlmState.driver.deleteVolume(vlmState.storVlmName);
            }
            catch (StorageException storExc)
            {
                throw new VolumeException(
                    "Deletion of the storage volume failed for resource '" + rscDfn.getName().displayValue +
                    "' volume " + vlmState.vlmNr.value,
                    getAbortMsg(rscDfn.getName(), vlmState.vlmNr),
                    "Deletion of the storage volume failed",
                    "- Check whether the storage pool is operating flawlessly\n",
                    null,
                    storExc
                );
            }
        }
        else
        {
            String message = "Storage volume deletion failed for resource '" + rscDfn.getName().displayValue +
                "' volume " + vlmState.vlmNr;
            errLog.reportProblem(
                Level.ERROR,
                new StorageException(
                    message,
                    message,
                    "The selected storage pool driver for the volume is unavailable",
                    null,
                    null,
                    null
                ),
                null, null, null
            );
        }
    }

    private void createVolumeMetaData(
        ResourceDefinition rscDfn,
        VolumeState vlmState
    )
        throws ExtCmdFailedException
    {
        ResourceName rscName = rscDfn.getName();
        drbdUtils.createMd(rscName, vlmState.vlmNr, FIXME_PEERS);
    }

    private void createResource(
        Node localNode,
        NodeName localNodeName,
        ResourceName rscName,
        Resource rsc,
        ResourceDefinition rscDfn,
        ResourceState rscState,
        Map<VolumeNumber, VolumeState> vlmStateMap
    )
        throws AccessDeniedException, ResourceException
    {
        Props nodeProps = rsc.getAssignedNode().getProps(wrkCtx);
        Props rscProps = rsc.getProps(wrkCtx);
        Props rscDfnProps = rscDfn.getProps(wrkCtx);

        for (VolumeState vlmState : vlmStateMap.values())
        {
            try
            {
                // Check backend storage
                evaluateStorageVolume(
                    rscName, rsc, rscDfn, localNode, localNodeName, vlmState,
                    nodeProps, rscProps, rscDfnProps
                );

                if (!vlmState.markedForDelete)
                {
                    Volume vlm = rsc.getVolume(vlmState.vlmNr);

                    if (vlm != null)
                    {
                        VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
                        if (!vlmState.driverKnown)
                        {
                            getVolumeStorageDriver(
                                rscName, localNode, vlm, vlmDfn,
                                vlmState, nodeProps, rscProps, rscDfnProps
                            );
                        }

                        // Check DRBD meta data
                        if (!vlmState.hasDisk)
                        {
                            System.out.println(
                                "Resource " + rscName.displayValue + " Volume " + vlmState.vlmNr.value +
                                " has no backend storage => hasMetaData = false, checkMetaData = false"
                            );
                            // If there is no disk, then there cannot be any meta data
                            vlmState.checkMetaData = false;
                            vlmState.hasMetaData = false;
                        }
                        else
                        if (vlmState.checkMetaData)
                        {
                            // Check for the existence of meta data
                            try
                            {
                                System.out.println(
                                    "Checking resource " + rscName.displayValue + " Volume " + vlmState.vlmNr.value +
                                    " meta data"
                                );
                                vlmState.hasMetaData = drbdUtils.hasMetaData(
                                    vlmState.driver.getVolumePath(vlmState.storVlmName),
                                    vlmState.minorNr.value, "internal"
                                );
                                System.out.println(
                                    "Resource " + rscName.displayValue + " Volume " + vlmState.vlmNr.value +
                                    " hasMetaData = " + vlmState.hasMetaData
                                );
                            }
                            catch (ExtCmdFailedException cmdExc)
                            {
                                errLog.reportError(Level.ERROR, cmdExc);
                            }
                        }

                        // Create backend storage if required
                        if (!vlmState.hasDisk && vlm != null)
                        {
                            createStorageVolume(rscDfn, vlmState);
                            vlmState.hasDisk = true;
                            vlmState.hasMetaData = false;
                        }

                        // TODO: Wait for the backend storage block device files to appear in the /dev directory
                        //       if the volume is supposed to have backend storage

                        // Set block device paths
                        if (vlmState.hasDisk)
                        {
                            String bdPath = vlmState.driver.getVolumePath(vlmState.storVlmName);
                            vlm.setBlockDevicePath(wrkCtx, bdPath);
                            vlm.setMetaDiskPath(wrkCtx, "internal");
                        }
                        else
                        {
                            vlm.setBlockDevicePath(wrkCtx, "none");
                            vlm.setMetaDiskPath(wrkCtx, null);
                        }
                        errLog.logTrace(
                            "Resource '" + rscName + "' volume " + vlmState.vlmNr.toString() +
                            " block device = %s, meta disk = %s",
                            vlm.getBlockDevicePath(wrkCtx),
                            vlm.getMetaDiskPath(wrkCtx)
                        );
                    }
                    else
                    {
                        // If there is no volume for the volumeState, then LINSTOR does not know about
                        // this volume, and the volume will later be removed from the resource
                        // when the resource is adjusted.
                        // Therefore, the volume is ignored, and no backend storage is created for the volume
                        vlmState.skip = true;
                    }
                }
            }
            catch (MdException mdExc)
            {
                vlmState.skip = true;
                reportMdException(mdExc, rscName, vlmState);
            }
            catch (VolumeException vlmExc)
            {
                vlmState.skip = true;
                errLog.reportProblem(Level.ERROR, vlmExc, null, null, null);
            }
            catch (StorageException storExc)
            {
                vlmState.skip = true;
                errLog.reportProblem(
                    Level.ERROR,
                    new VolumeException(
                        "The storage driver could not determine the block device path for " +
                        "volume " + vlmState.vlmNr + " of resource " + rscName.displayValue,
                        this.getAbortMsg(rscName, vlmState.vlmNr),
                        "The storage driver could not determine the block device path for the volume's " +
                        "backend storage",
                        "- Check whether the storage driver is configured correctly\n" +
                        "- Check whether any external programs required by the storage driver are\n" +
                        "  functional\n",
                        null,
                        storExc
                    ),
                    null,
                    null,
                    null
                );
            }
        }

        // Create DRBD resource configuration file
        System.out.println(
            "Creating resource " + rscName.displayValue + " configuration file"
        );
        createResourceConfiguration(rsc, rscDfn);

        // Create volume meta data
        for (VolumeState vlmState : vlmStateMap.values())
        {
            try
            {
                if (!(vlmState.skip || vlmState.markedForDelete))
                {
                    try
                    {
                        if (vlmState.hasDisk && !vlmState.hasMetaData)
                        {
                            System.out.println(
                                "Creating resource " + rscName.displayValue + " Volume " +
                                vlmState.vlmNr.value + " meta data"
                            );
                            createVolumeMetaData(rscDfn, vlmState);
                        }
                    }
                    catch (ExtCmdFailedException cmdExc)
                    {
                        throw new VolumeException(
                            "Meta data creation for resource '" + rscName.displayValue + "' volume " +
                            vlmState.vlmNr.value + " failed",
                            getAbortMsg(rscName),
                            "Meta data creation failed because the execution of an external command failed",
                            "- Check whether the required software is installed\n" +
                            "- Check whether the application's search path includes the location\n" +
                            "  of the external software\n" +
                            "- Check whether the application has execute permission for " +
                            "the external command\n",
                            null,
                            cmdExc
                        );
                    }
                }
            }
            catch (VolumeException vlmExc)
            {
                vlmState.skip = true;
                errLog.reportProblem(Level.ERROR, vlmExc, null, null, null);
            }
        }

        // Create the DRBD resource
        if (rscState.requiresAdjust)
        {
            System.out.println("Adjusting resource " + rscName.displayValue);
            try
            {
                drbdUtils.adjust(rscName, false, false, false, null);
            }
            catch (ExtCmdFailedException cmdExc)
            {
                throw new ResourceException(
                    "Adjusting the DRBD state of resource '" + rscName.displayValue + " failed",
                    getAbortMsg(rscName),
                    "The external command for adjusting the DRBD state of the resource failed",
                    "- Check whether the required software is installed\n" +
                    "- Check whether the application's search path includes the location\n" +
                    "  of the external software\n" +
                    "- Check whether the application has execute permission for the external command\n",
                    null,
                    cmdExc
                );
            }
        }

        // Delete volumes
        for (VolumeState vlmState : vlmStateMap.values())
        {
            try
            {
                if (vlmState.markedForDelete && !vlmState.skip)
                {
                    Volume vlm = rsc.getVolume(vlmState.vlmNr);
                    VolumeDefinition vlmDfn = vlm != null ? vlm.getVolumeDefinition() : null;
                    // If this is a volume state that describes a volume seen by DRBD but not known
                    // to LINSTOR (e.g., manually configured in DRBD), then no volume object and
                    // volume definition object will be present.
                    // The backing storage for such volumes is ignored, as they are not managed
                    // by LINSTOR.
                    if (vlm != null && vlmDfn != null)
                    {
                        if (!vlmState.driverKnown)
                        {
                            getVolumeStorageDriver(
                                rscName, localNode, vlm, vlmDfn, vlmState,
                                nodeProps, rscProps, rscDfnProps
                            );
                        }
                        if (vlmState.driver != null)
                        {
                            deleteStorageVolume(rscDfn, vlmState);
                        }
                    }
                }
            }
            catch (VolumeException vlmExc)
            {
                errLog.reportProblem(Level.ERROR, vlmExc, null, null, null);
            }
        }
        // TODO: Notify the controller of successful deletion of volumes

        // TODO: Wait for the DRBD resource to reach the target state
    }

    private void createResourceConfiguration(
        Resource rsc,
        ResourceDefinition rscDfn
    )
        throws AccessDeniedException, ResourceException
    {
        ResourceName rscName = rscDfn.getName();
        Map<ResourceName, Resource> peerResources = new TreeMap<>();
        {
            Iterator<Resource> peerRscIter = rscDfn.iterateResource(wrkCtx);
            while (peerRscIter.hasNext())
            {
                Resource peerRsc = peerRscIter.next();
                if (peerRsc != rsc)
                {
                    peerResources.put(peerRsc.getDefinition().getName(), peerRsc);
                }
            }
        }

        try (
            FileOutputStream resFileOut = new FileOutputStream(
                FIXME_CONFIG_PATH + "/" + rscName.displayValue + DRBD_CONFIG_SUFFIX
            )
        )
        {
            String content = ConfFile.asConfigFile(wrkCtx, rsc, peerResources.values());
            resFileOut.write(content.getBytes());
        }
        catch (IOException ioExc)
        {
            String ioErrorMsg = ioExc.getMessage();
            throw new ResourceException(
                "Creation of the DRBD configuration file for resource '" + rscName.displayValue +
                "' failed due to an I/O error",
                getAbortMsg(rscName),
                "Creation of the DRBD configuration file failed due to an I/O error",
                "- Check whether enough free space is available for the creation of the file\n" +
                "- Check whether the application has write access to the target directory\n" +
                "- Check whether the storage is operating flawlessly",
                "The error reported by the runtime environment or operating system is:\n" +
                ioErrorMsg != null ?
                ioErrorMsg :
                "The runtime environment or operating system did not provide a description of " +
                "the I/O error",
                ioExc
            );
        }
    }

    private void setResourcePrimary(
        Resource rsc
    )
        throws ResourceException
    {
        ResourceName rscName = rsc.getDefinition().getName();
        try {
            drbdUtils.primary(rscName, true, false);
        }
        catch (ExtCmdFailedException cmdExc)
        {
            throw new ResourceException(
                "Setting primary on the DRBD resource '" + rscName.getDisplayName() + " failed",
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
    }

    /**
     * Deletes a resource
     * (and will somehow have to inform the device manager, or directly the controller, if the resource
     * was successfully deleted)
     */
    private void deleteResource(
        Resource rsc,
        ResourceDefinition rscDfn,
        Node localNode,
        ResourceState rscState,
        Map<VolumeNumber, VolumeState> vlmStateMap
    )
        throws AccessDeniedException, ResourceException, NoInitialStateException
    {
        ResourceName rscName = rscDfn.getName();

        // Determine the state of the DRBD resource
        evaluateDelDrbdResource(rsc, rscDfn, rscState, vlmStateMap);

        // Shut down the DRBD resource if it is still active
        if (rscState.isPresent)
        {
            try
            {
                drbdUtils.down(rscName);
            }
            catch (ExtCmdFailedException cmdExc)
            {
                throw new ResourceException(
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
        }

        // TODO: Wait for the DRBD resource to disappear

        // Delete the DRBD resource configuration file
        deleteResourceConfiguration(rscName);

        boolean vlmDelFailed = false;
        // Delete backend storage volumes
        for (VolumeState vlmState : vlmStateMap.values())
        {
            Props nodeProps = rsc.getAssignedNode().getProps(wrkCtx);
            Props rscProps = rsc.getProps(wrkCtx);
            Props rscDfnProps = rscDfn.getProps(wrkCtx);

            try
            {
                Volume vlm = rsc.getVolume(vlmState.vlmNr);
                VolumeDefinition vlmDfn = vlm != null ? vlm.getVolumeDefinition() : null;
                // If this is a volume state that describes a volume seen by DRBD but not known
                // to LINSTOR (e.g., manually configured in DRBD), then no volume object and
                // volume definition object will be present.
                // The backing storage for such volumes is ignored, as they are not managed
                // by LINSTOR.
                if (vlm != null && vlmDfn != null)
                {
                    if (!vlmState.driverKnown)
                    {
                        getVolumeStorageDriver(
                            rscName, localNode, vlm, vlmDfn, vlmState,
                            nodeProps, rscProps, rscDfnProps
                        );
                    }
                    if (vlmState.driver != null)
                    {
                        deleteStorageVolume(rscDfn, vlmState);
                    }
                }
            }
            catch (VolumeException vlmExc)
            {
                vlmDelFailed = true;
                errLog.reportProblem(Level.ERROR, vlmExc, null, null, null);
            }
        }
        if (vlmDelFailed)
        {
            throw new ResourceException(
                "Deletion of resource '" + rscName.displayValue + "' failed because deletion of the resource's " +
                "volumes failed",
                getAbortMsg(rscName),
                "Deletion of at least one of the resource's volumes failed",
                "Review the reports and/or log entries for the failed operations on the resource's volumes\n" +
                "for more information on the cause of the error and possible correction measures",
                null
            );
        }

        // Notify the controller of successful deletion of the resource
        stlt.getDeviceManager().notifyResourceDeleted(rsc);
    }

    /**
     * Deletes the DRBD resource configuration file for a resource
     *
     * @param rscName Name of the resource that should have its DRBD configuration file deleted
     * @throws ResourceException if deletion of the DRBD configuration file fails
     */
    private void deleteResourceConfiguration(ResourceName rscName)
        throws ResourceException
    {
        try
        {
            FileSystem dfltFs = FileSystems.getDefault();
            Path cfgFilePath = dfltFs.getPath(FIXME_CONFIG_PATH, rscName.displayValue + DRBD_CONFIG_SUFFIX);
            Files.delete(cfgFilePath);

            // Double-check whether the file exists
            File cfgFile = cfgFilePath.toFile();
            if (cfgFile.exists())
            {
                throw new IOException(
                    "File still exists after a delete operation reported successful completion"
                );
            }
        }
        catch (NoSuchFileException ignored)
        {
            // Failed deletion of a file that does not exist in the first place
            // is not an error
        }
        catch (IOException ioExc)
        {
            String ioErrorMsg = ioExc.getMessage();
            throw new ResourceException(
                "Deletion of the DRBD configuration file for resource '" + rscName.displayValue +
                "' failed due to an I/O error",
                getAbortMsg(rscName),
                "Deletion of the DRBD configuration file failed due to an I/O error",
                "- Check whether the application has write access to the target directory\n" +
                "- Check whether the storage is operating flawlessly",
                "The error reported by the runtime environment or operating system is:\n" +
                ioErrorMsg != null ?
                ioErrorMsg :
                "The runtime environment or operating system did not provide a description of " +
                "the I/O error",
                ioExc
            );
        }
    }

    private String getAbortMsg(ResourceName rscName)
    {
        return "Operations on resource '" + rscName.displayValue + "' were aborted";
    }

    private String getAbortMsg(ResourceName rscName, VolumeNumber vlmNr)
    {
        return "Operations on volume " + vlmNr.value + " of resource '" + rscName.displayValue + "' were aborted";
    }

    public void debugListSatelliteObjects()
    {
        System.out.println();
        System.out.println("\u001b[1;31m== BEGIN DrbdDeviceHandler.debugListSatelliteObjects() ==\u001b[0m");
        if (stlt.getLocalNode() != null)
        {
            System.out.printf(
                "localNode = %s\n",
                stlt.getLocalNode().getName().displayValue
            );
        }
        else
        {
            System.out.printf("localNode = not initialized\n");
        }
        for (Node curNode : stlt.nodesMap.values())
        {
            System.out.printf(
                "Node %s\n",
                curNode.getName().displayValue
            );
        }
        for (ResourceDefinition curRscDfn : stlt.rscDfnMap.values())
        {
            try
            {
                System.out.printf(
                    "    RscDfn %-24s Port %5d\n",
                    curRscDfn.getName(), curRscDfn.getPort(wrkCtx).value
                );
            }
            catch (AccessDeniedException ignored)
            {
            }
        }
        Node localNode = stlt.getLocalNode();
        if (localNode != null)
        {
            try
            {
                Iterator<Resource> rscIter = localNode.iterateResources(wrkCtx);
                while (rscIter.hasNext())
                {
                    Resource curRsc = rscIter.next();
                    System.out.printf(
                        "Assigned resource %-24s:\n",
                        curRsc.getDefinition().getName().displayValue
                    );
                    Iterator<Volume> vlmIter = curRsc.iterateVolumes();
                    while (vlmIter.hasNext())
                    {
                        Volume curVlm = vlmIter.next();
                        VolumeDefinition curVlmDfn = curVlm.getVolumeDefinition();
                        try
                        {
                            System.out.printf(
                                "    Volume %5d Size %9d\n",
                                curVlm.getVolumeDefinition().getVolumeNumber().value,
                                curVlmDfn.getVolumeSize(wrkCtx)
                            );
                        }
                        catch (AccessDeniedException ignored)
                        {
                        }
                    }
                }

                Iterator<StorPool> storPoolIter = localNode.iterateStorPools(wrkCtx);
                while (storPoolIter.hasNext())
                {
                    try
                    {
                        StorPool curStorPool = storPoolIter.next();
                        StorageDriver driver = curStorPool.getDriver(wrkCtx);
                        String driverClassName = driver != null ? driver.getClass().getName() : "null";
                        System.out.printf(
                            "Storage pool %-24s Driver %s\n",
                            curStorPool.getName().displayValue,
                            driverClassName
                        );
                    }
                    catch (AccessDeniedException ignored)
                    {
                    }
                }
            }
            catch (AccessDeniedException ignored)
            {
            }
        }
        System.out.println("\u001b[1;31m== END DrbdDeviceHandler.debugListSatelliteObjects() ==\u001b[0m");
        System.out.println();
    }

    private void reportMdException(MdException mdExc, ResourceName rscName, VolumeState vlmState)
    {
        errLog.reportProblem(
            Level.ERROR,
            new VolumeException(
                "Meta data calculation for resource '" + rscName.displayValue + "' volume " +
                vlmState.vlmNr.value + " failed",
                "Operations on resource " + rscName.displayValue + " volume " + vlmState.vlmNr.value +
                " were aborted",
                "The calculation of the volume's DRBD meta data size failed",
                "Check whether the volume's properties, such as size, DRBD peer count and activity log " +
                "settings, are within the range supported by DRBD",
                mdExc.getMessage(),
                mdExc
            ),
            null, null, null
        );
    }

    private void debugPrintState(
        ResourceName rscName,
        ResourceState rscState,
        Map<VolumeNumber, VolumeState> vlmStateMap
    )
    {
        // BEGIN DEBUG
            StringBuilder rscActions = new StringBuilder();
            rscActions.append("Resource '").append(rscName.displayValue).append("'\n");
            rscActions.append("    isPresent = ").append(rscState.isPresent).append("\n");
            rscActions.append("    requiresAdjust = ").append(rscState.requiresAdjust).append("\n");
            rscActions.append("    isPrimary = ").append(rscState.isPrimary).append("\n");
            for (VolumeState vlmState : vlmStateMap.values())
            {
                rscActions.append("    Volume ").append(vlmState.vlmNr.value).append("\n");
                rscActions.append("        skip          = ").append(vlmState.skip).append("\n");
                rscActions.append("        isPresent     = ").append(vlmState.isPresent).append("\n");
                rscActions.append("        hasDisk       = ").append(vlmState.hasDisk).append("\n");
                rscActions.append("        diskFailed    = ").append(vlmState.diskFailed).append("\n");
                rscActions.append("        hasMetaData   = ").append(vlmState.hasMetaData).append("\n");
                rscActions.append("        checkMetaData = ").append(vlmState.checkMetaData).append("\n");
                rscActions.append("        netSize       = ").append(vlmState.netSize).append(" kiB\n");
                rscActions.append("        grossSize     = ").append(vlmState.grossSize).append(" kiB\n");
            }
            System.out.println(rscActions.toString());
            // END DEBUG
    }

    static class ResourceState
    {
        // Indicates whether the resource is currently active (in use by DRBD)
        boolean isPresent       = false;

        // Indicates whether the resource needs to be adjusted (typically using 'drbdadm adjust')
        boolean requiresAdjust  = false;

        // Indicates whether the resource is currently in the DRBD Primary role
        boolean isPrimary       = false;
    }

    static class VolumeState
    {
        VolumeNumber vlmNr;
        MinorNumber  minorNr;

        // Indicates whether the resource should be deleted
        boolean markedForDelete = false;

        // Whether to skip/ignore the volume in following steps
        boolean skip            = false;

        // Indicates whether the volume is currently active (in use by DRBD)
        boolean isPresent       = false;

        // Indicates whether a storage backend volume is present
        boolean hasDisk         = false;

        // Indicates whether DRBD meta data is present on the storage backend volume
        //
        // To avoid overwriting existing meta data if the check for meta data fails,
        // the initial assumption is that there is existing meta data, and the check
        // attempts to prove that there is no meta data
        boolean hasMetaData     = true;

        // Indicates whether a check for meta data should be performed
        boolean checkMetaData   = true;

        // Indicates whether DRBD thinks the volume's backend storage volume has failed
        boolean diskFailed      = false;

        // Indicates whether a lookup for the volume's StorageDriver has already been performed
        // Note that this does not imply that the driver reference is non-null
        boolean driverKnown     = false;

        // Reference to the storage driver for the storage backend volume
        StorageDriver driver    = null;

        // Name of the storage pool that is selected for the storage backend volume
        StorPoolName storPoolName = null;

        // Name of the storage backend volume as known to the storage driver
        String storVlmName      = null;

        // Net size (without DRBD meta data) of the volume in kiB
        long netSize = 0L;

        // Gross size (with internal DRBD meta data) of the volume in kiB
        long grossSize = 0L;

        VolumeState(VolumeNumber volNrRef, long netSizeSpec)
        {
            vlmNr = volNrRef;
            netSize = netSizeSpec;
        }
    }

    static class ResourceException extends LinStorException
    {
        public ResourceException(String message)
        {
            super(message);
        }

        public ResourceException(String message, Throwable cause)
        {
            super(message, cause);
        }

        public ResourceException(
            String message,
            String descriptionText,
            String causeText,
            String correctionText,
            String detailsText
        )
        {
            super(message, descriptionText, causeText, correctionText, detailsText, null);
        }

        public ResourceException(
            String message,
            String descriptionText,
            String causeText,
            String correctionText,
            String detailsText,
            Throwable cause
        )
        {
            super(message, descriptionText, causeText, correctionText, detailsText, cause);
        }
    }

    static class VolumeException extends LinStorException
    {
        public VolumeException(String message)
        {
            super(message);
        }

        public VolumeException(String message, Throwable cause)
        {
            super(message, cause);
        }

        public VolumeException(
            String message,
            String descriptionText,
            String causeText,
            String correctionText,
            String detailsText
        )
        {
            super(message, descriptionText, causeText, correctionText, detailsText, null);
        }

        public VolumeException(
            String message,
            String descriptionText,
            String causeText,
            String correctionText,
            String detailsText,
            Throwable cause
        )
        {
            super(message, descriptionText, causeText, correctionText, detailsText, cause);
        }
    }
}
