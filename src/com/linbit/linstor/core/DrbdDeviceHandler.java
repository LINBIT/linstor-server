package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.drbd.DrbdAdm;
import com.linbit.drbd.md.MdException;
import com.linbit.drbd.md.MetaData;
import com.linbit.drbd.md.MetaDataApi;
import com.linbit.extproc.ExtCmdFailedException;
import com.linbit.linstor.ConfFile;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.MinorNumber;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.SatelliteCoreServices;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.drbdstate.DrbdConnection;
import com.linbit.linstor.drbdstate.DrbdResource;
import com.linbit.linstor.drbdstate.DrbdStateTracker;
import com.linbit.linstor.drbdstate.DrbdVolume;
import com.linbit.linstor.drbdstate.NoInitialStateException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageDriver;
import com.linbit.linstor.storage.StorageException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import org.slf4j.event.Level;

class DrbdDeviceHandler implements DeviceHandler
{
    private Satellite stlt;
    private AccessContext wrkCtx;
    private SatelliteCoreServices coreSvcs;
    private DrbdStateTracker drbdState;
    private DrbdAdm drbdUtils;
    private ErrorReporter errLog;
    private MetaDataApi drbdMd;

    private final short FIXME_PEERS = 7;
    private final int FIXME_STRIPES = 1;
    private final long FIXME_STRIPE_SIZE = 32;
    private final String FIXME_CONFIG_PATH = "/etc/drbd.d";

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
                                // FIXME: Get the storage volume path and name from the storage driver
                                vlmState.hasMetaData = drbdUtils.hasMetaData(
                                    "/dev/drbdpool/" + vlmState.storVlmName, vlmState.minorNr.value, "internal"
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
                        if (!vlmState.hasDisk)
                        {
                            createStorageVolume(rscDfn, vlmState);
                            vlmState.hasDisk = true;
                            vlmState.hasMetaData = false;
                        }

                        // TODO: Wait for the backend storage block device files to appear in the /dev directory
                        //       if the volume is supposed to have backend storage

                        // Set block device paths
                        Volume vlm = rsc.getVolume(vlmState.vlmNr);
                        if (vlmState.hasDisk)
                        {
                            // FIXME: Get the storage volume path and name from the storage driver
                            vlm.setBlockDevicePath(wrkCtx, "/dev/drbdpool/" + vlmState.storVlmName);
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
                        if (!vlmState.skip)
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
                                    "Operations on resource '" + rscName.displayValue + "' volume " +
                                    vlmState.vlmNr.value + " were aborted.",
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
                        adjustResource(rsc, rscDfn, localNodeName, rscState, vlmStateMap);
                    }
                    catch (ExtCmdFailedException cmdExc)
                    {
                        throw new ResourceException(
                            "Adjusting the DRBD state of resource '" + rscName.displayValue + " failed",
                            "Operations on resource '" + rscName.displayValue + " were aborted.",
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
                    "Actions for resource '" + rscName.displayValue + "' were aborted. " +
                    "DRBD state tracking is unavailable.",
                    "Evaluation of the current state of the resource '" + rscName.displayValue + "' was aborted",
                    "Tracking of the current state of DRBD is currently unavailable.",
                    "Actions will continue when DRBD state tracking has recovered.",
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

    /**
     * Ascertains the current status of a resource and determines the changes
     * required to reach the target state that was set for the resource
     */
    void evaluateResource(Resource rsc)
    {

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
        DrbdResource drbdRsc = drbdState.getDrbdResource(rsc.getDefinition().getName().displayValue);
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
        if (drbdRsc.getRole() == DrbdResource.Role.UNKNOWN)
        {
            rscState.requiresAdjust = true;
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
        throws AccessDeniedException, VolumeException, MdException, StorageException
    {
        // Evaluate volumes state by checking for the presence of backend-storage
        Volume vlm = rsc.getVolume(vlmState.vlmNr);
        VolumeDefinition vlmDfn = rscDfn.getVolumeDfn(wrkCtx, vlmState.vlmNr);
        errLog.logTrace(
            "Evaluating storage volume for resource '" + rscDfn.getName().displayValue + "' " +
            "volume " + vlmState.vlmNr.toString()
        );

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
                vlmState.driver = driver;
            }

            if (!vlmState.hasDisk)
            {
                if (vlmState.driver != null)
                {
                    long netSize = vlmDfn.getVolumeSize(wrkCtx);
                    long expectedSize = drbdMd.getGrossSize(
                        netSize, FIXME_PEERS, FIXME_STRIPES, FIXME_STRIPE_SIZE
                    );
                    try
                    {
                        driver.checkVolume(vlmState.storVlmName, expectedSize);
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
                else
                {
                    errLog.logTrace(
                        "Cannot find storage pool '" + spName.displayValue + "' for volume " +
                        vlmState.vlmNr.toString() + " on the local node '" + localNodeName + "'"
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
                "An invalid storage pool name is specified for resource '" + rscName.displayValue + "' volume " +
                vlmState.vlmNr.value,
                "Operations on resource " + rscName.displayValue + " volume " + vlmState.vlmNr.value +
                " were aborted",
                "The state of the volume's backend storage cannot be determined, because an invalid " +
                "storage pool name was specified for the volume",
                "Correct the property that selects the storage pool for this volume.\n" +
                "Note that the property may be set on the volume or may be inherited from other objects " +
                "such as the corresponding resource definition or the node to which the resource is " +
                "assigned.",
                detailsMsg,
                nameExc
            );
        }
    }

    private void createStorageVolume(
        ResourceDefinition rscDfn,
        VolumeState vlmState
    )
        throws StorageException, VolumeException, MdException
    {
        if (vlmState.driver != null)
        {
            vlmState.grossSize = drbdMd.getGrossSize(
                vlmState.netSize, FIXME_PEERS, FIXME_STRIPES, FIXME_STRIPE_SIZE
            );
            vlmState.driver.createVolume(vlmState.storVlmName, vlmState.grossSize);
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

    private void createVolumeMetaData(
        ResourceDefinition rscDfn,
        VolumeState vlmState
    )
        throws ExtCmdFailedException
    {
        ResourceName rscName = rscDfn.getName();
        drbdUtils.createMd(rscName, vlmState.vlmNr, FIXME_PEERS);
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
                "/etc/drbd.d/" + rscName.displayValue + ".res"
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
                "Operations on resource " + rscName.displayValue + " were aborted",
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

    /**
     * Creates a new resource with empty volumes or with volumes restored from snapshots
     */
    private void adjustResource(
        Resource rsc,
        ResourceDefinition rscDfn,
        NodeName localNodeName,
        ResourceState rscState,
        Map<VolumeNumber, VolumeState> vlmStateMap
    )
        throws AccessDeniedException, ExtCmdFailedException
    {
        ResourceName rscName = rscDfn.getName();
        drbdUtils.adjust(rscName, false, false, false, null);
    }

    /**
     * Restores snapshots into the volumes of a new resource
     */
    void restoreResource(Resource rsc)
    {

    }

    /**
     * Deletes a resource
     * (and will somehow have to inform the device manager, or directly the controller, if the resource
     * was successfully deleted)
     */
    void deleteResource(Resource rsc)
    {

    }

    /**
     * Takes a snapshot from a resource's volumes
     */
    void createSnapshot(Resource rsc)
    {

    }

    /**
     * Deletes a snapshot
     */
    void deleteSnapshot(Resource rsc)
    {

    }

    /**
     * Handles volume creation
     */
    void createVolume(Resource rsc, Volume vlm)
        throws AccessDeniedException, MdException
    {
        VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
        long netSizeKiB = vlmDfn.getVolumeSize(wrkCtx);
        long grossSizeKiB = drbdMd.getGrossSize(netSizeKiB, (short) 7, 1, 32);

        // TODO: create backend storage on the selected storage pool
    }

    /**
     * Handles volume deletion
     */
    void deleteVolume(Resource rsc, Volume vlm)
    {

    }

    /**
     * Creates a new empty volume
     */
    void newEmptyVolume(Resource rsc, Volume vlm)
    {

    }

    /**
     * Restores a volume snapshot into a new volume
     */
    void newVolumeFromSnapshot(Resource rsc, Volume vlm)
    {

    }

    public void debugListSatelliteObjects()
    {
        System.out.println();
        System.out.println("\u001b[1;31m== BEGIN DrbdDeviceHandler.debugListSatelliteObjects() ==\u001b[0m");
        System.out.printf(
            "localNode = %s\n",
            stlt.localNode.getName().displayValue
        );
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
        Node localNode = stlt.localNode;
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
        boolean isPresent = false;
        boolean requiresAdjust = false;
    }

    static class VolumeState
    {
        VolumeNumber vlmNr;
        MinorNumber minorNr;
        boolean skip = false;
        boolean isPresent = false;
        boolean hasDisk = false;
        // Assume there is existing meta data and then prove that there is not,
        // to avoid overwriting existing meta data upon failure to check
        boolean hasMetaData = true;
        // Indicates whether a check for meta data should be performed
        boolean checkMetaData = true;
        boolean diskFailed = false;
        StorageDriver driver = null;
        StorPoolName storPoolName = null;
        String storVlmName = null;
        long netSize = 0L;
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
