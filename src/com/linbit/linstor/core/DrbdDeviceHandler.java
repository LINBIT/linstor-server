package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.drbd.DrbdAdm;
import com.linbit.drbd.md.MdException;
import com.linbit.drbd.md.MetaData;
import com.linbit.drbd.md.MetaDataApi;
import com.linbit.extproc.ExtCmdFailedException;
import com.linbit.linstor.ConfFile;
import com.linbit.linstor.LinStorException;
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
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
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
        try
        {
            ResourceDefinition rscDfn = rsc.getDefinition();
            ResourceName rscName = rscDfn.getName();
            Node localNode = rsc.getAssignedNode();
            NodeName localNodeName = localNode.getName();

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
                    vlmStateMap.put(vlmNr, vlmState);
                }
            }

            // Evaluate resource & volumes state by checking the DRBD state
            try
            {
                evaluateDrbdResource(rsc, rscDfn, rscState, vlmStateMap);
            }
            catch (NoInitialStateException drbdStateExc)
            {
                errLog.reportProblem(
                    Level.ERROR,
                    new LinStorException(
                        "Actions for resource '" + rscName.displayValue + "' aborted. " +
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


            try
            {
                evaluateStorageVolumes(rsc, rscDfn, localNode, localNodeName, vlmStateMap);
            }
            catch (Exception exc)
            {
                // FIXME: Narrow exception handling
                errLog.reportError(exc);
            }

            // BEGIN DEBUG
            StringBuilder rscActions = new StringBuilder();
            rscActions.append("Resource '").append(rscName.displayValue).append("'\n");
            rscActions.append("    isPresent = ").append(rscState.isPresent).append("\n");
            rscActions.append("    requiresAdjust = ").append(rscState.requiresAdjust).append("\n");
            for (VolumeState vlmState : vlmStateMap.values())
            {
                rscActions.append("    Volume ").append(vlmState.vlmNr.value).append("\n");
                rscActions.append("        isPresent = ").append(vlmState.isPresent).append("\n");
                rscActions.append("        hasDisk     = ").append(vlmState.hasDisk).append("\n");
                rscActions.append("        isFailed    = ").append(vlmState.isFailed).append("\n");
                rscActions.append("        hasMetaData = ").append(vlmState.hasMetaData).append("\n");
            }
            System.out.println(rscActions.toString());
            // END DEBUG

            // Create DRBD resource configuration file
            try
            {
                createResourceConfiguration(rsc, rscDfn);
            }
            catch (IOException ioExc)
            {
                errLog.reportError(Level.ERROR, ioExc);
            }

            // Create volume backend storage
            for (VolumeState vlmState : vlmStateMap.values())
            {
                if (!vlmState.hasDisk)
                {
                    try
                    {
                        createStorageVolume(rscDfn, vlmState);
                        createVolumeMetaData(rscDfn, vlmState);
                    }
                    catch (StorageException storExc)
                    {
                        errLog.reportProblem(Level.ERROR, storExc, null, null, null);
                    }
                    catch (MdException mdExc)
                    {
                        String message = "Creation of backend storage for resource '" + rscDfn.getName().displayValue +
                            "' " + "volume " + vlmState.vlmNr.toString() + " failed";
                        String causeMsg = mdExc.getMessage();
                        if (causeMsg == null)
                        {
                            causeMsg = "DRBD meta data calculation failed";
                        }
                        errLog.reportProblem(
                            Level.ERROR,
                            new LinStorException(
                                message,
                                message,
                                causeMsg,
                                "Verify that the volume size as well as the following DRBD parameters are within " +
                                "the design limits:\n" +
                                "    peer count\n    AL stripe count\n    AL stripe size",
                                null,
                                mdExc
                            ),
                            null, null, null
                        );
                    }
                }
            }

            // Create DRBD resource
            try
            {
                createResource(rsc, rscDfn, localNodeName, rscState, vlmStateMap);
            }
            catch (IOException | ExtCmdFailedException ioExc)
            {
                errLog.reportError(Level.ERROR, ioExc);
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
        for (VolumeState vlmState : vlmStateMap.values())
        {
            DrbdVolume drbdVlm = drbdRsc.getVolume(vlmState.vlmNr);
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
                        vlmState.isFailed = true;
                        // fall-through
                    case NEGOTIATING:
                        // fall-through
                    case UNKNOWN:
                        // The local disk state should not be unknown,
                        // try adjusting anyways
                        // fall-through
                        rscState.requiresAdjust = true;
                        break;
                    case UP_TO_DATE:
                        // fall-through
                    case ATTACHING:
                        // fall-through
                    case CONSISTENT:
                        // fall-through
                    case INCONSISTENT:
                        // fall-through
                    case OUTDATED:
                        // fall-through
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
        }
    }

    private void evaluateStorageVolumes(
        Resource rsc,
        ResourceDefinition rscDfn,
        Node localNode,
        NodeName localNodeName,
        Map<VolumeNumber, VolumeState> vlmStateMap
    )
    {
        // Evaluate volumes state by checking for the presence of backend-storage
        try
        {
            Props nodeProps = rsc.getAssignedNode().getProps(wrkCtx);
            Props rscProps = rsc.getProps(wrkCtx);
            Props rscDfnProps = rscDfn.getProps(wrkCtx);

            for (VolumeState vlmState : vlmStateMap.values())
            {
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

                String spNameStr = vlmPrioProps.getProp(ApiConsts.KEY_STOR_POOL_NAME);
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
                        String storVlmName = rscDfn.getName().value + "_" +
                            String.format("%05d", vlmDfn.getVolumeNumber().value);
                        long netSize = vlmDfn.getVolumeSize(wrkCtx);
                        long expectedSize = drbdMd.getGrossSize(
                            netSize, FIXME_PEERS, FIXME_STRIPES, FIXME_STRIPE_SIZE
                        );
                        try
                        {
                            driver.checkVolume(storVlmName, expectedSize);
                            vlmState.hasDisk = true;
                            errLog.logTrace(
                                "Existing storage volume found for resource '" +
                                rscDfn.getName().displayValue + "' " + "volume " + vlmState.vlmNr.toString()
                            );
                        }
                        catch (StorageException storExc)
                        {
                            errLog.reportProblem(Level.TRACE, storExc, null, null, null);
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
        }
        catch (Exception exc)
        {
            // FIXME: Narrow to storage exceptions
            errLog.reportError(exc);
        }
    }

    private void createStorageVolume(
        ResourceDefinition rscDfn,
        VolumeState vlmState
    )
        throws StorageException, MdException
    {
        if (vlmState.driver != null)
        {
            vlmState.grossSize = drbdMd.getGrossSize(
                vlmState.netSize, FIXME_PEERS, FIXME_STRIPES, FIXME_STRIPE_SIZE
            );
            vlmState.driver.createVolume(
                rscDfn.getName().displayValue + "_" + String.format("%05d", vlmState.vlmNr.value),
                vlmState.grossSize
            );
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
        throws AccessDeniedException, IOException
    {
        ResourceName rscName = rscDfn.getName();
        Set<Resource> peerResources = new TreeSet<>();
        {
            Iterator<Resource> peerRscIter = rscDfn.iterateResource(wrkCtx);
            while (peerRscIter.hasNext())
            {
                Resource peerRsc = peerRscIter.next();
                if (peerRsc != rsc)
                {
                    peerResources.add(peerRsc);
                }
            }
        }

        try (
            FileOutputStream resFileOut = new FileOutputStream(
                "/etc/drbd.d/" + rscName.displayValue + ".res"
            )
        )
        {
            String content = ConfFile.asConfigFile(wrkCtx, rsc, peerResources);
            resFileOut.write(content.getBytes());
        }
    }

    /**
     * Creates a new resource with empty volumes or with volumes restored from snapshots
     */
    private void createResource(
        Resource rsc,
        ResourceDefinition rscDfn,
        NodeName localNodeName,
        ResourceState rscState,
        Map<VolumeNumber, VolumeState> vlmStateMap
    )
        throws AccessDeniedException, IOException, ExtCmdFailedException
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

    static class ResourceState
    {
        boolean isPresent = false;
        boolean requiresAdjust = false;
    }

    static class VolumeState
    {
        VolumeNumber vlmNr;
        boolean isPresent = false;
        boolean hasDisk = false;
        boolean hasMetaData = false;
        boolean isFailed = false;
        StorageDriver driver = null;
        StorPoolName storPoolName = null;
        long netSize;
        long grossSize;

        VolumeState(VolumeNumber volNrRef, long netSizeSpec)
        {
            vlmNr = volNrRef;
            netSize = netSizeSpec;
        }
    }
}
