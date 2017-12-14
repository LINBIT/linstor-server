package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.drbd.md.MdException;
import com.linbit.drbd.md.MetaData;
import com.linbit.drbd.md.MetaDataApi;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.SatelliteCoreServices;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolDefinition;
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
    private ErrorReporter errLog;
    private MetaDataApi drbdMd;

    private final short FIXME_PEERS = 7;
    private final int FIXME_STRIPES = 1;
    private final long FIXME_STRIPE_SIZE = 32;

    DrbdDeviceHandler(Satellite stltRef, AccessContext wrkCtxRef, SatelliteCoreServices coreSvcsRef)
    {
        stlt = stltRef;
        wrkCtx = wrkCtxRef;
        coreSvcs = coreSvcsRef;
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
        // TODO: Implement
        errLog.logTrace(
            "DrbdDeviceHandler: dispatchRsc() - Begin resource '" +
            rsc.getDefinition().getName().displayValue + "' check simulation (5 seconds)"
        );

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

                vlmStateMap.put(vlmNr, new VolumeState(vlmNr));
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
                    "Actions for resource '" + rscName.displayValue + "' aborted. DRBD state tracking is unavailable.",
                    "Evaluation of the current state of the resource '" + rscName.displayValue + "' was aborted",
                    "Tracking of the current state of DRBD is currently unavailable.",
                    "Actions will continue when DRBD state tracking has recovered.",
                    null
                ),
                null, null,
                "This error was generated while attempting to evaluate the current state of the resource"
            );
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

        // Evaluate volumes state by checking for the presence of backend-storage
        try
        {
            Props nodeProps = rsc.getAssignedNode().getProps(wrkCtx);
            Props rscProps = rsc.getProps(wrkCtx);
            Props rscDfnProps = rscDfn.getProps(wrkCtx);

            for (VolumeState vlmState : vlmStateMap.values())
            {
                if (!vlmState.hasDisk)
                {
                    Volume vlm = rsc.getVolume(vlmState.vlmNr);
                    VolumeDefinition vlmDfn = rscDfn.getVolumeDfn(wrkCtx, vlmState.vlmNr);
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
                    StorPoolDefinition storPoolDfn = stlt.storPoolDfnMap.get(spName);
                    if (storPoolDfn != null)
                    {
                        StorPool storagePool = storPoolDfn.getStorPool(wrkCtx, localNodeName);

                        StorageDriver driver = storagePool.getDriver(wrkCtx);

                        String storVlmName = rscDfn.getName().value + "_" + vlmDfn.getVolumeNumber().toString();
                        long netSize = vlmDfn.getVolumeSize(wrkCtx);
                        long expectedSize = drbdMd.getGrossSize(
                            netSize, FIXME_PEERS, FIXME_STRIPES, FIXME_STRIPE_SIZE
                        );
                        try
                        {
                            driver.checkVolume(storVlmName, expectedSize);
                        }
                        catch (StorageException storExc)
                        {
                            errLog.reportProblem(Level.TRACE, storExc, null, null, null);
                        }
                    }
                    else
                    {
                        errLog.logTrace(
                            "Cannot find storage pool '" + spName.displayValue + "' " +
                            "on the local node '" + localNodeName + "'"
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

        errLog.logTrace(
            "DrbdDeviceHandler: dispatchRsc() - End resource '" +
            rsc.getDefinition().getName().displayValue + "' check simulation"
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

    /**
     * Creates a new resource with empty volumes or with volumes restored from snapshots
     */
    void createResource(Resource rsc)
    {

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

        VolumeState(VolumeNumber volNrRef)
        {
            vlmNr = volNrRef;
        }
    }
}
