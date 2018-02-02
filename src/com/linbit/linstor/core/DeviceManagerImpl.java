package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.WorkQueue;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.SatelliteCoreServices;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.drbdstate.DrbdEventService;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.Privilege;
import com.linbit.locks.AtomicSyncPoint;
import com.linbit.locks.SyncPoint;
import org.slf4j.event.Level;

import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

class DeviceManagerImpl implements Runnable, SystemService, DeviceManager
{
    // CAUTION! Avoid locking the sched lock and satellite locks like the reconfigurationLock, rscDfnMapLock, etc.
    //          at the same time (nesting locks).
    //          The APIs that update resource definition, volume definition, resource, volume, etc. data with
    //          new information from the controller hold reconfigurationLock and other locks while applying
    //          updates, and then inform this device manager instance about the updates by taking the sched
    //          lock and calling notify(). It is quite likely that they still hold other locks when doing so,
    //          therefore no other locks should be taken while the sched lock is held, so as to avoid deadlock.
    private final Object sched = new Object();

    private final Satellite stltInstance;
    private final AccessContext wrkCtx;
    private final SatelliteCoreServices coreSvcs;
    private final ErrorReporter errLog;

    private StltUpdateTrackerImpl updTracker;

    // Tracks objects that require requesting updates from the controller
    private final StltUpdateTrackerImpl.UpdateBundle updPendingBundle = new StltUpdateTrackerImpl.UpdateBundle();

    // Tracks objects that are waiting to be updated with data received from the controller
    private final StltUpdateTrackerImpl.UpdateBundle rcvPendingBundle = new StltUpdateTrackerImpl.UpdateBundle();

    private Thread svcThr;

    private final AtomicBoolean runningFlag     = new AtomicBoolean(false);
    private final AtomicBoolean svcCondFlag     = new AtomicBoolean(false);
    private final AtomicBoolean waitUpdFlag     = new AtomicBoolean(true);
    private final AtomicBoolean fullSyncFlag    = new AtomicBoolean(false);
    private final AtomicBoolean shutdownFlag    = new AtomicBoolean(false);

    private final Set<ResourceName> deletedRscSet = new TreeSet<>();
    private final Set<VolumeDefinition.Key> deletedVlmSet = new TreeSet<>();

    private static final ServiceName DEV_MGR_SVC_NAME;
    static
    {
        try
        {
            DEV_MGR_SVC_NAME = new ServiceName("DeviceManager");
        }
        catch (InvalidNameException invName)
        {
            throw new ImplementationError(
                "The built-in name of the DeviceManager service is invalid",
                invName
            );
        }
    }

    public static final String SVC_INFO = "Manages storage, transport and replication resources";

    private ServiceName devMgrInstName;

    private DeviceHandler drbdHnd;

    private boolean stateAvailable;
    private DrbdEventService drbdEvent;

    private WorkQueue workQ;

    private long cycleNr = 0;

    DeviceManagerImpl(
        Satellite stltRef,
        AccessContext wrkCtxRef,
        SatelliteCoreServices coreSvcsRef,
        DrbdEventService drbdEventRef,
        WorkQueue workQRef
    )
    {
        stltInstance = stltRef;
        wrkCtx = wrkCtxRef;
        coreSvcs = coreSvcsRef;
        errLog = coreSvcsRef.getErrorReporter();
        drbdEvent = drbdEventRef;
        updTracker = new StltUpdateTrackerImpl(sched);
        svcThr = null;
        devMgrInstName = DEV_MGR_SVC_NAME;
        drbdHnd = new DrbdDeviceHandler(stltInstance, wrkCtx, coreSvcs);
        workQ = workQRef;

        drbdEvent.addDrbdStateChangeObserver(this);
        stateAvailable = drbdEvent.isDrbdStateAvailable();
    }

    /**
     * Dispatch resource to a specific handler depending on type
     */
    void dispatchResource(AccessContext wrkCtxRef, Resource rsc, SyncPoint phaseLockRef)
    {
        // Select the resource handler for the resource depeding on resource type
        // Currently, the DRBD resource handler is used for all resources
        DeviceHandlerInvocation devHndInv = new DeviceHandlerInvocation(drbdHnd, rsc, phaseLockRef);

        workQ.submit(devHndInv);
    }

    @Override
    public void setServiceInstanceName(ServiceName instanceName)
    {
        if (instanceName != null)
        {
            devMgrInstName = instanceName;
        }
    }

    @Override
    public void start() throws SystemServiceStartException
    {
        shutdownFlag.set(false);
        if (runningFlag.compareAndSet(false, true))
        {
            svcThr = new Thread(this);
            svcThr.setName(devMgrInstName.displayValue);
            svcThr.start();
        }
        // BEGIN DEBUG - turn on trace logging
        try
        {
            AccessContext privCtx = wrkCtx.clone();
            privCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);
            errLog.setTraceEnabled(privCtx, true);
        }
        catch (AccessDeniedException accExc)
        {
            errLog.logWarning(
                "Enabling TRACE logging failed (not authorized) -- worker context not authorized"
            );
        }
        // END DEBUG
    }

    @Override
    public void shutdown()
    {
        synchronized (sched)
        {
            shutdownFlag.set(true);
            svcCondFlag.set(true);
            sched.notify();
        }
    }

    @Override
    public void awaitShutdown(long timeout) throws InterruptedException
    {
        // Since svcThr may be set to null at any time when a currently running,
        // thread exits, copy the reference to avoid the race condition
        Thread waitThr = svcThr;
        if (waitThr != null)
        {
            waitThr.join(timeout);
        }
    }

    @Override
    public void drbdStateAvailable()
    {
        synchronized (sched)
        {
            stateAvailable = true;
            // Do not wait with running the device handerls until new update notifications arrive,
            // instead, apply all pending changes and then run the device handlers immediately
            waitUpdFlag.set(false);
            svcCondFlag.set(true);
            sched.notify();
        }
    }

    @Override
    public void drbdStateUnavailable()
    {
        stateAvailable = false;
    }

    @Override
    public void nodeUpdateApplied(Set<NodeName> nodeSet)
    {
        synchronized (sched)
        {
            for (NodeName nodeName : nodeSet)
            {
                rcvPendingBundle.updNodeMap.remove(nodeName);
            }
            if (rcvPendingBundle.isEmpty())
            {
                sched.notify();
            }
        }
    }

    @Override
    public void rscDefUpdateApplied(Set<ResourceName> rscDfnSet)
    {
        synchronized (sched)
        {
            for (ResourceName rscName : rscDfnSet)
            {
                rcvPendingBundle.updRscDfnMap.remove(rscName);
            }
            if (rcvPendingBundle.isEmpty())
            {
                sched.notify();
            }
        }
    }

    @Override
    public void storPoolUpdateApplied(Set<StorPoolName> storPoolSet)
    {
        synchronized (sched)
        {
            for (StorPoolName storPoolName : storPoolSet)
            {
                rcvPendingBundle.updStorPoolMap.remove(storPoolName);
            }
            if (rcvPendingBundle.isEmpty())
            {
                sched.notify();
            }
        }
    }

    @Override
    public void rscUpdateApplied(Map<ResourceName, Set<NodeName>> rscMap)
    {
        synchronized (sched)
        {
            rscUpdateAppliedImpl(rscMap);
            if (rcvPendingBundle.isEmpty())
            {
                sched.notify();
            }
        }
    }

    @Override
    public void updateApplied(
        Set<NodeName> nodeSet,
        Set<ResourceName> rscDfnSet,
        Set<StorPoolName> storPoolSet,
        Map<ResourceName, Set<NodeName>> rscMap
    )
    {
        synchronized (sched)
        {
            if (nodeSet != null)
            {
                for (NodeName nodeName : nodeSet)
                {
                    rcvPendingBundle.updNodeMap.remove(nodeName);
                }
            }
            if (rscDfnSet != null)
            {
                for (ResourceName rscName : rscDfnSet)
                {
                    rcvPendingBundle.updRscDfnMap.remove(rscName);
                }
            }
            if (storPoolSet != null)
            {
                for (StorPoolName storPoolName : storPoolSet)
                {
                    rcvPendingBundle.updStorPoolMap.remove(storPoolName);
                }
            }
            if (rscMap != null)
            {
                rscUpdateAppliedImpl(rscMap);
            }

            if (rcvPendingBundle.isEmpty())
            {
                sched.notify();
            }
        }
    }

    public void fullSyncApplied()
    {
        synchronized (sched)
        {
            // Clear any previously valid state
            updPendingBundle.clear();
            rcvPendingBundle.clear();

            fullSyncFlag.set(true);
            svcCondFlag.set(true);
            sched.notify();
        }
    }

    @Override
    public StltUpdateTracker getUpdateTracker()
    {
        return updTracker;
    }

    // Caller must hold the scheduler lock ('synchronized (sched)')
    private void rscUpdateAppliedImpl(Map<ResourceName, Set<NodeName>> rscMap)
    {
        for (Map.Entry<ResourceName, Set<NodeName>> entry : rscMap.entrySet())
        {
            ResourceName rscName = entry.getKey();
            Map<NodeName, UUID> pendNodeSet = rcvPendingBundle.updRscMap.get(rscName);
            if (pendNodeSet != null)
            {
                Set<NodeName> updNodeSet = entry.getValue();
                for (NodeName nodeName : updNodeSet)
                {
                    pendNodeSet.remove(nodeName);
                }

                if (pendNodeSet.isEmpty())
                {
                    rcvPendingBundle.updRscMap.remove(rscName);
                }
            }
        }
    }

    @Override
    public void run()
    {
        errLog.logTrace("DeviceManager service started");
        try
        {
            devMgrLoop();
        }
        finally
        {
            runningFlag.set(false);
            errLog.logTrace("DeviceManager service stopped");
        }
    }

    private void devMgrLoop()
    {

        SyncPoint phaseLock = new AtomicSyncPoint();

        // Tracks objects that need to be dispatched to a device handler
        final Set<ResourceName> dispatchRscSet = new TreeSet<>();

        while (!shutdownFlag.get())
        {
            errLog.logTrace("Begin DeviceManager cycle %d", cycleNr);

            try
            {
                boolean fullSyncApplied = fullSyncFlag.getAndSet(false);
                if (fullSyncApplied)
                {
                    errLog.logTrace("DeviceManager: Executing device handlers after full sync");

                    // Clear the previous state
                    dispatchRscSet.clear();

                    Lock rcfgRdLock = stltInstance.reconfigurationLock.readLock();
                    Lock rscDfnMapRdLock = stltInstance.rscDfnMapLock.readLock();

                    // Schedule all known resources for dispatching to the device handlers
                    try
                    {
                        rcfgRdLock.lock();
                        rscDfnMapRdLock.lock();
                        dispatchRscSet.addAll(stltInstance.rscDfnMap.keySet());
                    }
                    finally
                    {
                        rscDfnMapRdLock.unlock();
                        rcfgRdLock.unlock();
                    }
                }
                else
                {
                    // Collects update notifications
                    // Blocks if waitUpdFlag is set
                    phaseCollectUpdateNotifications();

                    // Set nonblocking collection of update notifications, so that if the
                    // device manager service restarts, and updates are pending receipt
                    // from the controller, collection of further update notifications
                    // will be nonblocking
                    waitUpdFlag.set(false);

                    // Requests updates from the controller
                    phaseRequestUpdateData(dispatchRscSet);

                    // Blocks until all updates have been received from the controller
                    phaseCollectUpdateData();
                }

                // Cancel nonblocking collection of update notifications
                waitUpdFlag.set(true);

                if (stateAvailable)
                {
                    phaseDispatchDeviceHandlers(phaseLock, dispatchRscSet);
                }
                else
                {
                    errLog.logTrace(
                        "Execution of device handlers skipped, because DRBD state tracking is currently inoperative."
                    );
                }
            }
            catch (SvcCondException scExc)
            {
                // Cancel service condition
                svcCondFlag.set(false);

                boolean shutdownRequested = shutdownFlag.getAndSet(false);
                if (shutdownRequested)
                {
                    break;
                }
            }
            catch (AccessDeniedException accExc)
            {
                errLog.reportError(
                    Level.ERROR,
                    new ImplementationError(
                        "The DeviceManager was started with an access context that does not have sufficient " +
                        "privileges to access all required information",
                        accExc
                    )
                );
                break;
            }
            catch (Exception exc)
            {
                errLog.reportError(
                    Level.ERROR,
                    new ImplementationError(
                        "The DeviceManager service caught an unhandled exception of type " +
                        exc.getClass().getSimpleName() + ".\n",
                        exc
                    )
                );
            }
            catch (ImplementationError implErr)
            {
                errLog.reportError(
                    Level.ERROR,
                    implErr
                );
            }
            finally
            {
                errLog.logTrace("End DeviceManager cycle %d", cycleNr);
                ++cycleNr;
            }
        }
    }

    private void phaseCollectUpdateNotifications()
        throws SvcCondException
    {
        synchronized (sched)
        {
            if (updPendingBundle.isEmpty())
            {
                errLog.logTrace("Collecting update notifications");
                // Do not block in this phase if updates have been requested from the controller
                // and are pending receipt
                updTracker.collectUpdateNotifications(updPendingBundle, svcCondFlag, waitUpdFlag.get());
                if (svcCondFlag.get())
                {
                    throw new SvcCondException();
                }
            }
        }
    }

    private void phaseRequestUpdateData(Set<ResourceName> dispatchRscSet)
        throws SvcCondException
    {
        errLog.logTrace("Requesting object updates from the controller");

        synchronized (sched)
        {
            // The set of objects that are pending receipt must be initialized before
            // sending the requests for updates, because receipt of updates races
            // with sending update requests.
            // Therefore, rcvPendingBundle must be prepared in the request phase
            // before requesting the updates instead of in the collect phase.
            updPendingBundle.copyUpdateRequestsTo(rcvPendingBundle);

            // Schedule all objects that will be updated for a device handler run
            dispatchRscSet.addAll(updPendingBundle.chkRscMap.keySet());
            dispatchRscSet.addAll(updPendingBundle.updRscDfnMap.keySet());
            dispatchRscSet.addAll(updPendingBundle.updRscMap.keySet());

            // Request updates from the controller
            requestNodeUpdates(updPendingBundle.updNodeMap);
            requestRscDfnUpdates(updPendingBundle.updRscDfnMap);
            requestRscUpdates(updPendingBundle.updRscMap);
            requestStorPoolUpdates(updPendingBundle.updStorPoolMap);

            updPendingBundle.clear();
        }
    }

    private void phaseCollectUpdateData()
        throws SvcCondException
    {
        boolean waitMsg = true;

        synchronized (sched)
        {
            // Wait until all requested updates are applied
            while (!svcCondFlag.get() && !rcvPendingBundle.isEmpty())
            {
                if (waitMsg)
                {
                    errLog.logTrace("Waiting for object updates to be received and applied");
                    waitMsg = false;
                }
                try
                {
                    sched.wait();
                }
                catch (InterruptedException ignored)
                {
                }
            }
            if (svcCondFlag.get())
            {
                throw new SvcCondException();
            }
        }

        errLog.logTrace("All object updates were received");
    }

    private void phaseDispatchDeviceHandlers(SyncPoint phaseLock, Set<ResourceName> dispatchRscSet)
        throws SvcCondException, AccessDeniedException
    {
        errLog.logTrace("Dispatching resources to device handlers");

        synchronized (sched)
        {
            // Add any check requests that were received in the meantime
            // into the dispatch set and clear the check requests
            dispatchRscSet.addAll(updPendingBundle.chkRscMap.keySet());
            updPendingBundle.chkRscMap.clear();
        }

        // BEGIN DEBUG
        // ((DrbdDeviceHandler) drbdHnd).debugListSatelliteObjects();
        // END DEBUG

        if (!dispatchRscSet.isEmpty())
        {
            stltInstance.reconfigurationLock.readLock().lock();
            try
            {
                NodeData localNode = stltInstance.getLocalNode();
                for (ResourceName rscName : dispatchRscSet)
                {
                    // Dispatch resources that were affected by changes to worker threads
                    // and to the resource's respective handler
                    ResourceDefinition rscDfn = stltInstance.rscDfnMap.get(rscName);
                    if (rscDfn != null)
                    {
                        Resource rsc = rscDfn.getResource(wrkCtx, localNode.getName());
                        if (rsc != null)
                        {
                            dispatchResource(wrkCtx, rsc, phaseLock);
                        }
                        else
                        {
                            errLog.logWarning(
                                "Dispatch request for resource definition '" + rscName.displayValue +
                                "' which has no corresponding resource object on this satellite"
                            );
                        }
                    }
                    else
                    {
                        errLog.logWarning(
                            "Dispatch request for resource definition '" + rscName.displayValue +
                            "' which is unknown to this satellite"
                        );
                    }
                }
                dispatchRscSet.clear();

                errLog.logTrace("Waiting for resource handlers to finish");
                // Wait until the phase advances from the current phase number after all
                // device handlers have finished
                phaseLock.await();
                errLog.logTrace("All resource handlers finished");

                // Cleanup deleted objects
                deletedObjectsCleanup();
            }
            finally
            {
                stltInstance.reconfigurationLock.readLock().unlock();
            }
        }
    }

    private void deletedObjectsCleanup()
        throws AccessDeniedException
    {
        final Set<NodeName> localDelNodeSet = new TreeSet<>();
        final Set<ResourceName> localDelRscSet  = new TreeSet<>();

        // Shallow-copy the sets to avoid having to mix locking the sched lock and
        // the satellite's reconfigurationLock, rscDfnMapLock
        synchronized (sched)
        {
            localDelRscSet.addAll(deletedRscSet);
            deletedRscSet.clear();
            // FIXME: All functionality for deleting volumes can probably be removed.
            //        Volumes are only deleted when a volume definition is deleted, which happens
            //        only on the controller. An update would then be received for the resource
            //        that contained the volume.
            deletedVlmSet.clear();
        }

        Lock rcfgRdLock = stltInstance.reconfigurationLock.readLock();
        Lock nodeMapWrLock = stltInstance.nodesMapLock.writeLock();
        Lock rscDfnMapWrLock = stltInstance.rscDfnMapLock.writeLock();

        SatelliteTransactionMgr transMgr = new SatelliteTransactionMgr();
        rcfgRdLock.lock();
        try
        {
            rscDfnMapWrLock.lock();
            try
            {
                for (ResourceName curRscName : localDelRscSet)
                {
                    ResourceDefinition curRscDfn = stltInstance.rscDfnMap.get(curRscName);
                    if (curRscDfn != null)
                    {
                        // Delete the resource from all nodes
                        Map<NodeName, Resource> rscMap = new TreeMap<>();
                        curRscDfn.copyResourceMap(wrkCtx, rscMap);
                        for (Resource delRsc : rscMap.values())
                        {
                            Node peerNode = delRsc.getAssignedNode();
                            delRsc.setConnection(transMgr);
                            delRsc.delete(wrkCtx);
                            if (peerNode != stltInstance.getLocalNode())
                            {
                                if (!(peerNode.getResourceCount() >= 1))
                                {
                                    // This satellite does no longer have any peer resources
                                    // on the peer node, so is does not need to know about this
                                    // peer node any longer, therefore
                                    // remember to delete the peer node too
                                    localDelNodeSet.add(peerNode.getName());
                                }
                            }
                        }
                        transMgr.commit();
                        // Since the local node no longer has the resource, it also does not need
                        // to know about the resource definition any longer, therefore
                        // delete the resource definition as well
                        stltInstance.rscDfnMap.remove(curRscName);
                    }
                }
            }
            finally
            {
                rscDfnMapWrLock.unlock();
            }

            nodeMapWrLock.lock();
            try
            {
                for (NodeName curNodeName : localDelNodeSet)
                {
                    stltInstance.nodesMap.remove(curNodeName);
                }
            }
            finally
            {
                nodeMapWrLock.unlock();
            }
        }
        catch (SQLException ignored)
        {
            // Satellite; does not throw SQLExceptions, because the database update methods
            // are no-ops -> ignored
        }
        finally
        {
            rcfgRdLock.unlock();
        }
    }

    private void requestNodeUpdates(Map<NodeName, UUID> nodesMap)
    {
        StltApiCallHandler apiCallHandler = stltInstance.getApiCallHandler();
        for (Entry<NodeName, UUID> entry : nodesMap.entrySet())
        {
            errLog.logTrace("Requesting update for node '" + entry.getKey().displayValue + "'");
            apiCallHandler.requestNodeUpdate(
                entry.getValue(),
                entry.getKey()
            );
        }
    }

    private void requestRscDfnUpdates(Map<ResourceName, UUID> rscDfnMap)
    {
        StltApiCallHandler apiCallHandler = stltInstance.getApiCallHandler();
        for (Entry<ResourceName, UUID> entry : rscDfnMap.entrySet())
        {
            errLog.logTrace("Requesting update for resource definition '" + entry.getKey().displayValue + "'");
            apiCallHandler.requestRscDfnUpate(
                entry.getValue(),
                entry.getKey()
            );
        }
    }

    private void requestRscUpdates(Map<ResourceName, Map<NodeName, UUID>> updRscMap)
    {
        StltApiCallHandler apiCallHandler = stltInstance.getApiCallHandler();
        for (Entry<ResourceName, Map<NodeName, UUID>> entry : updRscMap.entrySet())
        {
            ResourceName rscName = entry.getKey();
            Map<NodeName, UUID> nodes = entry.getValue();
            for (Entry<NodeName, UUID> nodeEntry : nodes.entrySet())
            {
                errLog.logTrace("Requesting update for resource '" + entry.getKey().displayValue + "'");
                apiCallHandler.requestRscUpdate(
                    nodeEntry.getValue(),
                    nodeEntry.getKey(),
                    rscName
                );
            }
        }
    }

    private void requestStorPoolUpdates(Map<StorPoolName, UUID> updStorPoolMap)
    {
        StltApiCallHandler apiCallHandler = stltInstance.getApiCallHandler();
        for (Entry<StorPoolName, UUID> entry : updStorPoolMap.entrySet())
        {
            errLog.logTrace("Requesting update for storage pool '" + entry.getKey().displayValue + "'");
            apiCallHandler.requestStorPoolUpdate(
                entry.getValue(),
                entry.getKey()
            );
        }
    }

    @Override
    public ServiceName getServiceName()
    {
        return DEV_MGR_SVC_NAME;
    }

    @Override
    public String getServiceInfo()
    {
        return SVC_INFO;
    }

    @Override
    public ServiceName getInstanceName()
    {
        return devMgrInstName;
    }

    @Override
    public boolean isStarted()
    {
        return runningFlag.get();
    }

    @Override
    public void notifyResourceDeleted(Resource rsc)
    {
        // Send delete notification to the controller
        Peer ctrlPeer = stltInstance.getControllerPeer();
        if (ctrlPeer != null)
        {
            String msgNodeName = rsc.getAssignedNode().getName().displayValue;
            String msgRscName = rsc.getDefinition().getName().displayValue;
            UUID rscUuid = rsc.getUuid();

            byte[] data = stltInstance.getApiCallHandler().getInterComSerializer()
                .builder(InternalApiConsts.API_NOTIFY_RSC_DEL, 1)
                .notifyResourceDeleted(msgNodeName, msgRscName, rscUuid)
                .build();

            if (data != null)
            {
                ctrlPeer.sendMessage(data);
            }
        }

        // Remember the resource for removal after the DeviceHandler instances have finished
        synchronized (sched)
        {
            deletedRscSet.add(rsc.getDefinition().getName());
        }
    }

    @Override
    public void notifyVolumeDeleted(Volume vlm)
    {
        // Send delete notification to the controller
        Peer ctrlPeer = stltInstance.getControllerPeer();
        if (ctrlPeer != null)
        {
            String msgNodeName = vlm.getResource().getAssignedNode().getName().displayValue;
            String msgRscName = vlm.getResource().getDefinition().getName().displayValue;

            byte[] data = stltInstance.getApiCallHandler().getInterComSerializer()
                .builder(InternalApiConsts.API_NOTIFY_VLM_DEL, 1)
                .notifyVolumeDeleted(
                    msgNodeName,
                    msgRscName,
                    vlm.getVolumeDefinition().getVolumeNumber().value,
                    vlm.getUuid()
                )
                .build();

            if (data != null)
            {
                ctrlPeer.sendMessage(data);
            }
        }

        // Remember the volume for removal after the DeviceHandler instances have finished
        synchronized (sched)
        {
            deletedVlmSet.add(new VolumeDefinition.Key(vlm));
        }
    }

    static class DeviceHandlerInvocation implements Runnable
    {
        private final DeviceHandler handler;
        private final Resource rsc;
        private final SyncPoint phaseLock;

        DeviceHandlerInvocation(DeviceHandler handlerRef, Resource rscRef, SyncPoint phaseLockRef)
        {
            handler = handlerRef;
            rsc = rscRef;
            phaseLock = phaseLockRef;
            phaseLock.register();
        }

        @Override
        public void run()
        {
            try
            {
                handler.dispatchResource(rsc);
            }
            finally
            {
                phaseLock.arrive();
            }
        }
    }

    class SvcCondException extends Exception
    {
    }
}
