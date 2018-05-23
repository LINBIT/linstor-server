package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;

import static com.linbit.SatelliteLinstorModule.STLT_WORKER_POOL_NAME;

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
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.drbdstate.DrbdEventService;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.Privilege;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.transaction.SatelliteTransactionMgr;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.locks.AtomicSyncPoint;
import com.linbit.locks.SyncPoint;
import org.slf4j.event.Level;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

@Singleton
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

    private final AccessContext wrkCtx;
    private final ErrorReporter errLog;

    private final CoreModule.NodesMap nodesMap;
    private final CoreModule.ResourceDefinitionMap rscDfnMap;

    private final ReadWriteLock reconfigurationLock;
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock rscDfnMapLock;

    private final StltUpdateRequester stltUpdateRequester;
    private final ControllerPeerConnector controllerPeerConnector;
    private final CtrlStltSerializer interComSerializer;

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

    private final LinStorScope deviceMgrScope;
    private final Provider<TransactionMgr> transMgrProvider;

    private final StltSecurityObjects stltSecObj;
    private final Provider<DeviceHandlerInvocationFactory> devHandlerInvocFactoryProvider;

    private static final ServiceName DEV_MGR_NAME;
    static
    {
        try
        {
            DEV_MGR_NAME = new ServiceName("DeviceManager");
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
    private volatile boolean abortDevHndFlag;
    private DrbdEventService drbdEvent;

    private WorkQueue workQ;

    private long cycleNr = 0;

    private final StltApiCallHandlerUtils apiCallHandlerUtils;


    @Inject
    DeviceManagerImpl(
        @DeviceManagerContext AccessContext wrkCtxRef,
        ErrorReporter errorReporterRef,
        CoreModule.NodesMap nodesMapRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        StltUpdateRequester stltUpdateRequesterRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        CtrlStltSerializer interComSerializerRef,
        DrbdEventService drbdEventRef,
        @Named(STLT_WORKER_POOL_NAME) WorkQueue workQRef,
        DrbdDeviceHandler drbdDeviceHandlerRef,
        StltApiCallHandlerUtils apiCallHandlerUtilsRef,
        LinStorScope deviceMgrScopeRef,
        Provider<TransactionMgr> transMgrProviderRef,
        StltSecurityObjects stltSecObjRef,
        Provider<DeviceHandlerInvocationFactory> devHandlerInvocFactoryProviderRef
    )
    {
        wrkCtx = wrkCtxRef;
        errLog = errorReporterRef;
        nodesMap = nodesMapRef;
        rscDfnMap = rscDfnMapRef;
        reconfigurationLock = reconfigurationLockRef;
        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        stltUpdateRequester = stltUpdateRequesterRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        interComSerializer = interComSerializerRef;
        drbdEvent = drbdEventRef;
        drbdHnd = drbdDeviceHandlerRef;
        workQ = workQRef;
        apiCallHandlerUtils = apiCallHandlerUtilsRef;
        deviceMgrScope = deviceMgrScopeRef;
        transMgrProvider = transMgrProviderRef;
        stltSecObj = stltSecObjRef;
        devHandlerInvocFactoryProvider = devHandlerInvocFactoryProviderRef;

        updTracker = new StltUpdateTrackerImpl(sched);
        svcThr = null;
        devMgrInstName = DEV_MGR_NAME;

        drbdEvent.addDrbdStateChangeObserver(this);
        stateAvailable = drbdEvent.isDrbdStateAvailable();
        abortDevHndFlag = false;
    }

    /**
     * Dispatch resource to a specific handler depending on type
     */
    void dispatchResource(Resource rsc, SyncPoint phaseLockRef)
    {
        // Select the resource handler for the resource depeding on resource type
        // Currently, the DRBD resource handler is used for all resources

        DeviceHandlerInvocation devHndInv = devHandlerInvocFactoryProvider.get().create(
            this,
            drbdHnd,
            rsc,
            phaseLockRef
        );
        // DeviceHandlerInvocation devHndInv = new DeviceHandlerInvocation(this, drbdHnd, rsc, phaseLockRef);

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
            // If the device manager is currently dispatching device handlers,
            // abort early and stop the service as soon as possible
            abortDevHndFlag = true;
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
    public void controllerUpdateApplied()
    {
        synchronized (sched)
        {
            rcvPendingBundle.updControllerMap.clear();
            sched.notify();
        }
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

    @Override
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
    public void abortDeviceHandlers()
    {
        abortDevHndFlag = true;
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
                assert(!updNodeSet.isEmpty());
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
        errLog.logDebug("DeviceManager service started");
        try
        {
            devMgrLoop();
        }
        finally
        {
            runningFlag.set(false);
            errLog.logDebug("DeviceManager service stopped");
        }
    }

    private void devMgrLoop()
    {

        SyncPoint phaseLock = new AtomicSyncPoint();

        // Tracks objects that need to be dispatched to a device handler
        final Set<ResourceName> dispatchRscSet = new TreeSet<>();

        while (!shutdownFlag.get())
        {
            errLog.logDebug("Begin DeviceManager cycle %d", cycleNr);

            try
            {
                boolean fullSyncApplied = fullSyncFlag.getAndSet(false);
                if (fullSyncApplied)
                {
                    errLog.logTrace("DeviceManager: Executing device handlers after full sync");

                    // Clear the previous state
                    dispatchRscSet.clear();

                    Lock rcfgRdLock = reconfigurationLock.readLock();
                    Lock rscDfnMapRdLock = rscDfnMapLock.readLock();

                    // Schedule all known resources for dispatching to the device handlers
                    try
                    {
                        rcfgRdLock.lock();
                        rscDfnMapRdLock.lock();
                        dispatchRscSet.addAll(rscDfnMap.keySet());
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
                errLog.logDebug("End DeviceManager cycle %d", cycleNr);
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
            requestControllerUpdates(updPendingBundle.updControllerMap);
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
            reconfigurationLock.readLock().lock();

            SatelliteTransactionMgr transMgr = new SatelliteTransactionMgr();
            deviceMgrScope.enter();
            deviceMgrScope.seed(TransactionMgr.class, transMgr);

            try
            {
                // Check whether the master key for encrypted volumes is known
                boolean haveMasterKey = stltSecObj.getCryptKey() != null;

                abortDevHndFlag = false;
                NodeData localNode = controllerPeerConnector.getLocalNode();
                Iterator<ResourceName> rscNameIter = dispatchRscSet.iterator();
                while (rscNameIter.hasNext() && !abortDevHndFlag)
                {
                    ResourceName rscName = rscNameIter.next();
                    // Dispatch resources that were affected by changes to worker threads
                    // and to the resource's respective handler
                    ResourceDefinition rscDfn = rscDfnMap.get(rscName);
                    if (rscDfn != null)
                    {
                        Resource rsc = rscDfn.getResource(wrkCtx, localNode.getName());
                        if (rsc != null)
                        {
                            boolean dispatch = true;
                            // If the master key is not known, skip dispatching resources that
                            // have encrypted volumes
                            if (!haveMasterKey)
                            {
                                Iterator<VolumeDefinition> vlmDfnIter = rscDfn.iterateVolumeDfn(wrkCtx);
                                while (vlmDfnIter.hasNext())
                                {
                                    VolumeDefinition vlmDfn = vlmDfnIter.next();
                                    if (vlmDfn.getFlags().isSet(wrkCtx, VolumeDefinition.VlmDfnFlags.ENCRYPTED))
                                    {
                                        dispatch = false;
                                        break;
                                    }
                                }
                            }
                            if (dispatch)
                            {
                                dispatchResource(rsc, phaseLock);
                            }
                            else
                            {
                                errLog.logWarning(
                                    "Skipped actions for encrypted resource '%s' because the " +
                                    "encryption key is not known yet",
                                    rscDfn.getName().displayValue
                                );
                            }
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

                if (abortDevHndFlag)
                {
                    errLog.logTrace("Stopped dispatching resource handlers due to abort request");
                }

                errLog.logTrace("Waiting for queued resource handlers to finish");
                // Wait until the phase advances from the current phase number after all
                // device handlers have finished
                phaseLock.await();
                errLog.logTrace("All dispatched resource handlers finished");

                // Cleanup deleted objects
                deletedObjectsCleanup();

                transMgr.commit();
            }
            finally
            {
                transMgr.rollback();
                deviceMgrScope.exit();
                reconfigurationLock.readLock().unlock();
            }
        }
    }

    private void deletedObjectsCleanup()
        throws AccessDeniedException
    {
        final Set<NodeName> localDelNodeSet = new TreeSet<>();
        final Set<ResourceName> localDelRscSet;
        final Set<VolumeDefinition.Key> localDelVlmSet;

        // Shallow-copy the sets to avoid having to mix locking the sched lock and
        // the satellite's reconfigurationLock, rscDfnMapLock
        synchronized (sched)
        {
            localDelRscSet = new TreeSet<>(deletedRscSet);
            deletedRscSet.clear();
            localDelVlmSet = new TreeSet<>(deletedVlmSet);
            deletedVlmSet.clear();
        }

        Lock rcfgRdLock = reconfigurationLock.readLock();
        Lock nodeMapWrLock = nodesMapLock.writeLock();
        Lock rscDfnMapWrLock = rscDfnMapLock.writeLock();

        rcfgRdLock.lock();
        try
        {
            rscDfnMapWrLock.lock();
            try
            {
                // From the perspective of this satellite, once a volume is deleted the corresponding peer volumes are
                // irrelevant and we can delete our local copy of the entire volume definition.
                for (VolumeDefinition.Key volumeKey : localDelVlmSet)
                {
                    ResourceDefinition curRscDfn = rscDfnMap.get(volumeKey.rscName);
                    if (curRscDfn != null)
                    {
                        VolumeDefinition curVlmDfn = curRscDfn.getVolumeDfn(wrkCtx, volumeKey.vlmNr);
                        if (curVlmDfn != null &&
                            curVlmDfn.getFlags().isSet(wrkCtx, VolumeDefinition.VlmDfnFlags.DELETE))
                        {
                            curVlmDfn.delete(wrkCtx);
                        }
                    }
                }

                for (ResourceName curRscName : localDelRscSet)
                {
                    ResourceDefinition curRscDfn = rscDfnMap.get(curRscName);
                    if (curRscDfn != null)
                    {
                        // Delete the resource from all nodes
                        Map<NodeName, Resource> rscMap = new TreeMap<>();
                        curRscDfn.copyResourceMap(wrkCtx, rscMap);
                        for (Resource delRsc : rscMap.values())
                        {
                            Node peerNode = delRsc.getAssignedNode();
                            delRsc.delete(wrkCtx);
                            if (peerNode != controllerPeerConnector.getLocalNode())
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
                        transMgrProvider.get().commit();
                        // Since the local node no longer has the resource, it also does not need
                        // to know about the resource definition any longer, therefore
                        // delete the resource definition as well
                        rscDfnMap.remove(curRscName);
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
                    nodesMap.remove(curNodeName);
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

    private void requestControllerUpdates(Map<NodeName, UUID> updateControllerMap)
    {
        if (!updateControllerMap.isEmpty())
        {
            stltUpdateRequester.requestControllerUpdate();
        }
    }

    private void requestNodeUpdates(Map<NodeName, UUID> updateNodesMap)
    {
        for (Entry<NodeName, UUID> entry : updateNodesMap.entrySet())
        {
            errLog.logTrace("Requesting update for node '" + entry.getKey().displayValue + "'");
            stltUpdateRequester.requestNodeUpdate(
                entry.getValue(),
                entry.getKey()
            );
        }
    }

    private void requestRscDfnUpdates(Map<ResourceName, UUID> updateRscDfnMap)
    {
        for (Entry<ResourceName, UUID> entry : updateRscDfnMap.entrySet())
        {
            errLog.logTrace("Requesting update for resource definition '" + entry.getKey().displayValue + "'");
            stltUpdateRequester.requestRscDfnUpate(
                entry.getValue(),
                entry.getKey()
            );
        }
    }

    private void requestRscUpdates(Map<ResourceName, Map<NodeName, UUID>> updRscMap)
    {
        for (Entry<ResourceName, Map<NodeName, UUID>> entry : updRscMap.entrySet())
        {
            ResourceName rscName = entry.getKey();
            Map<NodeName, UUID> nodes = entry.getValue();
            for (Entry<NodeName, UUID> nodeEntry : nodes.entrySet())
            {
                errLog.logTrace("Requesting update for resource '" + entry.getKey().displayValue + "'");
                stltUpdateRequester.requestRscUpdate(
                    nodeEntry.getValue(),
                    nodeEntry.getKey(),
                    rscName
                );
            }
        }
    }

    private void requestStorPoolUpdates(Map<StorPoolName, UUID> updStorPoolMap)
    {
        for (Entry<StorPoolName, UUID> entry : updStorPoolMap.entrySet())
        {
            errLog.logTrace("Requesting update for storage pool '" + entry.getKey().displayValue + "'");
            stltUpdateRequester.requestStorPoolUpdate(
                entry.getValue(),
                entry.getKey()
            );
        }
    }

    @Override
    public ServiceName getServiceName()
    {
        return DEV_MGR_NAME;
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
    public void notifyResourceApplied(Resource rsc)
    {
        // Send applySuccess notification to the controller

        Peer ctrlPeer = controllerPeerConnector.getControllerPeer();
        if (ctrlPeer != null)
        {
            byte[] data = null;
            try
            {
                data = interComSerializer
                    .builder(InternalApiConsts.API_NOTIFY_RSC_APPLIED, 1)
                    .notifyResourceApplied(
                        rsc,
                        apiCallHandlerUtils.getFreeSpace()
                    )
                    .build();
            }
            catch (StorageException exc)
            {
                errLog.reportError(exc);
            }

            if (data != null)
            {
                ctrlPeer.sendMessage(data);
            }
        }
    }

    @Override
    public void notifyResourceDeleted(Resource rsc)
    {
        // Send delete notification to the controller
        Peer ctrlPeer = controllerPeerConnector.getControllerPeer();
        if (ctrlPeer != null)
        {
            String msgNodeName = rsc.getAssignedNode().getName().displayValue;
            String msgRscName = rsc.getDefinition().getName().displayValue;
            UUID rscUuid = rsc.getUuid();

            byte[] data = null;
            try
            {
                data = interComSerializer
                    .builder(InternalApiConsts.API_NOTIFY_RSC_DEL, 1)
                    .notifyResourceDeleted(
                        msgNodeName,
                        msgRscName,
                        rscUuid,
                        apiCallHandlerUtils.getFreeSpace()
                    )
                    .build();
            }
            catch (StorageException exc)
            {
                errLog.reportError(exc);
            }

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
        Peer ctrlPeer = controllerPeerConnector.getControllerPeer();
        if (ctrlPeer != null)
        {
            String msgNodeName = vlm.getResource().getAssignedNode().getName().displayValue;
            String msgRscName = vlm.getResource().getDefinition().getName().displayValue;

            ctrlPeer.sendMessage(interComSerializer
                .builder(InternalApiConsts.API_NOTIFY_VLM_DEL, 1)
                .notifyVolumeDeleted(
                    msgNodeName,
                    msgRscName,
                    vlm.getVolumeDefinition().getVolumeNumber().value,
                    vlm.getUuid()
                )
                .build()
            );
        }

        // Remember the volume for removal after the DeviceHandler instances have finished
        synchronized (sched)
        {
            deletedVlmSet.add(new VolumeDefinition.Key(vlm));
        }
    }

    static class DeviceHandlerInvocation implements Runnable
    {
        private final DeviceManagerImpl devMgr;
        private final DeviceHandler handler;
        private final Resource rsc;
        private final SyncPoint phaseLock;
        private LinStorScope devHndInvScope;
        private TransactionMgr transMgr;

        @AssistedInject
        DeviceHandlerInvocation(
            @Assisted DeviceManagerImpl devMgrRef,
            @Assisted DeviceHandler handlerRef,
            @Assisted Resource rscRef,
            @Assisted SyncPoint phaseLockRef,
            TransactionMgr transMgrRef, // should be the one from devMgr's scope
            LinStorScope devHndInvScopeRef
        )
        {
            devMgr = devMgrRef;
            handler = handlerRef;
            rsc = rscRef;
            phaseLock = phaseLockRef;
            transMgr = transMgrRef;
            devHndInvScope = devHndInvScopeRef;
            phaseLock.register();
        }

        @Override
        public void run()
        {
            try
            {
                if (!devMgr.abortDevHndFlag)
                {
                    devHndInvScope.enter();
                    devHndInvScope.seed(TransactionMgr.class, transMgr);

                    handler.dispatchResource(rsc);
                }
            }
            finally
            {
                devHndInvScope.exit();
                phaseLock.arrive();
            }
        }
    }

    class SvcCondException extends Exception
    {
    }

    interface DeviceHandlerInvocationFactory
    {
        DeviceHandlerInvocation create(
            DeviceManagerImpl devMgrRef,
            DeviceHandler handlerRef,
            Resource rscRef,
            SyncPoint phaseLockRef
        );
    }
}
