package com.linbit.linstor.core.devmgr;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.drbd.DrbdVersion;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.core.StltSecurityObjects;
import com.linbit.linstor.core.StltUpdateRequester;
import com.linbit.linstor.core.StltUpdateTracker;
import com.linbit.linstor.core.StltUpdateTrackerImpl;
import com.linbit.linstor.core.StltUpdateTrackerImpl.UpdateBundle;
import com.linbit.linstor.core.StltUpdateTrackerImpl.UpdateNotification;
import com.linbit.linstor.core.UpdateMonitor;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.satellite.StltApiCallHandlerUtils;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.drbdstate.DrbdEventService;
import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.common.ResourceStateEvent;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.layer.DeviceLayer;
import com.linbit.linstor.storage.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.transaction.SatelliteTransactionMgr;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionMgrUtil;
import com.linbit.locks.AtomicSyncPoint;
import com.linbit.locks.SyncPoint;
import com.linbit.utils.Either;

import static com.linbit.linstor.api.ApiConsts.MODIFIED;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.stream.Collectors;

import com.google.inject.Key;
import com.google.inject.name.Names;
import org.slf4j.event.Level;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Scheduler;

@Singleton
class DeviceManagerImpl implements Runnable, SystemService, DeviceManager, DeviceLayer.NotificationListener
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
    private final CoreModule.ResourceGroupMap rscGrpMap;
    private final CoreModule.ResourceDefinitionMap rscDfnMap;

    private final ReadWriteLock reconfigurationLock;
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock rscDfnMapLock;
    private final ReadWriteLock storPoolDfnMapLock;

    private final StltUpdateRequester stltUpdateRequester;
    private final ControllerPeerConnector controllerPeerConnector;
    private final CtrlStltSerializer interComSerializer;

    private StltUpdateTrackerImpl updTracker;

    // Tracks objects that require requesting updates from the controller
    private final UpdateBundle updPendingBundle = new StltUpdateTrackerImpl.UpdateBundle();

    // Tracks objects that are waiting to be updated with data received from the controller
    private final UpdateBundle rcvPendingBundle = new StltUpdateTrackerImpl.UpdateBundle();

    // Tracks nodes that need to be dispatched to a device handler and the sinks that should receive responses
    private final Map<NodeName, List<FluxSink<ApiCallRc>>> pendingDispatchNodes = new TreeMap<>();

    // Tracks resources that need to be dispatched to a device handler and the sinks that should receive responses
    private final Map<ResourceName, List<FluxSink<ApiCallRc>>> pendingDispatchRscs = new TreeMap<>();

    // Tracks sinks that need to be completed once the dispatch phase is complete
    private final List<FluxSink<ApiCallRc>> pendingResponseSinks = new ArrayList<>();

    private Thread svcThr;

    private final AtomicBoolean runningFlag     = new AtomicBoolean(false);
    private final AtomicBoolean svcCondFlag     = new AtomicBoolean(false);
    private final AtomicBoolean waitUpdFlag     = new AtomicBoolean(true);
    private final AtomicBoolean fullSyncFlag    = new AtomicBoolean(false);
    private final AtomicBoolean shutdownFlag    = new AtomicBoolean(false);
    private final AtomicBoolean collectUpdateNofiticationForceWakeFlag = new AtomicBoolean(false);

    private final Map<NodeName, ApiCallRc> dispatchNodeResponses = new TreeMap<>();
    private final Map<ResourceName, ApiCallRc> dispatchRscResponses = new TreeMap<>();

    private final Set<ResourceName> deletedRscSet = new TreeSet<>();
    private final Set<VolumeDefinition.Key> deletedVlmSet = new TreeSet<>();
    private final Set<VolumeDefinition.Key> drbdResizedVlmSet = new TreeSet<>();
    private final Set<SnapshotDefinition.Key> deletedSnapshotSet = new TreeSet<>();

    private final LinStorScope deviceMgrScope;
    private final Provider<TransactionMgr> transMgrProvider;

    private final StltSecurityObjects stltSecObj;

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

    private final DrbdVersion drbdVersion;
    private boolean stateAvailable;
    private volatile boolean abortDevHndFlag;
    private DrbdEventService drbdEvent;

    private long cycleNr = 0;

    private UpdateMonitor updateMonitor;

    private final StltApiCallHandlerUtils apiCallHandlerUtils;

    private final DeviceHandlerImpl devHandler;
    private ResourceStateEvent resourceStateEvent;


    @Inject
    DeviceManagerImpl(
        @DeviceManagerContext AccessContext wrkCtxRef,
        ErrorReporter errorReporterRef,
        CoreModule.NodesMap nodesMapRef,
        CoreModule.ResourceGroupMap rscGrpMapRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef,
        StltUpdateRequester stltUpdateRequesterRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        CtrlStltSerializer interComSerializerRef,
        DrbdEventService drbdEventRef,
        StltApiCallHandlerUtils apiCallHandlerUtilsRef,
        LinStorScope deviceMgrScopeRef,
        Provider<TransactionMgr> transMgrProviderRef,
        StltSecurityObjects stltSecObjRef,
        Scheduler scheduler,
        UpdateMonitor updateMonitorRef,
        ResourceStateEvent resourceStateEventRef,
        DeviceHandlerImpl deviceHandlerRef,
        DrbdVersion drbdVersionRef
    )
    {
        wrkCtx = wrkCtxRef;
        errLog = errorReporterRef;
        nodesMap = nodesMapRef;
        rscGrpMap = rscGrpMapRef;
        rscDfnMap = rscDfnMapRef;
        reconfigurationLock = reconfigurationLockRef;
        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        storPoolDfnMapLock = storPoolDfnMapLockRef;
        stltUpdateRequester = stltUpdateRequesterRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        interComSerializer = interComSerializerRef;
        drbdEvent = drbdEventRef;
        apiCallHandlerUtils = apiCallHandlerUtilsRef;
        deviceMgrScope = deviceMgrScopeRef;
        transMgrProvider = transMgrProviderRef;
        stltSecObj = stltSecObjRef;
        updateMonitor = updateMonitorRef;
        resourceStateEvent = resourceStateEventRef;
        drbdVersion = drbdVersionRef;

        updTracker = new StltUpdateTrackerImpl(sched, scheduler);
        svcThr = null;
        devMgrInstName = DEV_MGR_NAME;

        drbdEvent.addDrbdStateChangeObserver(this);
        stateAvailable = drbdEvent.isDrbdStateAvailable();
        abortDevHndFlag = false;

        devHandler = deviceHandlerRef;
    }

    /**
     * Dispatch resources and/or snapshots to a specific handler depending on type
     */
    private void dispatchResources(
        Set<Resource> resourcesToDispatch,
        Set<Snapshot> snapshotsToDispatch,
        SyncPoint phaseLock
    )
    {
         devHandler.dispatchResources(resourcesToDispatch, snapshotsToDispatch);
         // // DeviceHandlerInvocation devHndInv = new DeviceHandlerInvocation(this, drbdHnd, rsc, phaseLockRef);
         //
         // workQ.submit(devHndInv);
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
    public void controllerUpdateApplied(Set<ResourceName> rscSet)
    {
        synchronized (sched)
        {
            markPendingRscDispatch(rcvPendingBundle.controllerUpdate.orElse(null), rscSet);
            rcvPendingBundle.controllerUpdate = Optional.empty();
            sched.notify();
        }
    }

    @Override
    public void nodeUpdateApplied(Set<NodeName> nodeSet, Set<ResourceName> rscSet)
    {
        synchronized (sched)
        {
            for (NodeName nodeName : nodeSet)
            {
                UpdateNotification updateNot = rcvPendingBundle.nodeUpdates.remove(nodeName);

                markPendingRscDispatch(updateNot, rscSet);
                markPendingNodeDispatch(updateNot, nodeName);
            }
            if (rcvPendingBundle.isEmpty())
            {
                sched.notify();
            }
        }
    }

    @Override
    public void storPoolUpdateApplied(
        Set<StorPoolName> storPoolSet,
        Set<ResourceName> rscSet,
        ApiCallRc responses
    )
    {
        synchronized (sched)
        {
            for (StorPoolName storPoolName : storPoolSet)
            {
                UpdateNotification updateNotification = rcvPendingBundle.storPoolUpdates.remove(storPoolName);

                markPendingRscDispatch(updateNotification, rscSet);

                List<FluxSink<ApiCallRc>> responseSinks = updateNotification == null ?
                    Collections.emptyList() :
                    updateNotification.getResponseSinks();
                for (FluxSink<ApiCallRc> responseSink : responseSinks)
                {
                    responseSink.next(responses);
                }
            }
            if (rcvPendingBundle.isEmpty())
            {
                sched.notify();
            }
        }
    }

    @Override
    public void rscUpdateApplied(Set<Resource.ResourceKey> rscKeySet)
    {
        synchronized (sched)
        {
            for (Resource.ResourceKey resourceKey : rscKeySet)
            {
                markPendingRscDispatch(
                    rcvPendingBundle.rscUpdates.remove(resourceKey),
                    rscKeySet.stream().map(Resource.ResourceKey::getResourceName).collect(Collectors.toSet())
                );
            }
            if (rcvPendingBundle.isEmpty())
            {
                sched.notify();
            }
        }
    }

    @Override
    public void snapshotUpdateApplied(Set<SnapshotDefinition.Key> snapshotKeySet)
    {
        synchronized (sched)
        {
            for (SnapshotDefinition.Key snapshotKey : snapshotKeySet)
            {
                markPendingRscDispatch(
                    rcvPendingBundle.snapshotUpdates.remove(snapshotKey),
                    snapshotKeySet.stream().map(SnapshotDefinition.Key::getResourceName).collect(Collectors.toSet())
                );
            }
            if (rcvPendingBundle.isEmpty())
            {
                sched.notify();
            }
        }
    }

    private void markPendingNodeDispatch(
        UpdateNotification updateNotification,
        NodeName nodeName
    )
    {
        List<FluxSink<ApiCallRc>> responseSink = updateNotification == null ?
            Collections.emptyList() :
            updateNotification.getResponseSinks();

        List<FluxSink<ApiCallRc>> responseSinks =
            pendingDispatchNodes.computeIfAbsent(nodeName, ignored -> new ArrayList<>());

        responseSinks.addAll(responseSink);
        pendingResponseSinks.addAll(responseSink);
    }

    private void markPendingRscDispatch(
        UpdateNotification updateNotification,
        Set<ResourceName> rscSet
    )
    {
        List<FluxSink<ApiCallRc>> responseSink = updateNotification == null ?
            Collections.emptyList() :
            updateNotification.getResponseSinks();
        for (ResourceName rscName : rscSet)
        {
            List<FluxSink<ApiCallRc>> responseSinks =
                pendingDispatchRscs.computeIfAbsent(rscName, ignored -> new ArrayList<>());
            responseSinks.addAll(responseSink);
        }
        pendingResponseSinks.addAll(responseSink);
    }

    @Override
    public void markResourceForDispatch(ResourceName name)
    {
        synchronized (sched)
        {
            markPendingRscDispatch(null, Collections.singleton(name));
            sched.notify();
        }
    }

    @Override
    public void markMultipleResourcesForDispatch(Set<ResourceName> rscSet)
    {
        synchronized (sched)
        {
            markPendingRscDispatch(null, rscSet);
            sched.notify();
        }
    }

    @Override
    public void forceWakeUpdateNotifications()
    {
        collectUpdateNofiticationForceWakeFlag.set(true);
    }

    @Override
    public void fullSyncApplied(Node localNode) throws StorageException
    {
        synchronized (sched)
        {
            // Clear any previously valid state
            updPendingBundle.clear();
            rcvPendingBundle.clear();

            fullSyncFlag.set(true);
            svcCondFlag.set(true);
            sched.notify();
            try
            {
                devHandler.localNodePropsChanged(localNode.getProps(wrkCtx));
            }
            catch (AccessDeniedException exc)
            {
                throw new ImplementationError(exc);
            }
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

        while (!shutdownFlag.get())
        {
            errLog.logDebug("Begin DeviceManager cycle %d", cycleNr);

            try
            {
                boolean fullSyncApplied = fullSyncFlag.getAndSet(false);
                if (fullSyncApplied)
                {
                    errLog.logTrace("DeviceManager: Executing device handlers after full sync");

                    Map<ResourceName, List<FluxSink<ApiCallRc>>> dispatchRscs = new TreeMap<>();

                    Lock rcfgRdLock = reconfigurationLock.readLock();
                    Lock rscDfnMapRdLock = rscDfnMapLock.readLock();

                    // Schedule all known resources for dispatching to the device handlers
                    try
                    {
                        rcfgRdLock.lock();
                        rscDfnMapRdLock.lock();
                        for (ResourceName resourceName : rscDfnMap.keySet())
                        {
                            dispatchRscs.put(resourceName, new ArrayList<>());
                        }
                    }
                    finally
                    {
                        rscDfnMapRdLock.unlock();
                        rcfgRdLock.unlock();
                    }

                    synchronized (sched)
                    {
                        pendingDispatchRscs.clear();
                        pendingDispatchRscs.putAll(dispatchRscs);
                    }
                    devHandler.fullSyncApplied(controllerPeerConnector.getLocalNode());
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

                    updateMonitor.waitUntilCurrentFullSyncApplied(sched);
                    if (shutdownFlag.get())
                    {
                        throw new SvcCondException();
                    }

                    // Requests updates from the controller
                    phaseRequestUpdateData();

                    // Blocks until all updates have been received from the controller
                    phaseCollectUpdateData();
                }

                // Cancel nonblocking collection of update notifications
                waitUpdFlag.set(true);

                if (!drbdVersion.hasDrbd9() || stateAvailable)
                {
                    phaseDispatchDeviceHandlers(phaseLock);
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
                updTracker.collectUpdateNotifications(
                    updPendingBundle,
                    svcCondFlag,
                    collectUpdateNofiticationForceWakeFlag,
                    waitUpdFlag.get() && pendingDispatchRscs.isEmpty() && pendingDispatchNodes.isEmpty()
                );
                if (svcCondFlag.get())
                {
                    throw new SvcCondException();
                }
            }
        }
    }

    private void phaseRequestUpdateData()
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

            // Request updates from the controller
            requestControllerUpdates(updPendingBundle.controllerUpdate.isPresent());
            requestNodeUpdates(extractUuids(updPendingBundle.nodeUpdates));
            requestStorPoolUpdates(extractUuids(updPendingBundle.storPoolUpdates));
            requestRscUpdates(extractUuids(updPendingBundle.rscUpdates));
            requestSnapshotUpdates(extractUuids(updPendingBundle.snapshotUpdates));

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

    private void phaseDispatchDeviceHandlers(SyncPoint phaseLock)
        throws AccessDeniedException
    {
        errLog.logTrace("Dispatching nodes and resources to device handlers");

        Map<NodeName, List<FluxSink<ApiCallRc>>> dispatchNodes;
        Map<ResourceName, List<FluxSink<ApiCallRc>>> dispatchRscs;
        List<FluxSink<ApiCallRc>> responseSinks;
        synchronized (sched)
        {
            // Add any dispatch requests that were received
            // into the dispatch set and clear the dispatch requests
            dispatchNodes = new TreeMap<>(pendingDispatchNodes);
            pendingDispatchNodes.clear();
            dispatchRscs = new TreeMap<>(pendingDispatchRscs);
            pendingDispatchRscs.clear();
            responseSinks = new ArrayList<>(pendingResponseSinks);
            pendingResponseSinks.clear();
        }

        // BEGIN DEBUG
        // ((DrbdDeviceHandler) drbdHnd).debugListSatelliteObjects();
        // END DEBUG

        if (!dispatchNodes.isEmpty() || !dispatchRscs.isEmpty() || !responseSinks.isEmpty())
        {
            Lock reconfWrLock = reconfigurationLock.writeLock();
            Lock nodesWrLock = nodesMapLock.writeLock();
            Lock rscDfnWrLock = rscDfnMapLock.writeLock();
            Lock storPoolWrLock = storPoolDfnMapLock.writeLock();
            reconfWrLock.lock();
            nodesWrLock.lock();
            rscDfnWrLock.lock();
            storPoolWrLock.lock();

            SatelliteTransactionMgr transMgr = new SatelliteTransactionMgr();
            Node localNode = controllerPeerConnector.getLocalNode();

            deviceMgrScope.enter();
            TransactionMgrUtil.seedTransactionMgr(deviceMgrScope, transMgr);
            deviceMgrScope.seed(NotificationListener.class, this);
            deviceMgrScope.seed(
                Key.get(Props.class, Names.named(DevMgrModule.LOCAL_NODE_PROPS)),
                localNode.getProps(wrkCtx)
            );

            try
            {
                // Check whether the master key for encrypted volumes is known
                boolean haveMasterKey = stltSecObj.getCryptKey() != null;

                abortDevHndFlag = false;

                /* DISPATCH RESOURCES AND SNAPSHOTS */

                Set<Resource> resourcesToDispatch = new TreeSet<>();
                Set<Snapshot> snapshotsToDispatch = new TreeSet<>();
                Set<Resource> remoteResourcesToDelete = new TreeSet<>();

                for (ResourceName rscName : dispatchRscs.keySet())
                {

                    // Dispatch resources that were affected by changes to worker threads
                    // and to the resource's respective handler
                    ResourceDefinition rscDfn = rscDfnMap.get(rscName);
                    if (rscDfn != null)
                    {
                        Resource rsc = rscDfn.getResource(wrkCtx, localNode.getName());

                        Iterator<Resource> rscIt = rscDfn.iterateResource(wrkCtx);
                        while (rscIt.hasNext())
                        {
                            Resource otherRsc = rscIt.next();
                            if (otherRsc != rsc &&
                                otherRsc.getStateFlags().isSet(wrkCtx, Resource.Flags.DELETE)
                            )
                            {
                                remoteResourcesToDelete.add(otherRsc);
                            }
                        }

                        List<Snapshot> snapshots = new ArrayList<>();
                        for (SnapshotDefinition snapshotDfn : rscDfn.getSnapshotDfns(wrkCtx))
                        {
                            Snapshot snapshot = snapshotDfn.getSnapshot(wrkCtx, localNode.getName());
                            if (snapshot != null)
                            {
                                snapshots.add(snapshot);
                            }
                        }

                        boolean needMasterKey = false;
                        // If the master key is not known, skip dispatching resources that
                        // have encrypted volumes
                        if (!haveMasterKey)
                        {
                            Iterator<VolumeDefinition> vlmDfnIter = rscDfn.iterateVolumeDfn(wrkCtx);
                            while (vlmDfnIter.hasNext())
                            {
                                VolumeDefinition vlmDfn = vlmDfnIter.next();
                                if (vlmDfn.getFlags().isSet(wrkCtx, VolumeDefinition.Flags.ENCRYPTED))
                                {
                                    needMasterKey = true;
                                    break;
                                }
                            }
                        }
                        if (!needMasterKey)
                        {
                            if (rsc != null)
                            {
                                // rsc can be null if it was deleted but a snapshot still exists
                                // such that the resource name was looked up
                                resourcesToDispatch.add(rsc);
                            }
                            snapshotsToDispatch.addAll(snapshots);
                            // dispatchResource(rscDfn, snapshots, phaseLock);
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
                            "' which is unknown to this satellite"
                        );
                    }
                    if (abortDevHndFlag)
                    {
                        break;
                    }
                }
                dispatchResources(resourcesToDispatch, snapshotsToDispatch, phaseLock);

                if (abortDevHndFlag)
                {
                    errLog.logTrace("Stopped dispatching resource handlers due to abort request");
                }

                errLog.logTrace("Waiting for queued resource handlers to finish");
                // Wait until the phase advances from the current phase number after all
                // device handlers have finished
                phaseLock.await();
                errLog.logTrace("All dispatched resource handlers finished");

                /* DISPATCH NODES */

                Set<Node> nodesToDelete = new TreeSet<>();
                for (NodeName nodeName : dispatchNodes.keySet())
                {
                    synchronized (sched)
                    {
                        dispatchNodeResponses.put(
                            nodeName,
                            ApiCallRcImpl.singleApiCallRc(MODIFIED, "Node changes applied.")
                        );
                    }
                }

                if (abortDevHndFlag)
                {
                    errLog.logTrace("Stopped dispatching node handlers due to abort request");
                }

                errLog.logTrace("Waiting for queued node handlers to finish");
                // Wait until the phase advances from the current phase number after all
                // device handlers have finished
                phaseLock.await();
                errLog.logTrace("All dispatched node handlers finished");

                respondToController(dispatchNodes, dispatchRscs, responseSinks);

                // Cleanup deleted objects
                deletedObjectsCleanup(nodesToDelete, remoteResourcesToDelete);
            }
            finally
            {
                // always commit transaction as we most likely changed our environment which we cannot
                // rollback
                transMgr.commit();
                deviceMgrScope.exit();
                storPoolWrLock.unlock();
                rscDfnWrLock.unlock();
                nodesWrLock.unlock();
                reconfWrLock.unlock();
            }
        }
    }

    private void respondToController(
        Map<NodeName, List<FluxSink<ApiCallRc>>> dispatchNodes,
        Map<ResourceName, List<FluxSink<ApiCallRc>>> dispatchRscs,
        List<FluxSink<ApiCallRc>> responseSinks
    )
    {
        synchronized (sched)
        {
            for (Entry<NodeName, ApiCallRc> dispatchNodeResponseEntry : dispatchNodeResponses.entrySet())
            {
                NodeName nodeName = dispatchNodeResponseEntry.getKey();
                ApiCallRc response = dispatchNodeResponseEntry.getValue();
                List<FluxSink<ApiCallRc>> sinks = dispatchNodes.get(nodeName);

                if (sinks != null)
                {
                    for (FluxSink<ApiCallRc> sink : sinks)
                    {
                        sink.next(response);
                    }
                }
            }
            dispatchNodeResponses.clear();

            for (Entry<ResourceName, ApiCallRc> dispatchRscResponseEntry : dispatchRscResponses.entrySet())
            {
                ResourceName resourceName = dispatchRscResponseEntry.getKey();
                ApiCallRc response = dispatchRscResponseEntry.getValue();
                List<FluxSink<ApiCallRc>> sinks = dispatchRscs.get(resourceName);

                if (sinks != null)
                {
                    for (FluxSink<ApiCallRc> sink : sinks)
                    {
                        sink.next(response);
                    }
                }
            }
            dispatchRscResponses.clear();

            for (FluxSink<ApiCallRc> responseSink : responseSinks)
            {
                responseSink.complete();
            }
        }
    }

    private void deletedObjectsCleanup(Set<Node> nodesToDelete, Set<Resource> remoteResourcesToDelete)
        throws AccessDeniedException
    {
        final Set<NodeName> localDelNodeSet = new TreeSet<>();
        final Set<ResourceName> localDelRscSet;
        final Set<VolumeDefinition.Key> localDelVlmSet;
        final Set<VolumeDefinition.Key> localDrbdResizedVlmSet;
        final Set<SnapshotDefinition.Key> localDelSnapshotSet;

        // Shallow-copy the sets to avoid having to mix locking the sched lock and
        // the satellite's reconfigurationLock, rscDfnMapLock
        synchronized (sched)
        {
            localDelRscSet = new TreeSet<>(deletedRscSet);
            deletedRscSet.clear();
            localDelVlmSet = new TreeSet<>(deletedVlmSet);
            deletedVlmSet.clear();
            localDrbdResizedVlmSet = new TreeSet<>(drbdResizedVlmSet);
            drbdResizedVlmSet.clear();
            localDelSnapshotSet = new TreeSet<>(deletedSnapshotSet);
            deletedSnapshotSet.clear();
        }

        Lock rcfgWrLock = reconfigurationLock.writeLock();
        Lock nodeMapWrLock = nodesMapLock.writeLock();
        Lock rscDfnMapWrLock = rscDfnMapLock.writeLock();

        rcfgWrLock.lock();
        try
        {
            rscDfnMapWrLock.lock();
            try
            {
                for (SnapshotDefinition.Key snapshotKey : localDelSnapshotSet)
                {
                    ResourceDefinition resourceDefinition = rscDfnMap.get(snapshotKey.getResourceName());
                    if (resourceDefinition != null)
                    {
                        SnapshotDefinition snapshotDefinition =
                            resourceDefinition.getSnapshotDfn(wrkCtx, snapshotKey.getSnapshotName());

                        for (Snapshot snapshot : snapshotDefinition.getAllSnapshots(wrkCtx))
                        {
                            snapshot.delete(wrkCtx);
                        }

                        snapshotDefinition.delete(wrkCtx);
                    }
                }

                for (VolumeDefinition.Key volumeKey : localDrbdResizedVlmSet)
                {
                    ResourceDefinition curRscDfn = rscDfnMap.get(volumeKey.rscName);
                    if (curRscDfn != null)
                    {
                        Resource curRsc = curRscDfn.getResource(
                            wrkCtx, controllerPeerConnector.getLocalNode().getName());
                        if (curRsc != null)
                        {
                            Volume curVlm = curRsc.getVolume(volumeKey.vlmNr);
                            if (curVlm != null)
                            {
                                curVlm.getFlags().disableFlags(wrkCtx, Volume.Flags.DRBD_RESIZE);
                            }
                        }
                    }
                }

                for (ResourceName curRscName : localDelRscSet)
                {
                    resourceStateEvent.get().closeStream(
                        ObjectIdentifier.resource(controllerPeerConnector.getLocalNodeName(), curRscName)
                    );
                    ResourceDefinition curRscDfn = rscDfnMap.get(curRscName);
                    if (curRscDfn != null)
                    {
                        // Delete the resource from all nodes
                        Map<NodeName, Resource> rscMap = new TreeMap<>();
                        curRscDfn.copyResourceMap(wrkCtx, rscMap);
                        for (Resource delRsc : rscMap.values())
                        {
                            Node peerNode = delRsc.getNode();
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

                        ResourceGroup rscGrp = curRscDfn.getResourceGroup();
                        // Since the local node no longer has the resource, it also does not need
                        // to know about the resource definition any longer, therefore
                        // delete the resource definition as well
                        curRscDfn.delete(wrkCtx); // just to be sure
                        rscDfnMap.remove(curRscName);
                        if (!rscGrp.hasResourceDefinitions(wrkCtx))
                        {
                            ResourceGroupName rscGrpName = rscGrp.getName();
                            rscGrp.delete(wrkCtx);
                            rscGrpMap.remove(rscGrpName);
                        }
                    }
                }

                for (Resource remoteRsc : remoteResourcesToDelete)
                {

                    if (!remoteRsc.isDeleted())
                    {
                        // if the remote resource never gets deleted, it will cause a
                        // divergent UUID exception when the "same" remote resource gets
                        // recreated
                        Node remoteNode = remoteRsc.getNode();
                        remoteRsc.delete(wrkCtx);

                        /*
                         *  Bugfix: if the remoteRsc was the last resource of the remote node
                         *  we will no longer receive updates about the remote node (why should we?)
                         *  The problem is, that if the remote node gets completely deleted
                         *  on the controller, and later recreated, and that "new" node deploys
                         *  a resource we are also interested in, we will receive the "new" node's UUID.
                         *  However, we will still find our old node-reference when looking up the
                         *  "new" node's name and therefore we will find the old node's UUID and check it
                         *  against the "new" node's UUID.
                         *  This will cause a UUID mismatch upon resource-creation on the other node
                         *  (which will trigger an update to us as we also need to know about the new resource
                         *  and it's node)
                         *
                         *  Therefore, we have to remove the remoteNode completely if it has no
                         *  resources left
                         */
                        if (!remoteNode.iterateResources(wrkCtx).hasNext())
                        {
                            nodesMap.remove(remoteNode.getName());
                            remoteNode.delete(wrkCtx);
                        }
                    }
                    // else the remoteRsc and if needed also the remoteNode were already deleted
                    // when the resource-definition was cleaned up
                }

                // From the perspective of this satellite, once a volume is deleted the corresponding peer volumes are
                // irrelevant and we can delete our local copy of the entire volume definition.
                for (VolumeDefinition.Key volumeKey : localDelVlmSet)
                {
                    ResourceDefinition curRscDfn = rscDfnMap.get(volumeKey.rscName);
                    if (curRscDfn != null)
                    {
                        VolumeDefinition curVlmDfn = curRscDfn.getVolumeDfn(wrkCtx, volumeKey.vlmNr);
                        if (curVlmDfn != null)
                        {
                            curVlmDfn.delete(wrkCtx);
                        }
                    }
                }

                // delete nodes
                for (Node node : nodesToDelete)
                {
                    if (!node.isDeleted())
                    {
                        node.delete(wrkCtx);
                        nodesMap.remove(node.getName());
                    }
                }

                transMgrProvider.get().commit();
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
        catch (DatabaseException ignored)
        {
            // Satellite; does not throw DatabaseExceptions, because the database update methods
            // are no-ops -> ignored
        }
        finally
        {
            rcfgWrLock.unlock();
        }
    }

    private void requestControllerUpdates(boolean updateController)
    {
        if (updateController)
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

    private void requestRscUpdates(Map<Resource.ResourceKey, UUID> rscUpdates)
    {
        for (Entry<Resource.ResourceKey, UUID> entry : rscUpdates.entrySet())
        {
            errLog.logTrace("Requesting update for resource '" + entry.getKey().getResourceName().displayValue + "'");
            stltUpdateRequester.requestRscUpdate(
                entry.getValue(),
                entry.getKey().getNodeName(),
                entry.getKey().getResourceName()
            );
        }
    }

    private void requestStorPoolUpdates(Map<StorPoolName, UUID> storPoolUpdates)
    {
        for (Entry<StorPoolName, UUID> entry : storPoolUpdates.entrySet())
        {
            errLog.logTrace("Requesting update for storage pool '" + entry.getKey().displayValue + "'");
            stltUpdateRequester.requestStorPoolUpdate(
                entry.getValue(),
                entry.getKey()
            );
        }
    }

    private void requestSnapshotUpdates(Map<SnapshotDefinition.Key, UUID> snapshotUpdates)
    {
        for (Entry<SnapshotDefinition.Key, UUID> entry : snapshotUpdates.entrySet())
        {
            errLog.logTrace("Requesting update for snapshot '" + entry.getKey().getSnapshotName().displayValue + "'" +
                " of resource '" + entry.getKey().getResourceName().displayValue + "'");
            stltUpdateRequester.requestSnapshotUpdate(
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
    public void notifyResourceDispatchResponse(ResourceName resourceName, ApiCallRc response)
    {
        // Remember the response and to send combined responses after DeviceHandler instances have finished
        synchronized (sched)
        {
            dispatchRscResponses.put(resourceName, response);
        }
    }

    @Override
    public void notifyResourceApplied(Resource rsc)
    {
        // Send applySuccess notification to the controller

        Peer ctrlPeer = controllerPeerConnector.getControllerPeer();
        if (ctrlPeer != null)
        {
            Map<StorPool, Either<SpaceInfo, ApiRcException>> spaceInfoQueryMap =
                apiCallHandlerUtils.getAllSpaceInfo(false);

            Map<StorPool, SpaceInfo> spaceInfoMap = new TreeMap<>();

            spaceInfoQueryMap.forEach((storPool, either) -> either.consume(
                spaceInfo -> spaceInfoMap.put(storPool, spaceInfo),
                apiRcException -> errLog.reportError(apiRcException.getCause())
            ));

            ctrlPeer.sendMessage(
                interComSerializer
                    .onewayBuilder(InternalApiConsts.API_NOTIFY_RSC_APPLIED)
                    .notifyResourceApplied(rsc, spaceInfoMap)
                    .build()
            );
        }
    }

    @Override
    public void notifyDrbdVolumeResized(Volume vlm)
    {
        // Remember the resize to clear the flag after DeviceHandler instances have finished
        synchronized (sched)
        {
            drbdResizedVlmSet.add(new VolumeDefinition.Key(vlm));
        }
    }

    @Override
    public void notifyResourceDeleted(Resource rsc)
    {
        // Remember the resource for removal after the DeviceHandler instances have finished
        synchronized (sched)
        {
            deletedRscSet.add(rsc.getDefinition().getName());
        }
    }

    @Override
    public void notifyVolumeDeleted(Volume vlm)
    {
        // Remember the volume for removal after the DeviceHandler instances have finished
        synchronized (sched)
        {
            deletedVlmSet.add(new VolumeDefinition.Key(vlm.getKey()));
        }
    }

    @Override
    public void notifySnapshotDeleted(Snapshot snapshot)
    {
        // Remember the snapshot for removal after the DeviceHandler instances have finished
        synchronized (sched)
        {
            deletedSnapshotSet.add(new SnapshotDefinition.Key(snapshot.getSnapshotDefinition()));
        }
    }

    @Override
    public void notifyFreeSpacesChanged(Map<StorPool, SpaceInfo> spaceInfoMap)
    {
        // Send update notification to the controller

        Peer ctrlPeer = controllerPeerConnector.getControllerPeer();
        if (ctrlPeer != null)
        {
            ctrlPeer.sendMessage(
                interComSerializer
                    .onewayBuilder(InternalApiConsts.API_UPDATE_FREE_CAPACITY)
                    .updateFreeCapacities(spaceInfoMap)
                    .build()
            );
        }
    }

    @Override
    public void notifyResourceFailed(Resource rsc, ApiCallRc apiCallRc)
    {
        Peer ctrlPeer = controllerPeerConnector.getControllerPeer();
        if (ctrlPeer != null)
        {
            ctrlPeer.sendMessage(
                interComSerializer
                    .onewayBuilder(InternalApiConsts.API_NOTIFY_RSC_FAILED)
                    .notifyResourceFailed(rsc, apiCallRc)
                    .build()
            );
        }
    }

    static <K> Map<K, UUID> extractUuids(Map<K, UpdateNotification> map)
    {
        return map.entrySet().stream()
            .collect(Collectors.toMap(
                Entry::getKey,
                entry -> entry.getValue().getUuid()
            ));
    }

    class SvcCondException extends Exception
    {
    }
}
