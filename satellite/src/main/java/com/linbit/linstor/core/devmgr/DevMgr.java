package com.linbit.linstor.core.devmgr;

//import com.linbit.ImplementationError;
//import com.linbit.InvalidNameException;
//import com.linbit.ServiceName;
//import com.linbit.SystemService;
//import com.linbit.SystemServiceStartException;
//import com.linbit.extproc.ExtCmdFactory;
//import com.linbit.linstor.InternalApiConsts;
//import com.linbit.linstor.NodeName;
//import com.linbit.linstor.Resource;
//import com.linbit.linstor.Resource.RscFlags;
//import com.linbit.linstor.annotation.SystemContext;
//import com.linbit.linstor.api.ApiCallRc;
//import com.linbit.linstor.api.LinStorScope;
//import com.linbit.linstor.api.SpaceInfo;
//import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
//import com.linbit.linstor.ResourceDataFactory;
//import com.linbit.linstor.ResourceDefinition;
//import com.linbit.linstor.ResourceName;
//import com.linbit.linstor.ResourceType;
//import com.linbit.linstor.Snapshot;
//import com.linbit.linstor.SnapshotDefinition;
//import com.linbit.linstor.StorPool;
//import com.linbit.linstor.StorPoolName;
//import com.linbit.linstor.Volume;
//import com.linbit.linstor.VolumeDataFactory;
//import com.linbit.linstor.VolumeDefinition;
//import com.linbit.linstor.core.ControllerPeerConnector;
//import com.linbit.linstor.core.CoreModule;
//import com.linbit.linstor.core.DeviceManager;
//import com.linbit.linstor.core.StltConfigAccessor;
//import com.linbit.linstor.core.StltUpdateTracker;
//import com.linbit.linstor.core.StltUpdateTrackerImpl;
//import com.linbit.linstor.core.StltUpdateTrackerImpl.UpdateBundle;
//import com.linbit.linstor.core.StltUpdateTrackerImpl.UpdateNotification;
//import com.linbit.linstor.core.UpdateMonitor;
//import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
//import com.linbit.linstor.core.apicallhandler.satellite.StltApiCallHandlerUtils;
//import com.linbit.linstor.core.devmgr.helper.LayeredResourcesHelper;
//import com.linbit.linstor.logging.ErrorReporter;
//import com.linbit.linstor.netcom.Peer;
//import com.linbit.linstor.security.AccessContext;
//import com.linbit.linstor.security.AccessDeniedException;
//import com.linbit.linstor.security.Privilege;
//import com.linbit.linstor.storage.LayerDataFactory;
//import com.linbit.linstor.storage.LayerFactory;
//import com.linbit.linstor.storage.StorageException;
//import com.linbit.linstor.storage.layer.DeviceLayer;
//import com.linbit.linstor.storage2.layer.kinds.DeviceLayerKind;
//import com.linbit.linstor.transaction.SatelliteTransactionMgr;
//import com.linbit.linstor.transaction.TransactionMgr;
//import com.linbit.utils.Either;
//import com.linbit.utils.Pair;
//import com.linbit.utils.RemoveAfterDevMgrRework;
//
//import javax.inject.Inject;
//import javax.inject.Named;
//import javax.inject.Provider;
//import javax.inject.Singleton;
//
//import java.sql.SQLException;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Optional;
//import java.util.Set;
//import java.util.TreeMap;
//import java.util.TreeSet;
//import java.util.Map.Entry;
//import java.util.concurrent.atomic.AtomicBoolean;
//import java.util.concurrent.locks.Lock;
//import java.util.concurrent.locks.ReadWriteLock;
//import java.util.function.Supplier;
//import java.util.stream.Collectors;
//
//import reactor.core.publisher.FluxSink;
//import reactor.core.scheduler.Scheduler;
//
//@Singleton
//public class DevMgr implements DeviceManager, SystemService
//{
//    // CAUTION! Avoid locking the sched lock and satellite locks like the reconfigurationLock, rscDfnMapLock, etc.
//    //          at the same time (nesting locks).
//    //          The APIs that update resource definition, volume definition, resource, volume, etc. data with
//    //          new information from the controller hold reconfigurationLock and other locks while applying
//    //          updates, and then inform this device manager instance about the updates by taking the sched
//    //          lock and calling notify(). It is quite likely that they still hold other locks when doing so,
//    //          therefore no other locks should be taken while the sched lock is held, so as to avoid deadlock.
//    private final Object sched = new Object();
//
//    private final ReadWriteLock reconfigurationLock;
//    private final ReadWriteLock nodesMapLock;
//    private final ReadWriteLock rscDfnMapLock;
//
//    private final CoreModule.NodesMap nodesMap;
//    private final CoreModule.ResourceDefinitionMap rscDfnMap;
//
//    private final ControllerPeerConnector controllerPeerConnector;
//
//    /**
//     * True as long as the deviceManager should continue its work.
//     * This can be stopped either by a system-shutdown or via the debug-console (service shutdown).
//     */
//    private final AtomicBoolean serviceRunning = new AtomicBoolean(false);
//
//    /**
//     * True as long as the deviceManager should continue the current main-loop.
//     */
//    private final AtomicBoolean loopRunning = new AtomicBoolean(false);
//
//    private List<Resource> resourcesToProcess = new ArrayList<>();
//    @RemoveAfterDevMgrRework // if all goes well, data should already arrive in new format from controller
//    private final LayeredResourcesHelper layeredRscHelper;
//
//    private final AccessContext sysCtx;
//    private final Provider<TransactionMgr> transMgrProvider;
//
//    private final TraverseOrder strategyTopDown;
//    private final TraverseOrder strategyBottomUp;
//
//    private final Map<DeviceLayerKind, DeviceLayer> deviceLayerLUT;
//
//    private final ExtCmdFactory extCmdFactory;
//    private final StltConfigAccessor stltCfgAccessor;
//    private final ErrorReporter errorReporter;
//    private final LinStorScope linstorScope;
//    private final LayerFactory layerFactory;
//
//    // private final UpdateTracker updateTracker;
//    private final StltUpdateTrackerImpl updateTracker;
//
//    private Thread devMgrThread;
//
//    private UpdateMonitor updateMonitor;
//
//    private int cycleCount = 0;
//
//
//
//    @Inject
//    public DevMgr(
//        @SystemContext AccessContext sysCtxRef,
//        ResourceDataFactory rscFactoryRef,
//        VolumeDataFactory vlmFactoryRef,
//        LayerDataFactory layerDataFactoryRef,
//        ExtCmdFactory extCmdFactoryRef,
//        StltConfigAccessor stltCfgAccessorRef,
//        ErrorReporter errorReporterRef,
//        Provider<TransactionMgr> transMgrProviderRef,
//        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
//        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
//        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
//        CoreModule.NodesMap nodesMapRef,
//        CoreModule.ResourceDefinitionMap rscDfnMapRef,
//        LinStorScope linstorScopeRef,
//        LayerFactory layerFactoryRef,
//        /*
//         * parameters needed for compatibility with old DeviceManager interface
//         */
//        Scheduler reactorScheduler,
//        ControllerPeerConnector controllerPeerConnectorRef,
//        CtrlStltSerializer interComSerializerRef,
//        StltApiCallHandlerUtils apiCallHandlerUtilsRef,
//        UpdateMonitor updateMonitorRef
//    )
//    {
//        sysCtx = sysCtxRef;
//        errorReporter = errorReporterRef;
//        transMgrProvider = transMgrProviderRef;
//        reconfigurationLock = reconfigurationLockRef;
//        nodesMapLock = nodesMapLockRef;
//        rscDfnMapLock = rscDfnMapLockRef;
//        nodesMap = nodesMapRef;
//        rscDfnMap = rscDfnMapRef;
//        linstorScope = linstorScopeRef;
//        layerFactory = layerFactoryRef;
//        layeredRscHelper = new LayeredResourcesHelper(
//            sysCtxRef,
//            rscFactoryRef,
//            vlmFactoryRef,
//            layerDataFactoryRef,
//            controllerPeerConnectorRef,
//            rscDfnMapRef,
//            errorReporterRef
//        );
//
//        strategyTopDown = new TopDownOrder(sysCtxRef);
//        strategyBottomUp = new BottomUpOrder(sysCtxRef);
//
//        deviceLayerLUT = new HashMap<>();
//
//        extCmdFactory = extCmdFactoryRef;
//        stltCfgAccessor = stltCfgAccessorRef;
//
//        // updateTracker = new UpdateTrackerImpl();
//        updateTracker = new StltUpdateTrackerImpl(sched, reactorScheduler);
//
//        // compatibility
//        devMgrInstName = DEV_MGR_NAME;
//        controllerPeerConnector = controllerPeerConnectorRef;
//        interComSerializer = interComSerializerRef;
//        apiCallHandlerUtils = apiCallHandlerUtilsRef;
//        updateMonitor = updateMonitorRef;
//    }
//
//    public void markResourceForUpdate(Resource rsc)
//    {
//        synchronized (sched)
//        {
//            resourcesToProcess.add(rsc);
//        }
//    }
//
//    private void mainLoop()
//    {
//System.out.println("main loop start");
//        while (keepServiceRunning())
//        {
//            errorReporter.logDebug("DevMgr: cycle %d start", cycleCount);
//
//
//debug();
//
//
//            try
//            {
//                waitUntil(() -> !keepLoopRunning());
//                updateMonitor.waitUntilCurrentFullSyncApplied(sched);
//System.out.println("wait until no work");
//                waitUntil(this::inIdlePhase);
//                updateTracker.collectUpdateNotifications(
//                    updPendingBundle,
//                    serviceRunning,
//                    loopRunning.get() && pendingDispatchRscs.isEmpty()
//                );
//                updPendingBundle.copyUpdateRequestsTo(rcvPendingBundle);
//System.out.println("wait until requests answered");
//                waitUntil(this::inWaitForRequestedDataPhase);
//System.out.println("processing resources");
//                processResources();
//            }
//            catch (InterruptedException ignored)
//            {
//            }
//            catch (StorageException exc)
//            {
//                errorReporter.reportError(exc);
//            }
//            catch (AccessDeniedException accDeniedExc)
//            {
//                throw new ImplementationError("DeviceManager has not enough privileges", accDeniedExc);
//            }
//            errorReporter.logDebug("DevMgr cycle %d finished", cycleCount);
//            cycleCount++;
//        }
//System.out.println("main loop end");
//    }
//
//    private void debug()
//    {
//        System.out.println("updPendingBundle: ");
//        System.out.println(" ctrl:  " + updPendingBundle.controllerUpdate.isPresent());
//        System.out.println(" nodes: " + updPendingBundle.nodeUpdates.keySet());
//        System.out.println(" rsc:   " + updPendingBundle.rscUpdates.keySet());
//        System.out.println(" snap:  " + updPendingBundle.snapshotUpdates.keySet());
//        System.out.println(" sp:    " + updPendingBundle.storPoolUpdates.keySet());
//        System.out.println("rcvPendingBundle: ");
//        System.out.println(" ctrl:  " + rcvPendingBundle.controllerUpdate.isPresent());
//        System.out.println(" nodes: " + rcvPendingBundle.nodeUpdates.keySet());
//        System.out.println(" rsc:   " + rcvPendingBundle.rscUpdates.keySet());
//        System.out.println(" snap:  " + rcvPendingBundle.snapshotUpdates.keySet());
//        System.out.println(" sp:    " + rcvPendingBundle.storPoolUpdates.keySet());
//        System.out.println("pendingDispatchRscs: " + pendingDispatchRscs.keySet());
//        System.out.println("updateTracker isEmpty: " + updateTracker.isEmpty());
//    }
//
//    /**
//     * Returns true as long as until at least one update can be requested from the controller
//     */
//    private boolean inIdlePhase()
//    {
//        return updPendingBundle.isEmpty() && pendingDispatchRscs.isEmpty() && updateTracker.isEmpty();
//    }
//
//    /**
//     * Returns true as long as the last pending update is applied
//     */
//    private boolean inWaitForRequestedDataPhase()
//    {
//        debug();
//        return !updateMonitor.isCurrentFullSyncApplied() || !rcvPendingBundle.isEmpty();
//    }
//
//    void processResources() throws StorageException, AccessDeniedException
//    {
//        List<Resource> allResources;
//        synchronized (sched)
//        {
//            convertResources();
//            allResources = new ArrayList<>(resourcesToProcess);
//            resourcesToProcess.clear();
//        }
//        /*
//        *  first we need to split the resources into bottom-up and top-down resources (regarding
//        *  device-layer traversing order). This depends on whether we need to create / adjust or delete
//        *  a volume
//        */
//
//        Map<TraverseOrder, List<Resource>> groupedResources = allResources.parallelStream()
//            .collect(Collectors.groupingBy(this::prepareRsc));
//
//        Lock reconfigWriteLock = reconfigurationLock.writeLock();
//        TransactionMgr transMgr = new SatelliteTransactionMgr();
//        linstorScope.enter();
//        linstorScope.seed(TransactionMgr.class, transMgr);
//        try
//        {
//            reconfigWriteLock.lock();
//
//            for (Entry<TraverseOrder, List<Resource>> entry : groupedResources.entrySet())
//            {
//                TraverseOrder traverseOrder = entry.getKey();
//                List<Resource> rscList = entry.getValue();
//
//                // System.out.println(traverseOrder.getClass().getSimpleName());
//
//                while (!rscList.isEmpty())
//                {
//                    List<Pair<DeviceLayerKind, List<Resource>>> batches = traverseOrder.getAllBatches(rscList);
//                    Collections.sort(
//                        batches,
//                        (batch1, batch2) ->
//                            Long.compare(
//                                // comparing 2 with 1 -> [0] should be the largest
//                                traverseOrder.getProcessableCount(batch2.objB, rscList),
//                                traverseOrder.getProcessableCount(batch1.objB, rscList)
//                            )
//                    );
//                    Pair<DeviceLayerKind, List<Resource>> nextBatch = batches.get(0);
//                    System.out.println(
//                        "awesome processing in action for " + nextBatch.objA.getClass().getSimpleName() +
//                        " resources: "
//                    );
//                    nextBatch.objB.forEach(rsc -> System.out.println("\t" + rsc));
//
//                    getDeviceLayer(nextBatch.objA).adjust(
//                        nextBatch.objB,
//                        Collections.emptyList(), // FIXME
//                        traverseOrder.getPhase()
//                    );
//
//                    // TODO: we need to prevent deploying resources which dependency resource failed
//                    rscList.removeAll(nextBatch.objB);
//
//                    // TODO: call flux-processing
//                    nextBatch.objB.stream().forEach(rsc -> pendingDispatchRscs.remove(rsc.getDefinition().getName()));
//                }
//            }
//        }
//        finally
//        {
//            try
//            {
//                transMgr.commit();
//            }
//            catch (SQLException exc)
//            {
//                throw new ImplementationError(exc);
//            }
//            reconfigWriteLock.unlock();
//            linstorScope.exit();
//        }
//    }
//
//    private DeviceLayer getDeviceLayer(DeviceLayerKind kind) throws StorageException
//    {
//        return layerFactory.getDeviceLayer(kind.getClass());
//    }
//
//    /**
//     * This method splits one {@link Resource} into device-layer-specific resources.
//     * In future versions of LINSTOR this method should get obsolete as the API layer should
//     * already receive the correct resources.
//     * @throws AccessDeniedException
//     */
//    @RemoveAfterDevMgrRework
//    private void convertResources() throws AccessDeniedException
//    {
//        // convert resourceNames to resources
//        Lock rscDfnReadLock = rscDfnMapLock.readLock();
//        try
//        {
//            rscDfnReadLock.lock();
//            for (ResourceName rscName : pendingDispatchRscs.keySet())
//            {
//                ResourceDefinition rscDfn = rscDfnMap.get(rscName);
//
//                resourcesToProcess.add(
//                    rscDfn.getResource(
//                        sysCtx,
//                        controllerPeerConnector.getLocalNodeName(),
//                        ResourceType.DEFAULT
//                    )
//                );
//            }
//        }
//        finally
//        {
//            rscDfnReadLock.unlock();
//        }
//
//        List<Resource> origResources = new ArrayList<>(resourcesToProcess);
//        resourcesToProcess.clear();
//        resourcesToProcess.addAll(layeredRscHelper.extractLayers(origResources));
//    }
//
//    /**
//     * <p>Prepares a {@link Volume} for the deviceLayers. This is done by calling each layer's
//     * prepare method. The layers are iterated in a top-down fashion.</p>
//     *
//     * <p>Layers are not expected to perform any external calls or throw any exceptions. This
//     * preparation step is only for calculating layer-stack related data, like gross size for each
//     * volume.</p>
//     *
//     * @param rsc
//     * @return the grouping key (currently one of {@link TraverseOrder#TOP_DOWN} or {@link TraverseOrder#BOTTOM_UP}).
//     */
//    private TraverseOrder prepareRsc(Resource rsc)
//    {
//        TraverseOrder ret;
//        try
//        {
//            // TODO calculate and update grossSize
//            if (rsc.getStateFlags().isSet(sysCtx, RscFlags.DELETE))
//            {
//                ret = strategyTopDown;
//            }
//            else
//            {
//                ret = strategyBottomUp;
//            }
//        }
//        catch (AccessDeniedException accDeniedExc)
//        {
//            throw new ImplementationError(accDeniedExc);
//        }
//        return ret;
//    }
//
//    private void waitUntil(Supplier<Boolean> waitCondition) throws InterruptedException
//    {
//        synchronized (sched)
//        {
//            while (waitCondition.get())
//            {
//                if (!keepLoopRunning())
//                {
//                    throw new InterruptedException();
//                }
//                sched.wait();
//            }
//        }
//    }
//
//    private boolean keepServiceRunning()
//    {
//        return serviceRunning.get();
//    }
//
//    private boolean keepLoopRunning()
//    {
//        return loopRunning.get() && serviceRunning.get();
//    }
//
//
//
//
//    /*
//     * Compatibility with old deviceManager interface
//     * Additionally the SystemService should be implemented by a separate DevMgrService class just to keep things clean
//     */
//
//    private static final ServiceName DEV_MGR_NAME;
//    static
//    {
//        try
//        {
//            DEV_MGR_NAME = new ServiceName("DeviceManager");
//        }
//        catch (InvalidNameException invName)
//        {
//            throw new ImplementationError(
//                "The built-in name of the DeviceManager service is invalid",
//                invName
//            );
//        }
//    }
//
//    public static final String SVC_INFO = "Manages storage, transport and replication resources";
//
//    private ServiceName devMgrInstName;
//
//    private final StltApiCallHandlerUtils apiCallHandlerUtils;
//    private final CtrlStltSerializer interComSerializer;
//
//    private final Set<ResourceName> deletedRscSet = new TreeSet<>();
//    private final Set<VolumeDefinition.Key> deletedVlmSet = new TreeSet<>();
//    private final Set<VolumeDefinition.Key> drbdResizedVlmSet = new TreeSet<>();
//    private final Set<SnapshotDefinition.Key> deletedSnapshotSet = new TreeSet<>();
//
//    private final Map<ResourceName, ApiCallRc> dispatchResponses = new TreeMap<>();
//
//    // Tracks objects that require requesting updates from the controller
//    private final UpdateBundle updPendingBundle = new StltUpdateTrackerImpl.UpdateBundle();
//
//    // Tracks objects that are waiting to be updated with data received from the controller
//    private final UpdateBundle rcvPendingBundle = new StltUpdateTrackerImpl.UpdateBundle();
//
//    // Tracks resources that need to be dispatched to a device handler and the sinks that should receive responses
//    private final Map<ResourceName, List<FluxSink<ApiCallRc>>> pendingDispatchRscs = new TreeMap<>();
//
//    // Tracks sinks that need to be completed once the dispatch phase is complete
//    private final List<FluxSink<ApiCallRc>> pendingResponseSinks = new ArrayList<>();
//
//    @Override
//    public void setServiceInstanceName(ServiceName instanceName)
//    {
//        if (instanceName != null)
//        {
//            devMgrInstName = instanceName;
//        }
//    }
//
//    @Override
//    public void start() throws SystemServiceStartException
//    {
//System.out.println("starting devMgr service");
//        if (serviceRunning.compareAndSet(false, true))
//        {
//System.out.println("starting devMgr service thread");
//            loopRunning.set(true);
//            devMgrThread = new Thread(this::mainLoop);
//            devMgrThread.setName(devMgrInstName.displayValue);
//            devMgrThread.start();
//        }
//        // BEGIN DEBUG - turn on trace logging
//        try
//        {
//            AccessContext privCtx = sysCtx.clone();
//            privCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);
//            errorReporter.setTraceEnabled(privCtx, true);
//        }
//        catch (AccessDeniedException accExc)
//        {
//            errorReporter.logWarning(
//                "Enabling TRACE logging failed (not authorized) -- worker context not authorized"
//            );
//        }
//        // END DEBUG
//    }
//
//    @Override
//    public void shutdown()
//    {
//        synchronized (sched)
//        {
//            // If the device manager is currently dispatching device handlers,
//            // abort early and stop the service as soon as possible
//            loopRunning.set(false);
//            serviceRunning.set(false);
//            sched.notify();
//        }
//    }
//
//    @Override
//    public void awaitShutdown(long timeout) throws InterruptedException
//    {
//        // Since devMgrThread may be set to null at any time when a currently running,
//        // thread exits, copy the reference to avoid the race condition
//        Thread waitThr = devMgrThread;
//        if (waitThr != null)
//        {
//            waitThr.join(timeout);
//        }
//    }
//
//    @Override
//    public void controllerUpdateApplied(Set<ResourceName> rscSet)
//    {
//        synchronized (sched)
//        {
//            markPendingDispatch(rcvPendingBundle.controllerUpdate.orElse(null), rscSet);
//            rcvPendingBundle.controllerUpdate = Optional.empty();
//            sched.notify();
//        }
//    }
//
//    @Override
//    public void nodeUpdateApplied(Set<NodeName> nodeSet, Set<ResourceName> rscSet)
//    {
//        synchronized (sched)
//        {
//            for (NodeName nodeName : nodeSet)
//            {
//                markPendingDispatch(rcvPendingBundle.nodeUpdates.remove(nodeName), rscSet);
//            }
//            if (rcvPendingBundle.isEmpty())
//            {
//                sched.notify();
//            }
//        }
//    }
//
//    @Override
//    public void storPoolUpdateApplied(
//        Set<StorPoolName> storPoolSet,
//        Set<ResourceName> rscSet,
//        ApiCallRc responses
//    )
//    {
//        synchronized (sched)
//        {
//            for (StorPoolName storPoolName : storPoolSet)
//            {
//                UpdateNotification updateNotification = rcvPendingBundle.storPoolUpdates.remove(storPoolName);
//
//                markPendingDispatch(updateNotification, rscSet);
//
//                List<FluxSink<ApiCallRc>> responseSinks = updateNotification == null ?
//                    Collections.emptyList() :
//                    updateNotification.getResponseSinks();
//                for (FluxSink<ApiCallRc> responseSink : responseSinks)
//                {
//                    responseSink.next(responses);
//                }
//            }
//            if (rcvPendingBundle.isEmpty())
//            {
//                sched.notify();
//            }
//        }
//    }
//
//    @Override
//    public void rscUpdateApplied(Set<Resource.Key> rscKeySet)
//    {
//        synchronized (sched)
//        {
//            for (Resource.Key resourceKey : rscKeySet)
//            {
//                markPendingDispatch(
//                    rcvPendingBundle.rscUpdates.remove(resourceKey),
//                    rscKeySet.stream().map(Resource.Key::getResourceName).collect(Collectors.toSet())
//                );
//            }
//            if (rcvPendingBundle.isEmpty())
//            {
//                sched.notify();
//            }
//        }
//    }
//
//    @Override
//    public void snapshotUpdateApplied(Set<SnapshotDefinition.Key> snapshotKeySet)
//    {
//        synchronized (sched)
//        {
//            for (SnapshotDefinition.Key snapshotKey : snapshotKeySet)
//            {
//                markPendingDispatch(
//                    rcvPendingBundle.snapshotUpdates.remove(snapshotKey),
//                    snapshotKeySet.stream().map(SnapshotDefinition.Key::getResourceName).collect(Collectors.toSet())
//                );
//            }
//            if (rcvPendingBundle.isEmpty())
//            {
//                sched.notify();
//            }
//        }
//    }
//
//
//    private void markPendingDispatch(
//        StltUpdateTrackerImpl.UpdateNotification updateNotification,
//        Set<ResourceName> rscSet
//    )
//    {
//        List<FluxSink<ApiCallRc>> responseSink = updateNotification == null ?
//            Collections.emptyList() :
//            updateNotification.getResponseSinks();
//        for (ResourceName rscName : rscSet)
//        {
//            List<FluxSink<ApiCallRc>> responseSinks =
//                pendingDispatchRscs.computeIfAbsent(rscName, ignored -> new ArrayList<>());
//            responseSinks.addAll(responseSink);
//        }
//        synchronized (sched)
//        {
//            sched.notify();
//        }
//        pendingResponseSinks.addAll(responseSink);
//    }
//
//    @Override
//    public void markResourceForDispatch(ResourceName name)
//    {
//        synchronized (sched)
//        {
//            markPendingDispatch(null, Collections.singleton(name));
//            sched.notify();
//        }
//    }
//
//    @Override
//    public void markMultipleResourcesForDispatch(Set<ResourceName> rscSet)
//    {
//        synchronized (sched)
//        {
//            markPendingDispatch(null, rscSet);
//            sched.notify();
//        }
//    }
//
//    @Override
//    public void fullSyncApplied()
//    {
//        Map<ResourceName, List<FluxSink<ApiCallRc>>> dispatchRscs = new TreeMap<>();
//
//        Lock rcfgRdLock = reconfigurationLock.readLock();
//        Lock rscDfnMapRdLock = rscDfnMapLock.readLock();
//        try
//        {
//            rcfgRdLock.lock();
//            rscDfnMapRdLock.lock();
//            for (ResourceName resourceName : rscDfnMap.keySet())
//            {
//                dispatchRscs.put(resourceName, new ArrayList<>());
//            }
//        }
//        finally
//        {
//            rscDfnMapRdLock.unlock();
//            rcfgRdLock.unlock();
//        }
//        synchronized (sched)
//        {
//            // Clear any previously valid state
//            updPendingBundle.clear();
//            rcvPendingBundle.clear();
//
//            pendingDispatchRscs.clear();
//            pendingDispatchRscs.putAll(dispatchRscs);
//
//System.out.println("Fullsync applied, added " + dispatchRscs.size() + " resources for dispatch");
//            sched.notify();
//        }
//    }
//
//    @Override
//    public void abortDeviceHandlers()
//    {
//        loopRunning.set(false);
//    }
//
//    @Override
//    public StltUpdateTracker getUpdateTracker()
//    {
//        return updateTracker;
//    }
//
//    @Override
//    public ServiceName getServiceName()
//    {
//        return DEV_MGR_NAME;
//    }
//
//    @Override
//    public String getServiceInfo()
//    {
//        return SVC_INFO;
//    }
//
//    @Override
//    public ServiceName getInstanceName()
//    {
//        return devMgrInstName;
//    }
//
//    @Override
//    public boolean isStarted()
//    {
//        return serviceRunning.get();
//    }
//
//    @Override
//    public void notifyResourceDispatchResponse(ResourceName resourceName, ApiCallRc response)
//    {
//        // Remember the response and to send combined responses after DeviceHandler instances have finished
//        synchronized (sched)
//        {
//            dispatchResponses.put(resourceName, response);
//        }
//    }
//    @Override
//    public void notifyResourceApplied(Resource rsc)
//    {
//        // Send applySuccess notification to the controller
//
//        Peer ctrlPeer = controllerPeerConnector.getControllerPeer();
//        if (ctrlPeer != null)
//        {
//            Map<StorPool, Either<SpaceInfo, ApiRcException>> spaceInfoQueryMap =
//                apiCallHandlerUtils.getAllSpaceInfo(false);
//
//            Map<StorPool, SpaceInfo> spaceInfoMap = new TreeMap<>();
//
//            spaceInfoQueryMap.forEach((storPool, either) -> either.consume(
//                spaceInfo -> spaceInfoMap.put(storPool, spaceInfo),
//                apiRcException -> errorReporter.reportError(apiRcException.getCause())
//            ));
//
//            ctrlPeer.sendMessage(
//                interComSerializer
//                    .onewayBuilder(InternalApiConsts.API_NOTIFY_RSC_APPLIED)
//                    .notifyResourceApplied(rsc, spaceInfoMap)
//                    .build()
//            );
//        }
//    }
//
//    @Override
//    public void notifyDrbdVolumeResized(Volume vlm)
//    {
//        // Remember the resize to clear the flag after DeviceHandler instances have finished
//        synchronized (sched)
//        {
//            drbdResizedVlmSet.add(new VolumeDefinition.Key(vlm));
//        }
//    }
//
//    @Override
//    public void notifyResourceDeleted(Resource rsc)
//    {
//        // Remember the resource for removal after the DeviceHandler instances have finished
//        synchronized (sched)
//        {
//            deletedRscSet.add(rsc.getDefinition().getName());
//        }
//    }
//
//    @Override
//    public void notifyVolumeDeleted(Volume vlm, long freeSpace)
//    {
//        // Remember the volume for removal after the DeviceHandler instances have finished
//        synchronized (sched)
//        {
//            deletedVlmSet.add(new VolumeDefinition.Key(vlm));
//        }
//    }
//
//    @Override
//    public void notifySnapshotDeleted(Snapshot snapshot)
//    {
//        // Remember the snapshot for removal after the DeviceHandler instances have finished
//        synchronized (sched)
//        {
//            deletedSnapshotSet.add(new SnapshotDefinition.Key(snapshot.getSnapshotDefinition()));
//        }
//    }
//}
