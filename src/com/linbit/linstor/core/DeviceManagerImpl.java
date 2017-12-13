package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.WorkQueue;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.SatelliteCoreServices;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.drbdstate.DrbdEventService;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.Privilege;
import com.linbit.locks.AtomicSyncPoint;
import com.linbit.locks.SyncPoint;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.event.Level;

class DeviceManagerImpl implements Runnable, SystemService, DeviceManager
{
    private final Object sched = new Object();

    private final Satellite stltInstance;
    private final AccessContext wrkCtx;
    private final SatelliteCoreServices coreSvcs;
    private final ErrorReporter errLog;

    private StltUpdateTrackerImpl updTracker;
    private StltUpdateTrackerImpl.UpdateBundle rcvPendingBundle;

    private Thread svcThr;

    private final AtomicBoolean runningFlag = new AtomicBoolean(false);
    private final AtomicBoolean shutdownFlag = new AtomicBoolean(false);

    private static final ServiceName devMgrName;
    static
    {
        try
        {
            devMgrName = new ServiceName("DeviceManager");
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
        rcvPendingBundle = new StltUpdateTrackerImpl.UpdateBundle();
        svcThr = null;
        devMgrInstName = devMgrName;
        drbdHnd = new DrbdDeviceHandler(coreSvcs);
        workQ = workQRef;

        drbdEvent.addDrbdStateChangeObserver(this);
        stateAvailable = drbdEvent.isDrbdStateAvailable();
    }

    /**
     * Dispatch resource to a specific handler depending on type
     */
    void dispatchResource(AccessContext wrkCtx, Resource rsc, SyncPoint phaseLockRef)
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
        // BEGIN DEBUG
        errLog.logTrace("DeviceManager service started");
        // END DEBUG
        SyncPoint phaseLock = new AtomicSyncPoint();
        StltUpdateTrackerImpl.UpdateBundle chgPendingBundle = new StltUpdateTrackerImpl.UpdateBundle();
        try
        {
            // BEGIN DEBUG
            errLog.logTrace("Enabling wrkCtx privileges");
            // END DEBUG
            wrkCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_OBJ_USE, Privilege.PRIV_MAC_OVRD);

            // TODO: Initial startup of all devices

            // TODO: Initial changes to reach the target state

            Set<ResourceName> dispatchRscSet = new TreeSet<>();
            long cycleNr = 0;
            do
            {
                // BEGIN DEBUG
                errLog.logTrace("Begin DeviceManager cycle %d", cycleNr);
                // END DEBUG
                // Wait until resource updates are pending
                synchronized (sched)
                {
                    // BEGIN DEBUG
                    errLog.logTrace("Collecting update notifications");
                    // END DEBUG
                    updTracker.collectUpdateNotifications(chgPendingBundle, shutdownFlag);
                    if (shutdownFlag.get())
                    {
                        break;
                    }

                    chgPendingBundle.copyUpdateRequestsTo(rcvPendingBundle);

                    // BEGIN DEBUG
                    errLog.logTrace("Requesting object updates from the controller");
                    // END DEBUG
                    // Request updates from the controller
                    requestNodeUpdates(chgPendingBundle.updNodeMap);
                    requestRscDfnUpdates(chgPendingBundle.updRscDfnMap);
                    requestRscUpdates(chgPendingBundle.updRscMap);
                    requestStorPoolUpdates(chgPendingBundle.updStorPoolMap);

                    // BEGIN DEBUG
                    errLog.logTrace("Waiting for object updates to arrive");
                    // END DEBUG
                    // Wait for the notification that all requested updates
                    // have been received and applied
                    while (!shutdownFlag.get() && !rcvPendingBundle.isEmpty())
                    {
                        try
                        {
                            sched.wait();
                        }
                        catch (InterruptedException ignored)
                        {
                        }
                    }
                    if (shutdownFlag.get())
                    {
                        break;
                    }
                    // BEGIN DEBUG
                    errLog.logTrace("All object updates were received");
                    // END DEBUG

                    // Merge check requests into update requests and clear the check requests
                    chgPendingBundle.updRscDfnMap.putAll(chgPendingBundle.chkRscMap);
                    chgPendingBundle.chkRscMap.clear();
                    // BEGIN DEBUG
                    errLog.logTrace("All object updates were received");
                    // END DEBUG
                }

                // TODO: if !stateAvailable try to collect additional updates (without losing the current)
                synchronized (sched)
                {
                    // BEGIN DEBUG
                    errLog.logTrace("Waiting for a valid DrbdEventsService state");
                    // END DEBUG
                    while (!shutdownFlag.get() && !stateAvailable)
                    {
                        try
                        {
                            sched.wait();
                        }
                        catch (InterruptedException ignored)
                        {
                        }
                    }
                }

                // Collect the names of all resources that must be dispatched for checking / adjusting
                dispatchRscSet.addAll(chgPendingBundle.chkRscMap.keySet());
                dispatchRscSet.addAll(chgPendingBundle.updRscDfnMap.keySet());
                dispatchRscSet.addAll(chgPendingBundle.updRscMap.keySet());

                // Remember the current phase number for this run of device handlers
                // BEGIN DEBUG
                errLog.logTrace("Scheduling resource handlers");
                // END DEBUG
                for (ResourceName rscName : dispatchRscSet)
                {
                    // Dispatch resources that were affected by changes to worker threads
                    // and to the resource's respective handler
                    ResourceDefinition rscDfn = stltInstance.rscDfnMap.get(rscName);
                    if (rscDfn != null)
                    {
                        Resource rsc = rscDfn.getResource(wrkCtx, stltInstance.localNode.getName());
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
                // BEGIN DEBUG
                errLog.logTrace("Waiting for resource handlers to finish");
                // END DEBUG
                // Wait until the phase advances from the current phase number after all
                // device handlers have finished
                phaseLock.await();
                // BEGIN DEBUG
                errLog.logTrace("All resource handlers finished");
                errLog.logTrace("End DeviceManager cycle %d", cycleNr);
                // END DEBUG
                ++cycleNr;
            }
            while (!shutdownFlag.get());
            errLog.logTrace("DeviceManager service stopped");
        }
        catch (AccessDeniedException accExc)
        {
            shutdownFlag.set(true);
            errLog.reportError(
                Level.ERROR,
                new ImplementationError(
                    "The DeviceManager was started with an access context that does not have sufficient " +
                    "privileges to access all required information",
                    accExc
                )
            );
        }
        finally
        {
            runningFlag.set(false);
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
        return devMgrName;
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
}
