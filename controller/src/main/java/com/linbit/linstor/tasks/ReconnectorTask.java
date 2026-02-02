package com.linbit.linstor.tasks;

import com.linbit.ImplementationError;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.core.CtrlAuthenticator;
import com.linbit.linstor.core.SatelliteConnector;
import com.linbit.linstor.core.apicallhandler.controller.CtrlBackupShippingAbortHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlNodeApiCallHandler;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.satellitestate.SatelliteResourceState;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.tasks.TaskScheduleService.Task;
import com.linbit.linstor.transaction.TransactionException;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.transaction.manager.TransactionMgrGenerator;
import com.linbit.linstor.transaction.manager.TransactionMgrUtil;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockType;
import com.linbit.utils.PairNonNull;

import static com.linbit.locks.LockGuardFactory.LockObj.CTRL_CONFIG;
import static com.linbit.locks.LockGuardFactory.LockObj.NODES_MAP;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import com.google.inject.Key;
import reactor.core.publisher.Flux;
import reactor.util.context.Context;

@Singleton
public class ReconnectorTask implements Task
{
    private static final int RECONNECT_SLEEP = 10_000;
    private static final String STATUS_CONNECTED = "Connected";

    /**
     * must be taken AFTER other possible LINSTOR locks. I.e.
     * <code>try(LockGuard lg = ...){ synchronized(syncObj){ ...}}<code>
     */
    private final Object syncObj = new Object();

    private final AccessContext apiCtx;
    private final HashSet<ReconnectConfig> reconnectorConfigSet = new HashSet<>();
    private final ErrorReporter errorReporter;
    private @Nullable PingTask pingTask;
    private final Provider<CtrlAuthenticator> authenticatorProvider;
    private final Provider<SatelliteConnector> satelliteConnector;
    private final TransactionMgrGenerator transactionMgrGenerator;
    private final LinStorScope reconnScope;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlBackupShippingAbortHandler backupShipAbortHandler;
    private final SystemConfRepository systemConfRepo;
    private final NodeRepository nodeRepository;
    private final Provider<CtrlNodeApiCallHandler> ctrlNodeApiCallHandler;

    @Inject
    public ReconnectorTask(
        @SystemContext AccessContext apiCtxRef,
        ErrorReporter errorReporterRef,
        Provider<CtrlAuthenticator> authenticatorRef,
        Provider<SatelliteConnector> satelliteConnectorRef,
        TransactionMgrGenerator transactionMgrGeneratorRef,
        LinStorScope reconnScopeRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlBackupShippingAbortHandler backupShipAbortHandlerRef,
        SystemConfRepository systemConfRepoRef,
        NodeRepository nodeRepositoryRef,
        Provider<CtrlNodeApiCallHandler> ctrlNodeApiCallHandlerRef
    )
    {
        apiCtx = apiCtxRef;
        errorReporter = errorReporterRef;
        authenticatorProvider = authenticatorRef;
        satelliteConnector = satelliteConnectorRef;
        reconnScope = reconnScopeRef;
        lockGuardFactory = lockGuardFactoryRef;
        transactionMgrGenerator = transactionMgrGeneratorRef;
        backupShipAbortHandler = backupShipAbortHandlerRef;
        systemConfRepo = systemConfRepoRef;
        nodeRepository = nodeRepositoryRef;
        ctrlNodeApiCallHandler = ctrlNodeApiCallHandlerRef;
    }

    void setPingTask(PingTask pingTaskRef)
    {
        pingTask = pingTaskRef;
    }

    public void add(Peer peer, boolean authenticateImmediately)
    {
        add(peer, authenticateImmediately, false);
    }

    public void add(Peer peer, boolean authenticateImmediately, boolean abortBackupShipments)
    {
        boolean sendAuthentication = false;
        @Nullable Node node = peer.getNode();
        synchronized (syncObj)
        {
            if (authenticateImmediately && peer.isConnected(false))
            {
                // no locks needed
                sendAuthentication = true;
                getPingTask().add(peer);
            }
            else
            {
                try
                {
                    if (node != null && !node.isDeleted())
                    {
                        if (!node.getFlags().isSet(apiCtx, Node.Flags.EVICTED))
                        {
                            // Remove any existing configs for this node before adding the new one.
                            // Each reconnection attempt creates a new Peer object, so we need to
                            // clean up stale entries to prevent duplicates from accumulating.
                            // Preserve the oldest offlineSince to maintain correct eviction timing.
                            @Nullable ReconnectConfig oldestRemoved = removeConfigsByNode(node);
                            ReconnectConfig toAdd;
                            if (oldestRemoved != null)
                            {
                                // Preserve offlineSince from the oldest removed config
                                toAdd = new ReconnectConfig(oldestRemoved, peer);
                            }
                            else
                            {
                                toAdd = new ReconnectConfig(peer, drbdConnectionsOk(node));
                            }
                            errorReporter.logDebug("ReconnectorTask add: " + toAdd);
                            reconnectorConfigSet.add(toAdd);
                            getFailedPeers(); // update evictionTime if necessary
                        }
                        else
                        {
                            errorReporter.logInfo(
                                "Node %s is evicted and will not be reconnected",
                                node.getKey().displayValue
                            );
                        }
                    }
                }
                catch (AccessDeniedException exc)
                {
                    throw new ImplementationError(exc);
                }
            }
        }

        if (abortBackupShipments)
        {
            /*
             * FIXME NEEDS PROPER FIX
             *
             * quick and dirty fix. this method could be called by changing the active
             * netif of a node. in this case, the caller is already in a scoped-transaction
             * which means that the above .subscribe will complain about
             *
             * The current scope has already been entered
             *
             * the only way I can think of now would require some rewriting of many method-signatures
             * converting them from non-flux to flux returning methods.
             */
            if (node != null && !node.isDeleted())
            {
                backupShipAbortHandler.abortAllShippingPrivileged(node, true)
                    .contextWrite(
                        Context.of(
                            ApiModule.API_CALL_NAME,
                            "Abort currently shipped snapshots",
                            AccessContext.class,
                            apiCtx,
                            Peer.class,
                            peer
                        )
                    )
                    .subscribe();
            }
        }

        if (sendAuthentication)
        {
            /*
             * DO NOT call this method while locking syncObj!
             */

            // no locks needed
            authenticatorProvider.get().sendAuthentication(peer);

            // do not add peer to ping task, as that is done when the satellite
            // authenticated successfully.
        }
    }

    /**
     * Helper method to get rid of @Nullable.
     */
    private PingTask getPingTask()
    {
        if (pingTask == null)
        {
            throw new ImplementationError("PingTask not yet injected");
        }
        return pingTask;
    }

    /**
     * Removes all ReconnectConfigs for the given node and returns the one with the oldest
     * offlineSince timestamp (or null if none were found).
     * Compares by Node, not Peer identity, because each reconnection attempt creates a new
     * Peer object. This ensures we find and remove all stale entries for the same node.
     * Must be called while holding syncObj lock.
     *
     * Note: This method does NOT clear the node's eviction timestamp. Callers should
     * explicitly call node.setEvictionTimestamp(null) if appropriate (e.g., in peerConnected).
     */
    private @Nullable ReconnectConfig removeConfigsByNode(@Nullable Node node)
    {
        @Nullable ReconnectConfig oldest = null;
        if (node != null)
        {
            if (node.isDeleted())
            {
                cleanupDeletedNode(node);
            }
            else
            {
                oldest = removeAndFindOldestConfig(node);
            }
        }
        return oldest;
    }

    /**
     * Given an already deleted node instance, this method still tries to cleanup possible entries in
     * <code>reconnectorConfigSet</code> using <code>==</code> instead of <code>.equals(..)</code>
     * @param node A Node that is already deleted.
     */
    private void cleanupDeletedNode(Node node)
    {
        Iterator<ReconnectConfig> it = reconnectorConfigSet.iterator();
        while (it.hasNext())
        {
            // we must not use node, but we can still try a cleanup using == instead of .equals
            ReconnectConfig config = it.next();
            if (config.peer.getNode().getKey().equals(node.getKey()))
            {
                it.remove();
            }
        }
    }

    /**
     * <p>This method assumes the parameter is not just non-null, but also non-deleting.
     * Since this method cannot enforce that no other thread deletes the given node while we iterate over our internal
     * <code>reconnectorConfigSet</code>, all calls of "node.delete(..)" must make sure that the ReconnectorTask
     * already gave up reconnecting to the given node.</p>
     * <p>This method searches and returns the oldest {@link ReconnectConfig} (if any) based on
     * {@link ReconnectConfig#offlineSince}.</p>
     * <p>This method also deletes all {@link ReconnectConfig} entries with peers that point to the given node.
     * @param node The node for which the oldest (if existing) {@link ReconnectConfig} should be returned
     */
    private @Nullable ReconnectConfig removeAndFindOldestConfig(Node node)
    {
        @Nullable ReconnectConfig oldest = null;
        Iterator<ReconnectConfig> it = reconnectorConfigSet.iterator();
        while (it.hasNext())
        {
            ReconnectConfig config = it.next();
            @Nullable Node configNode = config.peer.getNode();
            if (configNode != null && configNode.isDeleted())
            {
                // just remove it
                it.remove();
            }
            else if (node.equals(configNode))
            {
                errorReporter.logDebug("ReconnectorTask remove: " + config);
                it.remove();
                if (oldest == null || config.offlineSince < oldest.offlineSince)
                {
                    oldest = config;
                }
            }
        }
        return oldest;
    }

    public void peerConnected(Peer peer)
    {
        boolean sendAuthentication = false;
        synchronized (syncObj)
        {
            @Nullable Node node = peer.getNode();
            @Nullable ReconnectConfig removed = removeConfigsByNode(node);
            if (removed != null)
            {
                // Node has reconnected, clear eviction timestamp
                if (pingTask != null)
                {
                    sendAuthentication = true;
                }
            }
            if (node != null)
            {
                node.setEvictionTimestamp(null);
            }
        }
        if (sendAuthentication)
        {
            /*
             * DO NOT call this method while locking syncObj!
             */

            // no locks needed
            authenticatorProvider.get().sendAuthentication(peer);

            // do not add peer to ping task, as that is done when the satellite
            // authenticated successfully.
        }
    }

    public void removePeer(Peer peer)
    {
        synchronized (syncObj)
        {
            removeConfigsByNode(peer.getNode());
            getPingTask().remove(peer);
        }
    }

    @Override
    public long run(long scheduleAt)
    {
        PairNonNull<ArrayList<ReconnectConfig>, ArrayList<PairNonNull<Flux<ApiCallRc>, Peer>>> pair;
        try (LockGuard lockGuard = lockGuardFactory.build(LockType.READ, NODES_MAP))
        {
            synchronized (syncObj)
            {
                // we are iterating the nodes and need to prevent getting a node deleted while we are iterating them
                pair = getFailedPeers();
            }
        }
        if (!pair.objA.isEmpty())
        {
            errorReporter.logTrace("Reconnect list: " + pair.objA);
        }
        ArrayList<ReconnectConfig> localList = pair.objA;
        runEvictionFluxes(pair.objB);

        // Track nodes we've already processed in this cycle to avoid duplicate reconnect
        // attempts when multiple stale configs exist for the same node.
        Set<NodeName> processedNodes = new HashSet<>();

        for (final ReconnectConfig config : localList)
        {
            @Nullable Node node = config.peer.getNode();
            @Nullable NodeName nodeName = node == null ? null : node.getKey(); // getKey avoids checkDeleted()
            if (node == null || processedNodes.contains(nodeName))
            {
                if (node == null)
                {
                    errorReporter.logDebug("Node of peer " + config.peer + " is null. Skipping");
                }
                else
                {
                    errorReporter.logDebug(
                        "Node (" + node + ") of peer " + config.peer + " already processed. Skipping"
                    );
                }
            }
            else
            {
                processedNodes.add(nodeName);

                // Check if the Node's current peer is connected, not the stale peer in config.
                // The node might have reconnected via a different Peer object.
                @Nullable Peer currentPeer = null;
                try
                {
                    currentPeer = node.getPeer(apiCtx);
                }
                catch (AccessDeniedException ignored)
                {
                    // should not happen with apiCtx
                }
                if (currentPeer != null && currentPeer.isConnected(false))
                {
                    errorReporter.logInfo(
                        "Node " + node.getName() + " has connected. Removed from reconnectList, added to pingList."
                    );
                    peerConnected(currentPeer);
                }
                else
                {
                    errorReporter.logDebug(
                        "Peer " + config.peer.getId() + " has not connected yet, retrying connect."
                    );
                    try
                    {
                        if (node.isDeleted())
                        {
                            errorReporter.logDebug(
                                "Peer %s's node got deleted, removing from reconnect list",
                                config.peer.getId()
                            );
                            removePeer(config.peer);
                        }
                        else
                        {
                            reconnectImpl(config);
                        }
                    }
                    catch (IOException ioExc)
                    {
                        // TODO: detailed error reporting
                        errorReporter.reportError(ioExc);
                    }
                }
            }
        }
        return getNextFutureReschedule(scheduleAt, RECONNECT_SLEEP);
    }

    /**
     * @param config The config that should be used for reconnect.
     * @throws IOException Thrown when something fails during actual reconnect
     */
    private void reconnectImpl(final ReconnectConfig config) throws IOException
    {
        @Nullable Node node = config.peer.getNode();
        if (node != null && !node.isDeleted())
        {
            // transactionMgr MUST be started before taking any linstor locks in order to avoid a deadlock with database
            // internal locks
            // worst case scenario: since we just checked if the node is already deleted (outside of any lock... which
            // is not great, but should be fine), all that might happen between now and when we have acquired the lock
            // is that the node gets deleted. In that case, we still have a second check within the try (with locks) to
            // deal with that situation, so we can also return the (unnecessarily created) transaction immediately again
            TransactionMgr transMgr = transactionMgrGenerator.startTransaction();
            try (
                LockGuard ignore = lockGuardFactory.create()
                    .read(CTRL_CONFIG)
                    .write(NODES_MAP)
                    .build();
                LinStorScope.ScopeAutoCloseable close = reconnScope.enter()
            )
            {
                // another check needed to detect race conditions (someone could have called node.delete() while we were
                // waiting for the lock)
                if (!node.isDeleted())
                {
                    reconnScope.seed(Key.get(AccessContext.class, PeerContext.class), apiCtx);
                    TransactionMgrUtil.seedTransactionMgr(reconnScope, transMgr);

                    // look for another netIf configured as satellite connection and set it as active
                    setNextNetIf(node, config);

                    transMgr.commit();
                    synchronized (syncObj)
                    {
                        Peer reconnectedPeer = config.peer.getConnector().reconnect(config.peer);
                        reconnectorConfigSet.remove(config);
                        reconnectorConfigSet.add(new ReconnectConfig(config, reconnectedPeer));
                    }
                }
            }
            catch (AccessDeniedException | DatabaseException exc)
            {
                errorReporter.logError(exc.getMessage());
            }
            finally
            {
                try
                {
                    transMgr.rollback();
                }
                catch (TransactionException exc)
                {
                    errorReporter.reportError(exc);
                }
                finally
                {
                    transMgr.returnConnection();
                }

            }
        }
    }

    private void runEvictionFluxes(ArrayList<PairNonNull<Flux<ApiCallRc>, Peer>> fluxList)
    {
        for (PairNonNull<Flux<ApiCallRc>, Peer> pair : fluxList)
        {
            Peer peer = pair.objB;
            pair.objA.contextWrite(
                Context.of(
                    ApiModule.API_CALL_NAME,
                    "Recon:AutoEvicting",
                    AccessContext.class,
                    peer.getAccessContext(),
                    Peer.class,
                    peer
                )
            )
            .subscribe();
        }
    }

    /**
     * Sets the next usable network interface of the given node as active. If a node has 3 or more NetIfs, this method
     * makes sure to not simply toggle between the first two (first non-selected netIf), but to actually iterate
     * through all available NetIfs before starting at the first again.
     */
    private void setNextNetIf(Node node, ReconnectConfig config) throws AccessDeniedException, DatabaseException
    {
        final @Nullable NetInterface currentActiveStltConn = node.getActiveStltConn(config.peer.getAccessContext());
        Iterator<NetInterface> netIfIt = node.iterateNetInterfaces(config.peer.getAccessContext());
        @Nullable NetInterface firstNetIf = null;
        @Nullable NetInterface nextNetIf = null;
        boolean setNext = false;
        boolean netIfUpdated = false;
        while (netIfIt.hasNext())
        {
            NetInterface netInterface = netIfIt.next();
            boolean usableAsStltConn = netInterface.isUsableAsStltConn(config.peer.getAccessContext());
            if (firstNetIf == null && usableAsStltConn)
            {
                firstNetIf = netInterface;
            }

            if (netInterface.equals(currentActiveStltConn))
            {
                setNext = true;
            }
            else
            if (setNext && usableAsStltConn)
            {
                // already after current connection, set new connection
                errorReporter.logInfo(
                    "Setting new active satellite connection on " + node.getKey().displayValue + " '" +
                        netInterface.getName() + "' " +
                        netInterface.getAddress(config.peer.getAccessContext()).getAddress()
                );
                node.setActiveStltConn(config.peer.getAccessContext(), netInterface);
                netIfUpdated = true;
                break;
            }
        }
        if (!netIfUpdated)
        {
            nextNetIf = firstNetIf;
        }

        if (nextNetIf != null)
        {
            if (!nextNetIf.equals(currentActiveStltConn))
            {
                errorReporter.logInfo(
                    "Setting new active satellite connection: '" +
                        nextNetIf.getName() + "' " +
                        nextNetIf.getAddress(config.peer.getAccessContext()).getAddress()
                );
                node.setActiveStltConn(config.peer.getAccessContext(), nextNetIf);
            }
        }
        else
        {
            errorReporter.logWarning(
                "No network interfaces usable for satellite communication found for node '" + node.getName() + "'"
            );
        }
    }

    public ArrayList<PairNonNull<Flux<ApiCallRc>, Peer>> rerunConfigChecks()
    {
        synchronized (syncObj)
        {
            return getFailedPeers().objB;
        }
    }

    private PairNonNull<ArrayList<ReconnectConfig>, ArrayList<PairNonNull<Flux<ApiCallRc>, Peer>>> getFailedPeers()
    {
        ArrayList<ReconnectConfig> retry = new ArrayList<>();
        ArrayList<ReconnectConfig> copy = new ArrayList<>(reconnectorConfigSet);
        ArrayList<PairNonNull<Flux<ApiCallRc>, Peer>> fluxes = new ArrayList<>();
        for (ReconnectConfig config : copy)
        {
            try
            {
                @Nullable Node node = config.peer.getNode();
                if (node != null && !node.isDeleted())
                {
                    boolean drbdOkNew = drbdConnectionsOk(node);
                    final PriorityProps props = new PriorityProps(
                        node.getProps(apiCtx),
                        systemConfRepo.getCtrlConfForView(apiCtx)
                    );
                    final long timeout = Long.parseLong(
                        props.getProp(
                            ApiConsts.KEY_AUTO_EVICT_AFTER_TIME,
                            ApiConsts.NAMESPC_DRBD_OPTIONS,
                            "60" // 1 hour
                        )
                    ) * 60 * 1000; // to milliseconds
                    final boolean allowEviction = Boolean.parseBoolean(
                        props.getProp(
                            ApiConsts.KEY_AUTO_EVICT_ALLOW_EVICTION,
                            ApiConsts.NAMESPC_DRBD_OPTIONS,
                            "false"
                        )
                    );
                    if (config.drbdOk != drbdOkNew)
                    {
                        config.drbdOk = drbdOkNew;
                        if (!config.drbdOk)
                        {
                            config.offlineSince = System.currentTimeMillis();
                        }
                    }
                    if (!node.getFlags().isSet(apiCtx, Node.Flags.EVICTED))
                    {
                        retry.add(config);
                        long evictionTimestamp = config.offlineSince + timeout;
                        if (allowEviction)
                        {
                            node.setEvictionTimestamp(evictionTimestamp);
                        }
                        else
                        {
                            node.setEvictionTimestamp(null);
                        }
                        if (!config.drbdOk && System.currentTimeMillis() >= evictionTimestamp)
                        {
                            if (allowEviction)
                            {
                                int numDiscon = reconnectorConfigSet.size();
                                int maxPercentDiscon = Integer.parseInt(
                                    props.getProp(
                                        ApiConsts.KEY_AUTO_EVICT_MAX_DISCONNECTED_NODES,
                                        ApiConsts.NAMESPC_DRBD_OPTIONS,
                                        "34"
                                    )
                                );
                                int numNodes = nodeRepository.getMapForView(apiCtx).size();
                                int maxDiscon = Math.round(maxPercentDiscon * numNodes / 100.0f);
                                if (numDiscon <= maxDiscon)
                                {
                                    errorReporter.logInfo(
                                        config.peer + " has been offline for too long, relocation of resources started."
                                    );
                                    fluxes.add(
                                        new PairNonNull<>(
                                            ctrlNodeApiCallHandler.get().declareEvicted(node),
                                            config.peer
                                        )
                                    );

                                    // evicted, stop trying reconnect
                                    retry.remove(config);
                                    reconnectorConfigSet.remove(config);
                                }
                                else
                                {
                                    errorReporter.logInfo(
                                        "Currently more than %d%% nodes are not connected to the controller. " +
                                            "The controller might have a problem with it's connections, therefore " +
                                            "no nodes will be declared as EVICTED",
                                        maxPercentDiscon
                                    );
                                }
                            }
                            else
                            {
                                errorReporter.logDebug(
                                    "The node %s will not be evicted since the property AutoEvictAllowEviction is " +
                                        "set to false.",
                                    node.getKey().displayValue
                                );
                            }
                        }
                    }
                }
            }
            catch (AccessDeniedException exc)
            {
                errorReporter.reportError(exc);
            }
        }
        return new PairNonNull<>(retry, fluxes);
    }

    private boolean drbdConnectionsOk(Node node)
    {
        boolean drbdOk = false;

        try
        {
            Set<Node> neighbors = getNeighbors(node);
            for (Node neighbor : neighbors)
            {
                if (neighbor.getPeer(apiCtx) != null)
                {
                    Map<ResourceName, SatelliteResourceState> neighborRscStates = neighbor.getPeer(apiCtx)
                        .getSatelliteState().getResourceStates();

                    for (SatelliteResourceState neighborRscState : neighborRscStates.values())
                    {
                        Map<NodeName, Map<NodeName, String>> connStates = neighborRscState.getConnectionStates();
                        for (Entry<NodeName, Map<NodeName, String>> conStateEntry : connStates.entrySet())
                        {
                            if (conStateEntry.getKey().equals(node.getKey()))
                            {
                                Map<NodeName, String> conStates = conStateEntry.getValue();
                                for (Entry<NodeName, String> conEntry : conStates.entrySet())
                                {
                                    String conVal = conEntry.getValue();
                                    if (conVal.equalsIgnoreCase(STATUS_CONNECTED))
                                    {
                                        drbdOk = true;
                                        break;
                                    }
                                }
                            }
                            else
                            {
                                String conValOrNull = conStateEntry.getValue().get(node.getKey());
                                if (conValOrNull != null && conValOrNull.equalsIgnoreCase(STATUS_CONNECTED))
                                {
                                    drbdOk = true;
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }

        return drbdOk;
    }

    private Set<Node> getNeighbors(Node nodeToCheck) throws AccessDeniedException
    {
        Set<Node> neighbors = new HashSet<>();
        Iterator<Resource> localRscIt;
        localRscIt = nodeToCheck.iterateResources(apiCtx);
        while (localRscIt.hasNext())
        {
            Resource localRsc = localRscIt.next();
            ResourceDefinition rscDfn = localRsc.getResourceDefinition();
            Iterator<Resource> rscIt = rscDfn.iterateResource(apiCtx);
            while (rscIt.hasNext())
            {
                Resource otherRsc = rscIt.next();
                if (!otherRsc.equals(localRsc))
                {
                    neighbors.add(otherRsc.getNode());
                }
            }
        }
        return neighbors;
    }

    public void startReconnecting(Collection<Node> nodes, AccessContext initCtx)
    {
        /*
         * We need this method so that all nodes are added starting connecting while having
         * this syncObj. If we would give up the syncObj between two startConnecting calls
         * we might run into a deadlock where one thread tries to connect (awaits authentication)
         * and another thread tries to start connecting the next node.
         */
        synchronized (syncObj)
        {
            SatelliteConnector stltConnector = satelliteConnector.get();
            for (Node node : nodes)
            {
                errorReporter.logDebug("Reconnecting to node '" + node.getKey().displayValue + "'.");
                stltConnector.startConnecting(node, initCtx);
            }
        }
    }

    private static class ReconnectConfig
    {
        private final Peer peer;
        private long offlineSince;
        private boolean drbdOk;

        private ReconnectConfig(Peer peerRef, boolean drbdRef)
        {
            peer = peerRef;
            drbdOk = drbdRef;
            offlineSince = System.currentTimeMillis();
        }

        private ReconnectConfig(ReconnectConfig other, Peer peerRef)
        {
            peer = peerRef;
            drbdOk = other.drbdOk;
            offlineSince = other.offlineSince;
        }

        @Override
        public String toString()
        {
            return "ReconnectConfig [peer=" + peer + ", offlineSince=" + offlineSince + ", drbdOk=" + drbdOk + "]";
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + peer.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (obj == null)
            {
                return false;
            }
            if (getClass() != obj.getClass())
            {
                return false;
            }
            ReconnectConfig other = (ReconnectConfig) obj;
            return Objects.equals(peer, other.peer);
        }
    }
}
