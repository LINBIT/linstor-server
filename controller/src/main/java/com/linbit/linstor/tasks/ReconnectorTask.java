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
import com.linbit.linstor.core.apicallhandler.controller.CtrlNodeApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlBackupShippingAbortHandler;
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
        Node node = peer.getNode();
        synchronized (syncObj)
        {
            if (authenticateImmediately && peer.isConnected(false))
            {
                // no locks needed
                sendAuthentication = true;
                pingTask.add(peer);
            }
            else
            {
                try
                {
                    if (!node.isDeleted())
                    {
                        if (!node.getFlags().isSet(apiCtx, Node.Flags.EVICTED))
                        {
                            reconnectorConfigSet.add(new ReconnectConfig(peer, drbdConnectionsOk(node)));
                            getFailedPeers(); // update evictionTime if necessary
                        }
                        else
                        {
                            errorReporter.logDebug("Node %s is evicted and will not be reconnected", node.getName());
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
            if (!node.isDeleted())
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

    public void peerConnected(Peer peer)
    {
        boolean sendAuthentication = false;
        synchronized (syncObj)
        {
            Object removed = null;
            ReconnectConfig toRemove = null;
            for (ReconnectConfig config : reconnectorConfigSet)
            {
                if (config.peer.equals(peer))
                {
                    toRemove = config;
                }
            }
            if (toRemove != null)
            {
                toRemove.peer.getNode().setEvictionTimestamp(null);
                removed = reconnectorConfigSet.remove(toRemove);
            }
            if (removed != null && pingTask != null)
            {
                sendAuthentication = true;
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
            ReconnectConfig toRemove = null;
            for (ReconnectConfig config : reconnectorConfigSet)
            {
                if (config.peer.equals(peer))
                {
                    toRemove = config;
                }
            }
            if (toRemove != null)
            {
                reconnectorConfigSet.remove(toRemove);
            }
            pingTask.remove(peer);
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
        ArrayList<ReconnectConfig> localList = pair.objA;
        runEvictionFluxes(pair.objB);

        for (final ReconnectConfig config : localList)
        {
            if (config.peer.isConnected(false))
            {
                errorReporter.logTrace(
                    config.peer + " has connected. Removed from reconnectList, added to pingList."
                );
                peerConnected(config.peer);
            }
            else
            {
                errorReporter.logTrace(
                    "Peer " + config.peer.getId() + " has not connected yet, retrying connect."
                );
                try
                {
                    Node node = config.peer.getNode();
                    if (node != null && !node.isDeleted())
                    {
                        // transactionMgr MUST be started before taking any linstor locks in order to avoid a deadlock
                        // with database internal locks
                        // worst case scenario: since we just checked if the node is already deleted (outside of any
                        // lock... which is not great, but should be fine), all that might happen between now and when
                        // we have acquired the lock is that the node gets deleted. In that case, we still have a second
                        // check within the try (with locks) to deal with that situation, so we can also return the
                        // (unnecessarily created) transaction immediately again
                        TransactionMgr transMgr = transactionMgrGenerator.startTransaction();
                        try (LockGuard ignore = lockGuardFactory.create()
                            .read(CTRL_CONFIG)
                            .write(NODES_MAP)
                            .build();
                            LinStorScope.ScopeAutoCloseable close = reconnScope.enter()
                        )
                        {
                            // another check needed to detect race conditions (someone could have called node.delete()
                            // while we were waiting for the lock)
                            if (!node.isDeleted())
                            {
                                reconnScope.seed(Key.get(AccessContext.class, PeerContext.class), apiCtx);
                                TransactionMgrUtil.seedTransactionMgr(reconnScope, transMgr);

                                // look for another netIf configured as satellite connection and set it as active
                                setNetIf(node, config);

                                transMgr.commit();
                                synchronized (syncObj)
                                {
                                    reconnectorConfigSet.add(
                                        new ReconnectConfig(
                                            config,
                                            config.peer.getConnector().reconnect(config.peer)
                                        )
                                    );
                                    reconnectorConfigSet.remove(config);
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
                    else
                    {
                        if (node == null)
                        {
                            errorReporter.logTrace(
                                "Peer %s's node is null (possibly rollbacked), removing from reconnect list",
                                config.peer.getId()
                            );
                        }
                        else
                        {
                            errorReporter.logTrace(
                                "Peer %s's node got deleted, removing from reconnect list",
                                config.peer.getId()
                            );
                        }
                        removePeer(config.peer);
                    }
                }
                catch (IOException ioExc)
                {
                    // TODO: detailed error reporting
                    errorReporter.reportError(ioExc);
                }
            }
        }
        return getNextFutureReschedule(scheduleAt, RECONNECT_SLEEP);
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

    private void setNetIf(Node node, ReconnectConfig config) throws AccessDeniedException, DatabaseException
    {
        NetInterface currentActiveStltConn = node
            .getActiveStltConn(config.peer.getAccessContext());
        Iterator<NetInterface> netIfIt = node
            .iterateNetInterfaces(config.peer.getAccessContext());
        NetInterface firstPossible = null;
        boolean setIf = false;
        while (netIfIt.hasNext())
        {
            NetInterface netInterface = netIfIt.next();
            if (netInterface.isUsableAsStltConn(config.peer.getAccessContext()))
            {
                // NetIf usable as StltConn
                if (!setIf && firstPossible == null && !netInterface.equals(currentActiveStltConn))
                {
                    // current connection not found yet, set default if none found
                    firstPossible = netInterface;
                }
                else if (setIf)
                {
                    // already after current connection, set new connection
                    errorReporter.logDebug(
                        "Setting new active satellite connection on " + node.getName().displayValue + " '" +
                            netInterface.getName() + "' " +
                            netInterface.getAddress(config.peer.getAccessContext()).getAddress()
                    );
                    node.setActiveStltConn(config.peer.getAccessContext(), netInterface);
                    break;
                }
            }
            if (netInterface.equals(currentActiveStltConn))
            {
                // current connection found, allow new connection after this
                setIf = true;
            }
        }
        if (currentActiveStltConn.equals(node.getActiveStltConn(config.peer.getAccessContext())))
        {
            // current connection not found, use default
            if (firstPossible != null)
            {
                errorReporter.logDebug(
                    "Setting new active satellite connection: '" +
                        firstPossible.getName() + "' " +
                        firstPossible.getAddress(config.peer.getAccessContext()).getAddress()
                );
                node.setActiveStltConn(config.peer.getAccessContext(), firstPossible);
            }
            // else do nothing and reuse current connection, since no other connection is valid
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
                Node node = config.peer.getNode();
                if (!node.isDeleted())
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
                                    errorReporter.logTrace(
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
                                    errorReporter.logTrace(
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
                                    "The node %s will not be evicted since the property AutoEvictAllowEviction is set " +
                                        "to false.",
                                    node.getName()
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
                        for (
                            Entry<NodeName, Map<NodeName, String>> conStateEntry : neighborRscState.getConnectionStates()
                                .entrySet()
                        )
                        {
                            if (conStateEntry.getKey().equals(node.getName()))
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
                                String conValOrNull = conStateEntry.getValue().get(node.getName());
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
                errorReporter.logDebug("Reconnecting to node '" + node.getName() + "'.");
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
