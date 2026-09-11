package com.linbit.linstor.core.apicallhandler;

import com.linbit.PlatformStlt;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer.CommonSerializerBuilder;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.api.prop.WhitelistPropsReconfigurator;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.CtrlAuth;
import com.linbit.linstor.api.protobuf.FullSync;
import com.linbit.linstor.api.protobuf.ProtoUuidUtils;
import com.linbit.linstor.backupshipping.BackupShippingMgr;
import com.linbit.linstor.core.ApplicationLifecycleManager;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.core.StltSecurityObjects;
import com.linbit.linstor.core.UpdateMonitorImpl;
import com.linbit.linstor.core.cfg.StltConfig;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.migration.StltMigrationHandler;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.event.EventBroker;
import com.linbit.linstor.layer.drbd.drbdstate.DrbdEventPublisher;
import com.linbit.linstor.layer.drbd.drbdstate.DrbdStateTracker;
import com.linbit.linstor.layer.storage.DeviceProviderMapper;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntAuthOuterClass.MsgIntAuth;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import jakarta.inject.Provider;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import org.mockito.Answers;

/**
 * Reproduces the "double reconnect" fullSyncId race:
 *
 * <p>
 * During a reconnect the controller can end up with two connections to the same satellite (a stale and a fresh
 * one), both delivering an Auth message. Both Auth handlers run concurrently on the satellite. Each of them
 * calls {@code setControllerPeer} (last one wins) and draws a new fullSyncId via
 * {@code UpdateMonitor.getNextFullSyncId()} - but those are two separate critical sections, so the two
 * handlers can interleave:
 * </p>
 * <ol>
 * <li>stale connection's auth runs {@code setControllerPeer(stale)}</li>
 * <li>fresh connection's auth runs {@code setControllerPeer(fresh)} (closes the stale connection, wins)</li>
 * <li>fresh connection's auth draws fullSyncId N and answers AUTH_ACCEPT(N) over the live connection</li>
 * <li>stale connection's auth draws fullSyncId N+1 - but its AUTH_ACCEPT(N+1) is sent into the already
 * closed stale connection and never reaches the controller</li>
 * </ol>
 * <p>
 * The satellite now expects a FullSync with id N+1 while the controller sends one with id N over the live
 * connection. The satellite discards it as outdated - see CI run 1442553,
 * test_migrate_zfs_cloned_snapshot_marked_for_deletion-1-crd-9.
 * </p>
 */
public class StltFullSyncIdRaceTest
{
    private static final int GATE_TIMEOUT_SEC = 10;

    private static final UUID CTRL_UUID = UUID.randomUUID();
    private static final UUID NODE_UUID = UUID.randomUUID();
    private static final String NODE_NAME = "node1";

    private final ThreadLocal<Peer> currentPeer = new ThreadLocal<>();
    private final Map<Thread, Long> fullSyncIdsSentByThread = new ConcurrentHashMap<>();

    private UpdateMonitorImpl updateMonitor;
    private TrackingControllerPeerConnector peerConnector;
    private StltApiCallHandler stltApiCallHandler;
    private CtrlAuth ctrlAuth;
    private Props stltProps;

    @Before
    public void setUp() throws Exception
    {
        ReadWriteLock reconfigurationLock = new ReentrantReadWriteLock(true);
        ReadWriteLock nodesMapLock = new ReentrantReadWriteLock(true);
        ReadWriteLock rscDfnMapLock = new ReentrantReadWriteLock(true);
        ReadWriteLock storPoolDfnMapLock = new ReentrantReadWriteLock(true);
        ReadWriteLock extFileMapLock = new ReentrantReadWriteLock(true);
        ReadWriteLock remoteMapLock = new ReentrantReadWriteLock(true);

        updateMonitor = new UpdateMonitorImpl(
            reconfigurationLock,
            nodesMapLock,
            rscDfnMapLock,
            storPoolDfnMapLock
        );
        peerConnector = new TrackingControllerPeerConnector();
        stltProps = mock(Props.class);

        StltExtToolsChecker extToolsChecker = mock(StltExtToolsChecker.class);
        when(extToolsChecker.getExternalTools(anyBoolean())).thenReturn(Collections.emptyMap());

        TransactionMgr transMgr = mock(TransactionMgr.class);
        Provider<TransactionMgr> transMgrProvider = () -> transMgr;
        Provider<Long> apiCallIdProvider = () -> 1L;

        stltApiCallHandler = new StltApiCallHandler(
            mock(ErrorReporter.class, withSettings().defaultAnswer(Answers.RETURNS_DEFAULTS)),
            mock(StltConfig.class),
            peerConnector,
            updateMonitor,
            mock(DeviceManager.class),
            mock(ApplicationLifecycleManager.class),
            mock(StltNodeApiCallHandler.class),
            mock(StltRscApiCallHandler.class),
            mock(StltStorPoolApiCallHandler.class),
            mock(StltSnapshotApiCallHandler.class),
            mock(StltExternalFilesApiCallHandler.class),
            mock(StltRemoteApiCallHandler.class),
            extToolsChecker,
            mock(CtrlStltSerializer.class),
            reconfigurationLock,
            nodesMapLock,
            rscDfnMapLock,
            storPoolDfnMapLock,
            extFileMapLock,
            remoteMapLock,
            stltProps,
            mock(CoreModule.NodesMap.class),
            mock(CoreModule.ResourceDefinitionMap.class),
            mock(CoreModule.StorPoolDefinitionMap.class),
            transMgrProvider,
            mock(StltSecurityObjects.class),
            mock(StltCryptApiCallHelper.class),
            mock(EventBroker.class),
            mock(WhitelistPropsReconfigurator.class),
            apiCallIdProvider,
            mock(DrbdStateTracker.class),
            mock(DrbdEventPublisher.class),
            mock(DeviceProviderMapper.class),
            mock(BackupShippingMgr.class),
            mock(StltApiCallHandlerUtils.class),
            mock(PlatformStlt.class),
            mock(StltMigrationHandler.class)
        );

        CommonSerializerBuilder serializerBuilder = mock(
            CommonSerializerBuilder.class,
            withSettings().defaultAnswer(Answers.RETURNS_SELF)
        );
        doAnswer(invocation ->
        {
            fullSyncIdsSentByThread.put(Thread.currentThread(), invocation.getArgument(0));
            return invocation.getMock();
        })
            .when(serializerBuilder)
            .authSuccess(
                anyLong(), any(), any(), any(), any(), any(), any(), any(), anyBoolean(), anyBoolean(),
                any(), any(), any(), any(), anyBoolean(), anyBoolean(), any(), any(), any(), any(), any()
            );
        when(serializerBuilder.build()).thenReturn(new byte[0]);

        CommonSerializer commonSerializer = mock(CommonSerializer.class);
        when(commonSerializer.headerlessBuilder()).thenReturn(serializerBuilder);

        ApiCallAnswerer apiCallAnswerer = mock(ApiCallAnswerer.class);
        when(apiCallAnswerer.answerBytes(any(), anyString())).thenReturn(new byte[0]);

        StltConfig stltConfig = mock(StltConfig.class);
        when(stltConfig.getWhitelistedExternalFilePaths()).thenReturn(Collections.emptySet());

        ctrlAuth = new CtrlAuth(
            mock(ErrorReporter.class),
            stltApiCallHandler,
            apiCallAnswerer,
            commonSerializer,
            currentPeer::get,
            stltConfig,
            mock(WhitelistProps.class)
        );
    }

    /**
     * Reproduces the deadly interleaving observed in CI run 1442553: the connection that got superseded during
     * authentication draws its fullSyncId AFTER the live connection drew its own. The satellite must not end up
     * expecting a fullSyncId that was never delivered to the controller: whatever id the satellite sent over
     * the live (active) controller connection must be the id the satellite expects for the next FullSync.
     */
    @Test
    public void concurrentAuthMustNotInvalidateFullSyncIdOfActiveConnection() throws Exception
    {
        Peer stalePeer = mockPeer("stalePeer");
        Peer freshPeer = mockPeer("freshPeer");

        CountDownLatch staleAuthenticated = new CountDownLatch(1);
        CountDownLatch freshAuthCompleted = new CountDownLatch(1);

        /*
         * authenticate() calls stltProps.setProp(KEY_NODE_NAME, ...) after setControllerPeer(...) but before
         * CtrlAuth draws the next fullSyncId. Pause the stale connection's auth exactly there until the fresh
         * connection's auth has completely finished (including drawing its fullSyncId and answering).
         */
        doAnswer(invocation ->
        {
            if (currentPeer.get() == stalePeer)
            {
                staleAuthenticated.countDown();
                freshAuthCompleted.await(GATE_TIMEOUT_SEC, TimeUnit.SECONDS);
            }
            return null;
        }).when(stltProps).setProp(anyString(), anyString());

        Thread staleAuthThread = startAuthThread(stalePeer, "stale-auth");
        assertTrue(
            "stale connection's auth did not reach setControllerPeer",
            staleAuthenticated.await(GATE_TIMEOUT_SEC, TimeUnit.SECONDS)
        );

        // now that the stale connection's auth already ran setControllerPeer(stale), run the fresh
        // connection's auth to completion. this closes the stale connection and makes fresh the active one
        Thread freshAuthThread = startAuthThread(freshPeer, "fresh-auth");
        freshAuthThread.join(TimeUnit.SECONDS.toMillis(GATE_TIMEOUT_SEC));
        assertFalse("fresh connection's auth did not finish", freshAuthThread.isAlive());
        freshAuthCompleted.countDown();

        // let the stale connection's auth finish (drawing its fullSyncId AFTER the fresh connection's auth)
        staleAuthThread.join(TimeUnit.SECONDS.toMillis(GATE_TIMEOUT_SEC));
        assertFalse("stale connection's auth did not finish", staleAuthThread.isAlive());

        assertSame("the fresh connection must be the active controller peer",
            freshPeer,
            peerConnector.getControllerPeer()
        );

        @Nullable Long fullSyncIdSentToLiveConn = fullSyncIdsSentByThread.get(freshAuthThread);
        assertNotNull(
            "no AUTH_ACCEPT was sent over the live (active) controller connection",
            fullSyncIdSentToLiveConn
        );
        assertEquals(
            "the satellite expects a different fullSyncId than it sent over the live (active) controller " +
                "connection. The expected id was sent into the already closed stale connection and will " +
                "never reach the controller, so the next FullSync would be refused as outdated forever.",
            (long) fullSyncIdSentToLiveConn,
            updateMonitor.getCurrentFullSyncId()
        );

        @Nullable Long fullSyncIdSentToStaleConn = fullSyncIdsSentByThread.get(staleAuthThread);
        assertNull(
            "an AUTH_ACCEPT was sent over the stale (already closed and superseded) connection",
            fullSyncIdSentToStaleConn
        );
    }

    /**
     * A FullSync that the satellite discards because its fullSyncId is outdated must NOT be answered with
     * SUCCESS: the controller would consider the node ONLINE while the satellite has no data at all and is
     * still waiting for a FullSync that will never arrive.
     */
    @Test
    public void outdatedFullSyncMustNotReportSuccess() throws Exception
    {
        long expectedFullSyncId = updateMonitor.getNextFullSyncId();

        FullSync.FullSyncResult result = stltApiCallHandler.applyFullSync(
            Collections.emptyMap(),
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet(),
            expectedFullSyncId - 1, // outdated
            new byte[0],
            new byte[0],
            new byte[0],
            new byte[0]
        );

        assertNotEquals(
            "an outdated (and therefore completely ignored) FullSync must not be answered with SUCCESS",
            FullSync.FullSyncStatus.SUCCESS,
            statusOf(result)
        );
    }

    private Thread startAuthThread(Peer peer, String threadName)
    {
        Thread thread = new Thread(
            () ->
            {
                currentPeer.set(peer);
                try
                {
                    ctrlAuth.execute(buildAuthMessage());
                }
                catch (IOException exc)
                {
                    throw new RuntimeException(exc);
                }
            },
            threadName
        );
        thread.start();
        return thread;
    }

    private Peer mockPeer(String peerId)
    {
        Peer peer = mock(Peer.class, peerId);
        when(peer.getId()).thenReturn(peerId);
        return peer;
    }

    private InputStream buildAuthMessage() throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        MsgIntAuth.newBuilder()
            .setNodeUuid(ProtoUuidUtils.serialize(NODE_UUID))
            .setNodeName(NODE_NAME)
            .setCtrlUuid(ProtoUuidUtils.serialize(CTRL_UUID))
            .build()
            .writeDelimitedTo(baos);
        return new ByteArrayInputStream(baos.toByteArray());
    }

    private FullSync.FullSyncStatus statusOf(FullSync.FullSyncResult result) throws Exception
    {
        Field statusField = FullSync.FullSyncResult.class.getDeclaredField("status");
        statusField.setAccessible(true);
        return (FullSync.FullSyncStatus) statusField.get(result);
    }

    /**
     * Minimal stand-in for ControllerPeerConnectorImpl mirroring its (intended) connection handling: the last
     * authenticated connection becomes the active controller peer and a previously active, different connection
     * gets closed.
     */
    private static final class TrackingControllerPeerConnector implements ControllerPeerConnector
    {
        private @Nullable Peer controllerPeer = null;

        @Override
        public synchronized void setControllerPeer(
            @Nullable UUID ctrlUuidRef,
            Peer controllerPeerRef,
            UUID nodeUuidRef,
            String nodeNameRef
        )
        {
            if (controllerPeer != null && controllerPeer != controllerPeerRef)
            {
                controllerPeer.closeConnection();
            }
            controllerPeer = controllerPeerRef;
        }

        @Override
        public synchronized @Nullable Peer getControllerPeer()
        {
            return controllerPeer;
        }

        @Override
        public @Nullable Node getLocalNode()
        {
            return null;
        }

        @Override
        public @Nullable NodeName getLocalNodeName()
        {
            return null;
        }

        @Override
        public void setControllerPeerToCurrentLocalNode()
        {
            // no-op
        }
    }
}
