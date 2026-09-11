package com.linbit.linstor.api;

import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcher;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlAuthResponseApiCallHandler;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.common.StltConfigOuterClass;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;

import jakarta.inject.Inject;
import jakarta.inject.Provider;

import java.util.Collections;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.inject.testing.fieldbinder.Bind;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import reactor.util.context.Context;

/**
 * Tests that the controller only reacts to authentication responses arriving for the node's CURRENT connection.
 *
 * <p>
 * During a reconnect the controller can have sent an Auth over a connection that has been replaced in the
 * meantime (double reconnect). The satellite's answer to such a superseded Auth carries an expectedFullSyncId
 * that will never be valid again (the newer Auth already invalidated it). If the controller processes such a
 * stale response, it authenticates the wrong peer object and sends a FullSync that the satellite is guaranteed
 * to refuse as outdated.
 * </p>
 */
public class CtrlAuthResponseTest extends ApiTestBase
{
    private static final long EXPECTED_FULL_SYNC_ID = 3L;

    @Inject
    private Provider<CtrlAuthResponseApiCallHandler> authResponseHandlerProvider;

    @Bind
    @Mock
    protected FreeCapacityFetcher freeCapacityFetcher;

    @Mock
    protected Peer currentSatellitePeer;
    @Mock
    protected Peer staleSatellitePeer;

    private NodeName testNodeName;
    private Node testNode;

    private boolean inScope = false;

    public CtrlAuthResponseTest() throws Exception
    {
        super();
        testNodeName = new NodeName("TestSatellite");
    }

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        testNode = nodeFactory.create(
            testNodeName,
            Node.Type.SATELLITE,
            null
        );
        testNode.setPeer(currentSatellitePeer);
        nodesMap.put(testNodeName, testNode);
        commitAndCleanUp(true);
        inScope = false;

        for (Peer peer : new Peer[]{currentSatellitePeer, staleSatellitePeer})
        {
            Mockito.when(peer.getNode()).thenReturn(testNode);
            Mockito.when(peer.getExtToolsManager()).thenReturn(Mockito.mock(ExtToolsManager.class));
            Mockito.when(peer.getSerializerLock()).thenReturn(new ReentrantReadWriteLock(true));
        }
    }

    @Override
    public void tearDown() throws Exception
    {
        commitAndCleanUp(inScope);
    }

    /**
     * An auth response arriving for a connection that is no longer the node's current connection must be
     * ignored completely: neither may the (dead) connection be marked as authenticated, nor may a FullSync
     * (based on an already invalidated expectedFullSyncId) be triggered.
     */
    @Test
    public void authResponseOnStaleConnectionMustBeIgnored() throws Exception
    {
        executeAuthResponse(staleSatellitePeer);

        Mockito.verify(staleSatellitePeer, Mockito.never()).setAuthenticated(Mockito.anyBoolean());
        Mockito.verify(staleSatellitePeer, Mockito.never()).setConnectionStatus(Mockito.any());
        // no FullSync may be triggered by the stale auth response - neither over the stale nor over the
        // current connection
        Mockito.verify(staleSatellitePeer, Mockito.never()).setFullSyncId(Mockito.anyLong());
        Mockito.verify(staleSatellitePeer, Mockito.never()).sendMessage(Mockito.any(byte[].class));
        Mockito.verify(currentSatellitePeer, Mockito.never()).setFullSyncId(Mockito.anyLong());
        Mockito.verify(currentSatellitePeer, Mockito.never()).sendMessage(Mockito.any(byte[].class));
    }

    /**
     * Regression guard: an auth response arriving on the node's current connection must still authenticate
     * the peer and trigger the FullSync.
     */
    @Test
    public void authResponseOnCurrentConnectionAuthenticates() throws Exception
    {
        executeAuthResponse(currentSatellitePeer);

        Mockito.verify(currentSatellitePeer).setAuthenticated(true);
        Mockito.verify(currentSatellitePeer).setFullSyncId(EXPECTED_FULL_SYNC_ID);
        Mockito.verify(currentSatellitePeer, Mockito.atLeastOnce()).sendMessage(Mockito.any(byte[].class));
    }

    private void executeAuthResponse(Peer answeringPeer)
    {
        int[] version = LinStor.VERSION_INFO_PROVIDER.getSemanticVersion();
        authResponseHandlerProvider.get()
            .authResponse(
                answeringPeer,
                true,
                new ApiCallRcImpl(),
                EXPECTED_FULL_SYNC_ID,
                "testuname",
                ApiConsts.Platform.LINUX,
                "TestOs",
                version[0],
                version[1],
                version[2],
                Collections.emptyList(),
                StltConfigOuterClass.StltConfig.getDefaultInstance(),
                Collections.emptyList(),
                false
            )
            .collectList()
            .contextWrite(Context.of(ApiModule.API_CALL_NAME, "test"))
            .block();
    }
}
