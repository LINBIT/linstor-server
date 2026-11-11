package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.netcom.ApiCallNoResponseException;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.PeerClosingConnectionException;
import com.linbit.linstor.netcom.PeerNotConnectedException;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.testutils.EmptyErrorReporter;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Bounds on controller -> satellite fan-out calls: reachability, per-node timeout, no data on errors. */
public class SatelliteCallGuardTest
{
    private ReadOnlyProps ctrlProps;
    private SatelliteCallGuard guard;
    private Node node;

    @Before
    public void setUp() throws Exception
    {
        ctrlProps = mock(ReadOnlyProps.class);
        SystemConfRepository repo = mock(SystemConfRepository.class);
        when(repo.getCtrlConfForView()).thenReturn(ctrlProps);
        guard = new SatelliteCallGuard(repo, new EmptyErrorReporter());
        node = mock(Node.class);
        when(node.getName()).thenReturn(new NodeName("stlt-1"));
    }

    @Test
    public void skippableErrors()
    {
        assertTrue(SatelliteCallGuard.isSkippable(new PeerNotConnectedException()));
        assertTrue(SatelliteCallGuard.isSkippable(new PeerClosingConnectionException()));
        assertTrue(SatelliteCallGuard.isSkippable(new ApiCallNoResponseException()));
        assertTrue(SatelliteCallGuard.isSkippable(new TimeoutException()));
        assertFalse(SatelliteCallGuard.isSkippable(new IllegalStateException()));
    }

    @Test
    public void timeoutDefaultAndOverride() throws Exception
    {
        assertEquals(Duration.ofSeconds(SatelliteCallGuard.DFLT_TIMEOUT_SEC), guard.getTimeout());

        when(ctrlProps.getProp(SatelliteCallGuard.KEY_TIMEOUT_SEC, SatelliteCallGuard.NAMESPC)).thenReturn("7");
        assertEquals(Duration.ofSeconds(7), guard.getTimeout());

        when(ctrlProps.getProp(SatelliteCallGuard.KEY_TIMEOUT_SEC, SatelliteCallGuard.NAMESPC)).thenReturn("-1");
        assertEquals(Duration.ofSeconds(SatelliteCallGuard.DFLT_TIMEOUT_SEC), guard.getTimeout());

        when(ctrlProps.getProp(SatelliteCallGuard.KEY_TIMEOUT_SEC, SatelliteCallGuard.NAMESPC)).thenReturn(null);
        when(ctrlProps.getProp(SatelliteCallGuard.AUX_KEY_TIMEOUT_SEC)).thenReturn("3");
        assertEquals(Duration.ofSeconds(3), guard.getTimeout());

        when(ctrlProps.getProp(SatelliteCallGuard.AUX_KEY_TIMEOUT_SEC)).thenReturn("nonsense");
        assertEquals(Duration.ofSeconds(SatelliteCallGuard.DFLT_TIMEOUT_SEC), guard.getTimeout());
    }

    @Test
    public void silentSatelliteBecomesEmptyAfterTimeout() throws Exception
    {
        when(ctrlProps.getProp(SatelliteCallGuard.KEY_TIMEOUT_SEC, SatelliteCallGuard.NAMESPC)).thenReturn("1");
        long start = System.nanoTime();
        List<Object> res = guard.guard(node, "test", Flux.never()).collectList().block(Duration.ofSeconds(10));
        long ms = (System.nanoTime() - start) / 1_000_000;
        assertTrue(res != null && res.isEmpty());
        assertTrue("took " + ms + " ms", ms >= 900 && ms < 5000);
    }

    @Test
    public void closingConnectionBecomesEmpty()
    {
        List<Object> res = guard.guard(node, "test", Flux.error(new PeerClosingConnectionException()))
            .collectList().block(Duration.ofSeconds(5));
        assertTrue(res != null && res.isEmpty());
    }

    @Test(expected = IllegalStateException.class)
    public void otherErrorsStillPropagate()
    {
        guard.guard(node, "test", Flux.error(new IllegalStateException("boom"))).blockLast(Duration.ofSeconds(5));
    }

    @Test
    public void answersPassThrough()
    {
        List<Object> res = guard.guard(node, "test", Flux.<Object>just("a", "b")).collectList()
            .block(Duration.ofSeconds(5));
        assertEquals(List.of("a", "b"), res);
    }

    @Test
    public void reachableOnlyWhenPeerOnline()
    {
        Peer peer = mock(Peer.class);
        when(node.getPeer()).thenReturn(peer);
        when(peer.isOnline()).thenReturn(false);
        assertFalse(guard.isReachable(node));
        when(peer.isOnline()).thenReturn(true);
        assertTrue(guard.isReachable(node));
        when(node.getPeer()).thenReturn(null);
        assertFalse(guard.isReachable(node));
    }
}
