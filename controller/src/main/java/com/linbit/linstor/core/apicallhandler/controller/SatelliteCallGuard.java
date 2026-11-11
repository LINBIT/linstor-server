package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.ApiCallNoResponseException;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.PeerClosingConnectionException;
import com.linbit.linstor.netcom.PeerNotConnectedException;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

import reactor.core.publisher.Flux;

/**
 * Bounds controller -> satellite fan-out calls (thin free space, allocated sizes, error reports).
 *
 * <ul>
 *     <li>{@link #isReachable(Node)}: only ask satellites whose peer is online (connected, authenticated, full sync
 *     applied). OFFLINE / reconnecting nodes are skipped instead of being asked.</li>
 *     <li>{@link #guard(Node, String, Flux)}: a per-node timeout; a node that does not answer in time, or whose
 *     connection drops / closes while the call is open, becomes "no data" (empty) with a warning, exactly as a
 *     disconnected node already was (PeerNotConnectedException -> empty).</li>
 * </ul>
 *
 * Timeout: controller property {@value #NAMESPC}/{@value #KEY_TIMEOUT_SEC} (seconds), default
 * {@value #DFLT_TIMEOUT_SEC}. Until the key is whitelisted in linstor-common it can also be set as
 * {@value #AUX_KEY_TIMEOUT_SEC} (Aux props are not whitelisted). Values &lt;= 0 or unparsable fall back to the default.
 */
@Singleton
public class SatelliteCallGuard
{
    public static final String NAMESPC = "Ctrl";
    public static final String KEY_TIMEOUT_SEC = "SatelliteCallTimeoutSec";
    public static final String AUX_KEY_TIMEOUT_SEC = "Aux/SatelliteCallTimeoutSec";
    public static final long DFLT_TIMEOUT_SEC = 30;

    private final SystemConfRepository systemConfRepository;
    private final ErrorReporter errorReporter;

    @Inject
    public SatelliteCallGuard(SystemConfRepository systemConfRepositoryRef, ErrorReporter errorReporterRef)
    {
        systemConfRepository = systemConfRepositoryRef;
        errorReporter = errorReporterRef;
    }

    public boolean isReachable(Node node)
    {
        @Nullable Peer peer = node.getPeer();
        return peer != null && peer.isOnline();
    }

    public <T> Flux<T> guard(Node node, String what, Flux<T> call)
    {
        final String nodeName = node.getName().displayValue;
        final Duration timeout = getTimeout();
        return call
            .timeout(timeout)
            .onErrorResume(
                SatelliteCallGuard::isSkippable,
                exc ->
                {
                    if (!(exc instanceof PeerNotConnectedException))
                    {
                        errorReporter.logWarning(
                            "%s: no answer from satellite '%s' (%s), treated as no data",
                            what,
                            nodeName,
                            exc instanceof TimeoutException ?
                                "timeout after " + timeout.toSeconds() + " s" :
                                exc.getClass().getSimpleName()
                        );
                    }
                    return Flux.empty();
                }
            );
    }

    static boolean isSkippable(Throwable exc)
    {
        return exc instanceof PeerNotConnectedException ||
            exc instanceof PeerClosingConnectionException ||
            exc instanceof ApiCallNoResponseException ||
            exc instanceof TimeoutException;
    }

    Duration getTimeout()
    {
        long sec = DFLT_TIMEOUT_SEC;
        try
        {
            @Nullable String val = systemConfRepository.getCtrlConfForView().getProp(KEY_TIMEOUT_SEC, NAMESPC);
            if (val == null)
            {
                val = systemConfRepository.getCtrlConfForView().getProp(AUX_KEY_TIMEOUT_SEC);
            }
            if (val != null)
            {
                long parsed = Long.parseLong(val.trim());
                if (parsed > 0)
                {
                    sec = parsed;
                }
            }
        }
        catch (Exception ignored)
        {
            // invalid key / unparsable value / props not available: keep the default
        }
        return Duration.ofSeconds(sec);
    }
}
