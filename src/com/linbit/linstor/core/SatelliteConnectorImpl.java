package com.linbit.linstor.core;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.Node;
import com.linbit.linstor.annotation.SatelliteConnectorContext;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.TcpConnector;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.tasks.PingTask;
import com.linbit.linstor.tasks.ReconnectorTask;

import java.io.IOException;
import java.net.InetSocketAddress;

@Singleton
public class SatelliteConnectorImpl implements SatelliteConnector
{
    private final ErrorReporter errorReporter;
    private final AccessContext connectorCtx;
    private final CtrlAuthenticationApiCallHandler authApiCallHandler;
    private final PingTask pingTask;
    private final ReconnectorTask reconnectorTask;

    @Inject
    public SatelliteConnectorImpl(
        ErrorReporter errorReporterRef,
        @SatelliteConnectorContext AccessContext connectorCtxRef,
        CtrlAuthenticationApiCallHandler authApiCallHandlerRef,
        PingTask pingTaskRef,
        ReconnectorTask reconnectorTaskRef
    )
    {
        errorReporter = errorReporterRef;
        connectorCtx = connectorCtxRef;
        authApiCallHandler = authApiCallHandlerRef;
        pingTask = pingTaskRef;
        reconnectorTask = reconnectorTaskRef;
    }

    @Override
    public void connectSatellite(
        final InetSocketAddress satelliteAddress,
        final TcpConnector tcpConnector,
        final Node node
    )
    {
        Runnable connectRunnable = new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    Peer peer = tcpConnector.connect(satelliteAddress, node);
                    {
                        node.setPeer(connectorCtx, peer);
                    }
                    if (peer.isConnected(false))
                    {
                        // no locks needed
                        authApiCallHandler.completeAuthentication(peer);
                        pingTask.add(peer);
                    }
                    else
                    {
                        reconnectorTask.add(peer);
                    }
                }
                catch (IOException ioExc)
                {
                    errorReporter.reportError(
                        new LinStorException(
                            "Cannot connect to satellite",
                            String.format(
                                "Establishing connection to satellite (%s:%d) failed",
                                satelliteAddress.getAddress().getHostAddress(),
                                satelliteAddress.getPort()
                            ),
                            "IOException occured. See cause for further details",
                            null,
                            null,
                            ioExc
                        )
                    );
                }
                catch (AccessDeniedException accDeniedExc)
                {
                    errorReporter.reportError(
                        new ImplementationError(
                            "System context has not enough privileges to set peer for a connecting node",
                            accDeniedExc
                        )
                    );
                    accDeniedExc.printStackTrace();
                }
            }
        };
        // This could possibly be offloaded to some specialized worker pool in the future,
        // but not to the main worker pool used for submitting inbound requests,
        // because submitting to the main worker pool from the Controller's initialization
        // routines or from another task that already runs on the main worker pool
        // can potentially deadlock if the worker pool's queue is full
        connectRunnable.run();
    }

}
