package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.LinStorRuntimeException;
import com.linbit.linstor.NetInterface;
import com.linbit.linstor.NetInterface.EncryptionType;
import com.linbit.linstor.Node.NodeType;
import com.linbit.linstor.Node;
import com.linbit.linstor.annotation.SatelliteConnectorContext;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.NetComContainer;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.TcpConnector;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.tasks.PingTask;
import com.linbit.linstor.tasks.ReconnectorTask;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.net.InetSocketAddress;

@Singleton
public class SatelliteConnectorImpl implements SatelliteConnector
{
    private final ErrorReporter errorReporter;
    private final AccessContext connectorCtx;
    private final Props ctrlConf;
    private final NetComContainer netComContainer;
    private final CtrlAuthenticator authenticator;
    private final PingTask pingTask;
    private final ReconnectorTask reconnectorTask;

    @Inject
    public SatelliteConnectorImpl(
        ErrorReporter errorReporterRef,
        @SatelliteConnectorContext AccessContext connectorCtxRef,
        @Named(LinStor.CONTROLLER_PROPS) Props ctrlConfRef,
        NetComContainer netComContainerRef,
        CtrlAuthenticator authenticatorRef,
        PingTask pingTaskRef,
        ReconnectorTask reconnectorTaskRef
    )
    {
        errorReporter = errorReporterRef;
        connectorCtx = connectorCtxRef;
        ctrlConf = ctrlConfRef;
        netComContainer = netComContainerRef;
        authenticator = authenticatorRef;
        pingTask = pingTaskRef;
        reconnectorTask = reconnectorTaskRef;
    }

    @Override
    public void startConnecting(Node node, AccessContext accCtx)
    {
        try
        {
            NodeType nodeType = node.getNodeType(accCtx);
            if (
                nodeType.equals(NodeType.SATELLITE) ||
                nodeType.equals(NodeType.COMBINED) ||
                nodeType.equals(NodeType.SWORDFISH_TARGET)
            )
            {
                NetInterface stltConn = node.getSatelliteConnection(accCtx);
                if (stltConn != null)
                {
                    EncryptionType type = stltConn.getStltConnEncryptionType(accCtx);
                    String serviceType;
                    switch (type)
                    {
                        case PLAIN:
                            serviceType = ControllerNetComInitializer.PROPSCON_KEY_DEFAULT_PLAIN_CON_SVC;
                            break;
                        case SSL:
                            serviceType = ControllerNetComInitializer.PROPSCON_KEY_DEFAULT_SSL_CON_SVC;
                            break;
                        default:
                            throw new ImplementationError(
                                "Unhandled default case for EncryptionType",
                                null
                            );
                    }
                    ServiceName dfltConSvcName;
                    try
                    {
                        dfltConSvcName = new ServiceName(
                            ctrlConf.getProp(serviceType)
                        );
                    }
                    catch (InvalidNameException invalidNameExc)
                    {
                        throw new LinStorRuntimeException(
                            "The ServiceName of the default TCP connector is not valid",
                            invalidNameExc
                        );
                    }
                    TcpConnector tcpConnector = netComContainer.getNetComConnector(dfltConSvcName);

                    if (tcpConnector != null)
                    {
                        connectSatellite(
                            new InetSocketAddress(
                                stltConn.getAddress(accCtx).getAddress(),
                                stltConn.getStltConnPort(accCtx).value
                            ),
                            tcpConnector,
                            node
                        );
                    }
                    else
                    {
                        throw new LinStorRuntimeException(
                            "Attempt to establish a " + type + " connection without a proper connector defined"
                        );
                    }
                }
            }
            else
            {
                errorReporter.logDebug("Not connecting to " + nodeType.name() + " node: '" + node.getName().displayValue + "'");
            }
        }
        catch (AccessDeniedException | InvalidKeyException exc)
        {
            throw new LinStorRuntimeException(
                "Access to an object protected by access controls was revoked while a " +
                    "controller<->satellite connect operation was in progress.",
                exc
            );
        }
    }

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
                        authenticator.completeAuthentication(peer);
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
