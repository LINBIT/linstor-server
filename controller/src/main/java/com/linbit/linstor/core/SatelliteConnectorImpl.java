package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.LinStorRuntimeException;
import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.NetInterface.EncryptionType;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.NetComContainer;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.TcpConnector;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
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
    private final ReadOnlyProps ctrlConf;
    private final NetComContainer netComContainer;
    private final ReconnectorTask reconnectorTask;

    @Inject
    public SatelliteConnectorImpl(
        ErrorReporter errorReporterRef,
        @Named(LinStor.CONTROLLER_PROPS) Props ctrlConfRef,
        NetComContainer netComContainerRef,
        ReconnectorTask reconnectorTaskRef
    )
    {
        errorReporter = errorReporterRef;
        ctrlConf = ctrlConfRef;
        netComContainer = netComContainerRef;
        reconnectorTask = reconnectorTaskRef;
    }

    @Override
    public void startConnecting(Node node, AccessContext accCtx)
    {
        startConnecting(node, accCtx, true);
    }

    public void startConnecting(Node node, AccessContext accCtx, boolean async)
    {
        try
        {
            Node.Type nodeType = node.getNodeType(accCtx);
            if (
                nodeType.equals(Node.Type.SATELLITE) ||
                    nodeType.equals(Node.Type.COMBINED) ||
                    nodeType.isSpecial()
            )
            {
                NetInterface activeStltConn = node.getActiveStltConn(accCtx);
                if (activeStltConn != null)
                {
                    EncryptionType type = activeStltConn.getStltConnEncryptionType(accCtx);
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
                        if (async)
                        {
                            connectSatelliteAsync(
                                new InetSocketAddress(
                                    activeStltConn.getAddress(accCtx).getAddress(),
                                    activeStltConn.getStltConnPort(accCtx).value
                                ),
                                tcpConnector,
                                node
                            );
                        }
                        else
                        {
                            connectSatellite(
                                new InetSocketAddress(
                                    activeStltConn.getAddress(accCtx).getAddress(),
                                    activeStltConn.getStltConnPort(accCtx).value
                                ),
                                tcpConnector,
                                node,
                                false
                            );
                        }
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
                node.setOfflinePeer(errorReporter, accCtx);
                errorReporter.logDebug(
                    "Not connecting to " + nodeType.name() + " node: '" + node.getName().getDisplayName() + "'"
                );
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

    public void connectSatelliteAsync(
        final InetSocketAddress satelliteAddress,
        final TcpConnector tcpConnector,
        final Node node
    )
    {
        Runnable connectRunnable = () -> connectSatellite(satelliteAddress, tcpConnector, node, true);
        // This could possibly be offloaded to some specialized worker pool in the future,
        // but not to the main worker pool used for submitting inbound requests,
        // because submitting to the main worker pool from the Controller's initialization
        // routines or from another task that already runs on the main worker pool
        // can potentially deadlock if the worker pool's queue is full
        connectRunnable.run();
    }

    public void connectSatellite(
        final InetSocketAddress satelliteAddress,
        final TcpConnector tcpConnector,
        final Node node,
        final boolean addToReconnector
    )
    {
        try
        {
            Peer peer = tcpConnector.connect(satelliteAddress, node);
            if (addToReconnector)
            {
                reconnectorTask.add(peer, false);
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
                    "IOException occurred. See cause for further details",
                    null,
                    null,
                    ioExc
                )
            );
        }
    }
}
