package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorRuntimeException;
import com.linbit.linstor.NetInterface;
import com.linbit.linstor.Node;
import com.linbit.linstor.Node.NodeType;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.CoreModule.NodesMap;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.ServerSocketChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Singleton
public class SwordfishTargetProcessManager
{
    private static final String LOCALHOST = "localhost";
    private static final String SATELLITE_OUT_FILE_FORMAT = "sfTargets/satellite-%d.log";

    private final NodesMap nodesMap;
    private final ErrorReporter errorReporter;
    private final AccessContext sysCtx;

    private final transient Map<Node, Process> childSatellites;

    @Inject
    public SwordfishTargetProcessManager(
        CoreModule.NodesMap nodesMapRef,
        ErrorReporter errorReporterRef,
        @SystemContext AccessContext initCtxRef
    )
    {
        nodesMap = nodesMapRef;
        errorReporter = errorReporterRef;
        sysCtx = initCtxRef;
        childSatellites = new HashMap<>();
    }

    public void initialize()
    {
        nodesMap.values().stream()
            .filter(node -> getNodeType(node).equals(NodeType.SWORDFISH_TARGET))
            .forEach(this::sneakyStart);
    }

    private NodeType getNodeType(Node node)
    {
        NodeType type;
        try
        {
            type = node.getNodeType(sysCtx);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(
                "System context has not enough privileges",
                exc
            );
        }
        return type;
    }

    private void sneakyStart(Node node)
    {
        try
        {
            startLocalSatelliteProcess(node);
        }
        catch (IOException exc)
        {
            throw new LinStorRuntimeException(
                "Could not start necessary swordfish target node due to IO error",
                exc
            );
        }
        catch (PortAlreadyInUseException exc)
        {
            errorReporter.logWarning(
                "Could not start swordfish_target '%s' as the required port %d is already in use.\n" +
                    "Please change the port of the given node",
                node.getName().displayValue,
                exc.getPort()
            );
        }
    }

    private Path getSatellitePath()
    {
        URL uLoc = getClass().getProtectionDomain().getCodeSource().getLocation();
        Path satellitePath = null;
        try
        {
            Path sourcePath = Paths.get(uLoc.toURI());
            if (sourcePath.toString().endsWith(".jar"))
            {
                satellitePath = sourcePath.getParent().resolve("../bin/Satellite").normalize();
            }
            else
            {
                satellitePath = sourcePath.resolve("Satellite").normalize();
            }
        }
        catch (URISyntaxException ignore)
        {
        }

        return satellitePath;
    }

    /**
     * <p>
     * Starts a satellite process with parameters
     *  <ul>
     *      <li>-s: skip the initial 'uname -n' check</li>
     *      <li>--port port: listen on given port instead of default port</li>
     *      <li>-d: start with debug console</li>
     *  </ul>
     *  The first two parameters are needed to be able to simply start multiple satellites on
     *  the same machine.
     *
     *  The third parameter will cause the started satellite to start reading from the debug console
     *  (stdIn). This has the convenient side-effect that if the controller shuts down (either
     *  gracefully or forcibly), the stdIn will be close, causing all additionally started satellites
     *  to also shut down (gracefully if possible).
     * </p>
     *
     * @param node
     * @return
     * @throws IOException
     * @throws PortAlreadyInUseException
     */
    public void startLocalSatelliteProcess(Node node) throws IOException, PortAlreadyInUseException
    {
        try
        {
            Iterator<NetInterface> netIfIt = node.iterateNetInterfaces(sysCtx);
            Integer port = null;
            while (netIfIt.hasNext())
            {
                NetInterface netIf = netIfIt.next();
                if (port != null)
                {
                    throw new ImplementationError("Swordfish target node has more than one network interface!");
                }
                port = netIf.getStltConnPort(sysCtx).value;
            }
            if (port == null)
            {
                throw new ImplementationError("Swordfish target node has no network interfaces");
            }

            ProcessBuilder pb = new ProcessBuilder(
                getSatellitePath().toString(),
                "-s",
                "--port",
                Integer.toString(port),
                "--bind-address",
                LOCALHOST,
                "-d"
            );
            pb.redirectErrorStream(true);
            File stltLog = errorReporter.getLogDirectory().resolve(
                String.format(SATELLITE_OUT_FILE_FORMAT, port)
            ).toFile();
            stltLog.getParentFile().mkdirs(); // just to be sure
            pb.redirectOutput(stltLog);

            ensurePortisAvailable(port);

            Process proc = pb.start();

            childSatellites.put(node, proc);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError("System context has not enough privileges", exc);
        }
    }

    public void stopProcess(Node node)
    {
        Process process = childSatellites.get(node);
        if (process == null)
        {
            errorReporter.logWarning("No swordfish-target satellite process found");
        }
        else
        {
            errorReporter.logTrace("Stopping swordfish-target process...");
            process.destroy();
            try
            {
                process.waitFor(2, TimeUnit.SECONDS);
            }
            catch (InterruptedException ignore)
            {
            }
            if (process.isAlive())
            {
                errorReporter.logTrace("Killing swordfish-target process...");
                process.destroyForcibly();
            }
        }
    }

    private void ensurePortisAvailable(Integer port) throws PortAlreadyInUseException
    {
        ServerSocketChannel ssc = null;
        try
        {
            SocketAddress bindAddress = new InetSocketAddress(LOCALHOST, port);
            ssc = ServerSocketChannel.open();
            ssc.bind(bindAddress);

        }
        catch (IOException exc)
        {
            throw new PortAlreadyInUseException(port);
        }
        finally
        {
            if (ssc != null)
            {
                try
                {
                    ssc.close();
                }
                catch (IOException exc)
                {
                    throw new LinStorRuntimeException("Failed to close testing server socket", exc);
                }
            }
        }
    }
}
