package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorRuntimeException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.CoreModule.NodesMap;
import com.linbit.linstor.core.apicallhandler.response.ApiException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.core.cfg.LinstorConfig;
import com.linbit.linstor.core.cfg.TomlConfigLoader;
import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.utils.FileUtils;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Singleton
public class SpecialSatelliteProcessManager
{
    private static final String LOCALHOST = "localhost";
    private static final String SATELLITE_LOG_DIRECTORY = "specialTargets";

    private static final int WAIT_FOR_START_RETRY_DELAY_MS = 100;
    private static final int WAIT_FOR_START_RETRY_COUNT = 1000; // 10 sec max

    private final NodesMap nodesMap;
    private final ErrorReporter errorReporter;
    private final CtrlConfig ctrlConf;

    private final transient Map<String, Process> childSatellites;
    private final transient Map<String, Integer> childPorts;

    @Inject
    public SpecialSatelliteProcessManager(
        CoreModule.NodesMap nodesMapRef,
        ErrorReporter errorReporterRef,
        CtrlConfig ctrlConfRef
    )
    {
        nodesMap = nodesMapRef;
        errorReporter = errorReporterRef;
        ctrlConf = ctrlConfRef;
        childSatellites = new HashMap<>();
        childPorts = new HashMap<>();
    }

    public void initialize()
    {
        nodesMap.values().stream()
            .filter(node -> getNodeType(node).isSpecial())
            .forEach(this::sneakyStart);
    }

    private Node.Type getNodeType(Node node)
    {
        Node.Type type;
        type = node.getNodeType();
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
            errorReporter.reportError(
                exc,
                null,
                "Could not start necessary special target node due to IO error"
            );
        }
        catch (PortAlreadyInUseException exc)
        {
            errorReporter.logWarning(
                "Could not start special target '%s' as the required port %d is already in use.\n" +
                    "Please change the port of the given node",
                node.getName().displayValue,
                exc.getPort()
            );
        }
    }

    private @Nullable Path getSatellitePath()
    {
        URL uLoc = getClass().getProtectionDomain().getCodeSource().getLocation();
        Path satellitePath = null;
        try
        {
            Path sourcePath = Paths.get(uLoc.toURI());
            if (sourcePath.toString().endsWith(".jar"))
            {
                Path parent = sourcePath.getParent();
                if (parent != null)
                {
                    satellitePath = parent.resolve("../bin/Satellite").normalize();
                }
                else
                {
                    errorReporter.logWarning("source path %s has no parent.", sourcePath);
                }
            }
            else
            {
                satellitePath = sourcePath.resolve("Satellite").normalize();
            }
        }
        catch (URISyntaxException ignored)
        {
            // ignored
        }

        return satellitePath;
    }

    public void startLocalSatelliteProcess(Node node) throws IOException, PortAlreadyInUseException
    {
        Iterator<NetInterface> netIfIt;
        netIfIt = node.iterateNetInterfaces();
        Integer port = null;
        while (netIfIt.hasNext())
        {
            NetInterface netIf = netIfIt.next();
            if (port != null)
            {
                throw new ImplementationError("Special target node has more than one network interface!");
            }
            port = netIf.getStltConnPort().value;
        }
        if (port == null)
        {
            throw new ImplementationError("Special target node has no network interfaces");
        }

        startLocalSatelliteProcess(node.getName().displayValue, port, node.getNodeType());
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
     */
    public void startLocalSatelliteProcess(String nodeNameStr, Integer port, Node.Type nodeType)
        throws IOException, PortAlreadyInUseException
    {
        Path specialSatLogDir = errorReporter.getLogDirectory().resolve(SATELLITE_LOG_DIRECTORY);
        Path specialErrLogDir = specialSatLogDir.resolve(String.format("satellite-%d-logs", port));

        String option;
        switch (nodeType)
        {
            case REMOTE_SPDK:
                option = "--remote-spdk";
                break;
            case EBS_TARGET:
                option = "--ebs";
                break;

            case AUXILIARY: // fall-through
            case COMBINED:// fall-through
            case CONTROLLER:// fall-through
            case SATELLITE:
            default:
                throw new ImplementationError(nodeType + " is not a special type. Use regular node create API");
        }

        Path confPath = getSpecStltConfPath(port);
        if (!Files.exists(confPath))
        {
            Files.createDirectories(confPath);
            Path parent = confPath.getParent();
            if (parent != null)
            {
                Path dfltSpecialStltConfPath = parent.resolve(LinstorConfig.LINSTOR_STLT_CONFIG);
                if (Files.exists(dfltSpecialStltConfPath))
                {
                    // copy /etc/linstor/linstor_satellite.toml (if exists) to
                    // /etc/linstor/specialStlt_<port>/linstor_satellite.toml
                    Files.copy(dfltSpecialStltConfPath, confPath.resolve(LinstorConfig.LINSTOR_STLT_CONFIG));
                }
            }
            else
            {
                errorReporter.logWarning("conf path %s has no parent.", confPath);
            }
        }

        List<String> cmd = new ArrayList<>(
            Arrays.asList(
                getSatellitePath().toString(),
                "-s",
                "--port", Integer.toString(port),
                "--bind-address", LOCALHOST,
                "-d",
                "--override-node-name", nodeNameStr,
                "--logs",
                specialErrLogDir.toAbsolutePath().toString(),
                "-c", confPath.toString(),
                option
            )
        );
        Path stltIncludeDir = getStltIncludeDir();
        if (stltIncludeDir != null)
        {
            // the copied config file is read from confPath, where a relative includeDir would not resolve
            cmd.add("--include-directory");
            cmd.add(stltIncludeDir.toString());
        }

        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);
        File stltLog = errorReporter.getLogDirectory().resolve(
            String.format(SATELLITE_LOG_DIRECTORY + "/satellite-%d.log", port)
        ).toFile();
        stltLog.getParentFile().mkdirs(); // just to be sure
        pb.redirectOutput(stltLog);

        ensurePortisAvailable(port);

        Process proc = pb.start();

        // create node usually waits for the first connection attempt - we should wait here a bit
        // so the just started satellite starts and can be connected

        waitForStart(port);

        childSatellites.put(nodeNameStr.toUpperCase(), proc);
        childPorts.put(nodeNameStr.toUpperCase(), port);
    }

    /**
     * The include directory of the default satellite configuration, so that a special satellite picks up the same
     * drop-in files as a regular satellite on this node. Null if there are none.
     *
     * <p>
     * Only what the satellite configuration file itself declares can be found this way. An --include-directory or
     * LS_INCLUDE_DIRECTORY on the regular satellite's unit is invisible here, so a node configured that way gets a
     * special satellite without those drop-ins.
     * </p>
     */
    private @Nullable Path getStltIncludeDir()
    {
        Path dfltStltConfPath = Paths.get(ctrlConf.getConfigDir(), LinstorConfig.LINSTOR_STLT_CONFIG);
        Path includeDir = null;
        try
        {
            Path effectiveIncludeDir = TomlConfigLoader.effectiveIncludeDir(
                dfltStltConfPath,
                LinstorConfig.LINSTOR_STLT_INCLUDE_DIR
            );
            if (Files.isDirectory(effectiveIncludeDir))
            {
                includeDir = effectiveIncludeDir;
            }
        }
        catch (LinStorRuntimeException exc)
        {
            errorReporter.logWarning(
                "Could not determine the include directory of '%s': %s",
                dfltStltConfPath.toString(),
                exc.getMessage()
            );
        }
        return includeDir;
    }

    private Path getSpecStltConfPath(@Nullable Integer port)
    {
        return Paths.get(ctrlConf.getConfigDir(), "specialStlt_" + port);
    }

    private void waitForStart(Integer portRef)
    {
        boolean started = false;
        try
        {
            for (int attempt = 0; attempt < WAIT_FOR_START_RETRY_COUNT; attempt++)
            {
                try
                {
                    new Socket(LOCALHOST, portRef).close();
                    started = true;
                    break;
                }
                catch (IOException exc)
                {
                    Thread.sleep(WAIT_FOR_START_RETRY_DELAY_MS);
                }
            }
        }
        catch (InterruptedException exc)
        {
            throw new ApiException(exc);
        }
        if (!started)
        {
             throw new ApiRcException(
                 ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_LINSTOR_MANAGED_SATELLITE_DID_NOT_START_PROPERLY,
                     "The started satellite which should represent the special storage device did not start properly"
                )
            );
        }
    }

    public @Nullable Integer stopProcess(Node node)
    {
        final String nodeName = node.getName().value;
        final Process process = childSatellites.get(nodeName);
        @Nullable Integer port = childPorts.get(nodeName);
        final Path specStltConfPath = getSpecStltConfPath(port);
        if (process == null)
        {
            errorReporter.logWarning("No special target satellite process found");
        }
        else
        {
            errorReporter.logTrace("Stopping special target process...");
            process.destroy();
            try
            {
                process.waitFor(2, TimeUnit.SECONDS);
            }
            catch (InterruptedException ignored)
            {
                // ignored
            }
            if (process.isAlive())
            {
                errorReporter.logTrace("Killing special target process...");
                process.destroyForcibly();
            }
        }

        if (Files.exists(specStltConfPath))
        {
            try
            {
                FileUtils.deleteDirectoryWithContent(specStltConfPath, errorReporter);
            }
            catch (IOException exc)
            {
                errorReporter.reportError(exc);
            }
        }

        childSatellites.remove(nodeName);
        childPorts.remove(nodeName);
        return port;
    }

    private void ensurePortisAvailable(Integer port) throws PortAlreadyInUseException
    {
        ServerSocketChannel ssc = null;
        Selector selector = null;
        PortAlreadyInUseException portAlreadyInUseExc = null;
        try
        {
            SocketAddress bindAddress = new InetSocketAddress(LOCALHOST, port);
            ssc = ServerSocketChannel.open();
            ssc.bind(bindAddress);
            ssc.configureBlocking(false);
            selector = Selector.open();
            ssc.register(selector, SelectionKey.OP_ACCEPT);
        }
        catch (IOException | RuntimeException exc)
        {
            /*
             * Do not simply throw the exception as otherwise an additional thrown exception
             * in the finally block would mask this original exception
             */
            portAlreadyInUseExc = new PortAlreadyInUseException(port);
        }
        finally
        {
            closeOrThrow(selector, "selector", portAlreadyInUseExc);
            closeOrThrow(ssc, "server socket", portAlreadyInUseExc);
        }
        if (portAlreadyInUseExc != null)
        {
            throw portAlreadyInUseExc;
        }
    }

    private void closeOrThrow(
        @Nullable Closeable closeable,
        String description,
        @Nullable PortAlreadyInUseException portAlreadyInUseExc
    )
    {
        if (closeable != null)
        {
            try
            {
                closeable.close();
            }
            catch (IOException exc)
            {
                throw new LinStorRuntimeException(
                    "Failed to close testing " + description,
                    exc
                ).addSuppressedThrowables(portAlreadyInUseExc);
            }
        }
    }
}
