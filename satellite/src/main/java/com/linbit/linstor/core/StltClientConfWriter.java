package com.linbit.linstor.core;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.cfg.StltConfig;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

/**
 * Publishes the address of the controller that is connected to this satellite in the configuration format of
 * the LINSTOR client.
 *
 * The client falls back to this file if the administrator never created a linstor-client.conf, which saves the
 * manual configuration step on every node that runs a satellite.
 *
 * The file is a hint, not a liveness indicator: it is deliberately kept when the controller disconnects, so that
 * the client still reports "cannot connect to &lt;controller&gt;" instead of silently falling back to localhost.
 * Since the default location is below /run, it is discarded on reboot anyway.
 *
 * The target directory is not created here, its lifetime belongs to whoever starts the satellite (systemd's
 * RuntimeDirectory for the shipped unit). Nothing is published if that directory is missing.
 */
@Singleton
public class StltClientConfWriter
{
    private static final Set<PosixFilePermission> FILE_PERMISSIONS = PosixFilePermissions.fromString("rw-r--r--");

    private final ErrorReporter errorReporter;
    private final StltConfig stltCfg;

    private @Nullable String lastWrittenContent;

    @Inject
    public StltClientConfWriter(ErrorReporter errorReporterRef, StltConfig stltCfgRef)
    {
        errorReporter = errorReporterRef;
        stltCfg = stltCfgRef;
    }

    /**
     * Rewrites the client configuration if the resulting content differs from what was written last.
     * Never throws; a satellite must stay operational even if it cannot write this hint.
     */
    public synchronized void update(Peer ctrlPeerRef)
    {
        @Nullable Path targetPath = getTargetPath();
        @Nullable InetAddress ctrlAddr = getCtrlAddress(ctrlPeerRef);
        if (targetPath != null && ctrlAddr != null)
        {
            if (ctrlAddr.isLinkLocalAddress())
            {
                // a zone-qualified link-local address is of no use to a client running in a different context
                errorReporter.logDebug(
                    "Not publishing the controller address %s for the LINSTOR client, it is link-local",
                    ctrlAddr.getHostAddress()
                );
            }
            else
            {
                String content = buildContent(ctrlAddr);
                if (!content.equals(lastWrittenContent))
                {
                    try
                    {
                        writeAtomically(targetPath, content);
                        lastWrittenContent = content;
                        errorReporter.logInfo(
                            "Published the controller address %s for the LINSTOR client in %s",
                            ctrlAddr.getHostAddress(),
                            targetPath
                        );
                    }
                    catch (IOException | UnsupportedOperationException exc)
                    {
                        lastWrittenContent = null;
                        errorReporter.logWarning(
                            "Failed to write the controller address for the LINSTOR client to %s: %s",
                            targetPath,
                            exc.getMessage()
                        );
                    }
                }
            }
        }
    }

    private @Nullable Path getTargetPath()
    {
        @Nullable Path targetPath = null;
        @Nullable String configuredPath = stltCfg.getClientConfFile();
        if (configuredPath != null && !configuredPath.isBlank())
        {
            Path candidate = Paths.get(configuredPath.trim()).toAbsolutePath();
            if (Files.isDirectory(candidate.getParent()))
            {
                targetPath = candidate;
            }
            else
            {
                errorReporter.logDebug(
                    "Not publishing the controller address for the LINSTOR client, %s does not exist",
                    candidate.getParent()
                );
            }
        }
        return targetPath;
    }

    private @Nullable InetAddress getCtrlAddress(Peer ctrlPeerRef)
    {
        // the port of the peer address is the controller's ephemeral source port, only the address is of interest
        @Nullable InetSocketAddress peerAddr = ctrlPeerRef.getHostAddr();
        return peerAddr == null ? null : peerAddr.getAddress();
    }

    private String buildContent(InetAddress ctrlAddrRef)
    {
        return String.join(
            System.lineSeparator(),
            "# Generated by the LINSTOR satellite, do not edit.",
            "# Address of the controller connected to this node, used by the LINSTOR client",
            "# only as long as no linstor-client.conf exists.",
            "[global]",
            "controllers = " + buildControllerUrl(ctrlAddrRef),
            ""
        );
    }

    /**
     * Only the plain endpoint is published, and without a port so the client applies its own default: the
     * satellite learns neither the port the controller's REST API listens on nor whether it serves https.
     */
    private String buildControllerUrl(InetAddress ctrlAddrRef)
    {
        String host = ctrlAddrRef.getHostAddress();
        if (ctrlAddrRef instanceof Inet6Address)
        {
            host = "[" + host + "]";
        }
        return "linstor://" + host;
    }

    private void writeAtomically(Path targetPathRef, String contentRef) throws IOException
    {
        Path tmpPath = Files.createTempFile(
            targetPathRef.getParent(),
            targetPathRef.getFileName().toString(),
            ".tmp"
        );
        try
        {
            Files.writeString(tmpPath, contentRef, StandardCharsets.UTF_8);
            if (targetPathRef.getFileSystem().supportedFileAttributeViews().contains("posix"))
            {
                // the client usually runs unprivileged, it has to be able to read this
                Files.setPosixFilePermissions(tmpPath, FILE_PERMISSIONS);
            }
            Files.move(
                tmpPath,
                targetPathRef,
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE
            );
        }
        catch (IOException | UnsupportedOperationException exc)
        {
            Files.deleteIfExists(tmpPath);
            throw exc;
        }
    }
}
