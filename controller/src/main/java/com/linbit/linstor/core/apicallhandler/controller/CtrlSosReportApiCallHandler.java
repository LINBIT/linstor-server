package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.extproc.DaemonHandler;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.extproc.ExtCmdFailedException;
import com.linbit.extproc.OutputProxy.EOFEvent;
import com.linbit.extproc.OutputProxy.Event;
import com.linbit.extproc.OutputProxy.ExceptionEvent;
import com.linbit.extproc.OutputProxy.StdErrEvent;
import com.linbit.extproc.OutputProxy.StdOutEvent;
import com.linbit.linstor.SosReportType;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.core.cfg.LinstorConfig;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.PeerNotConnectedException;
import com.linbit.linstor.proto.responses.FileOuterClass;
import com.linbit.linstor.proto.responses.MsgSosReportOuterClass;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;
import com.linbit.utils.FileCollector;
import com.linbit.utils.StringUtils;

import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.inject.Inject;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

@Singleton
public class CtrlSosReportApiCallHandler
{
    private static final int DFLT_DEQUE_CAPACITY = 10;

    private static final String SOS_PREFIX = "sos_";
    private static final String SOS_SUFFIX = ".tar.gz";
    private final DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
    private final DateFormat sdfUtc = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
    private final Provider<AccessContext> peerAccCtx;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final NodeRepository nodeRepository;
    private final CtrlStltSerializer stltComSerializer;
    private final ErrorReporter errorReporter;
    private final ExtCmdFactory extCmdFactory;
    private final CtrlConfig ctrlCfg;
    private final DbEngine db;

    @Inject
    public CtrlSosReportApiCallHandler(
        ErrorReporter errorReporterRef,
        @PeerContext
        Provider<AccessContext> peerAccCtxRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        NodeRepository nodeRepositoryRef,
        CtrlStltSerializer clientComSerializerRef,
        ExtCmdFactory extCmdFactoryRef,
        CtrlConfig ctrlCfgRef,
        DbEngine dbRef
    )
    {
        errorReporter = errorReporterRef;
        peerAccCtx = peerAccCtxRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        nodeRepository = nodeRepositoryRef;
        stltComSerializer = clientComSerializerRef;
        extCmdFactory = extCmdFactoryRef;
        ctrlCfg = ctrlCfgRef;
        db = dbRef;
        sdfUtc.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    public Flux<String> getSosReport(
        Set<String> nodes,
        Date since
    )
    {
        Flux<String> ret;
        try
        {
            final Path tmpDir = Files.createTempDirectory("sos");
            makeDir(tmpDir.resolve("tmp"));

            ret = scopeRunner.fluxInTransactionlessScope(
                "Send SOS report queries",
                lockGuardFactory.buildDeferred(LockType.READ, LockObj.NODES_MAP),
                () -> sendRequests(nodes, tmpDir, since)
            ).flatMap(
                sosReportAnswer -> scopeRunner.fluxInTransactionalScope(
                    "Receiving SOS report",
                    lockGuardFactory.buildDeferred(LockType.READ, LockObj.NODES_MAP),
                    () ->
                    {
                        deserializeErrorReportsInto(sosReportAnswer.getT2(), tmpDir);
                        return Flux.<String>empty();
                    }
                )
            ).concatWith(
                scopeRunner.fluxInTransactionalScope(
                    "Finishing SOS report",
                    lockGuardFactory.buildDeferred(LockType.READ, LockObj.NODES_MAP),
                    () -> finishReport(nodes, tmpDir, since)
                )
            );
        }
        catch (IOException exc)
        {
            errorReporter.reportError(exc);
            ret = Flux.error(exc);
        }
        return ret;
    }

    private Flux<Tuple2<NodeName, ByteArrayInputStream>> sendRequests(
        Set<String> nodes,
        Path tmpDir,
        Date since
    )
        throws AccessDeniedException
    {
        HashSet<String> copyNodes = new HashSet<>();
        nodes.forEach(node -> copyNodes.add(node.toLowerCase()));
        Stream<Node> nodeStream = nodeRepository.getMapForView(peerAccCtx.get()).values().stream();
        if (!copyNodes.isEmpty())
        {
            nodeStream = nodeStream.filter(
                node -> copyNodes.contains(node.getName().getDisplayName().toLowerCase())
            );
        }

        List<Tuple2<NodeName, Flux<ByteArrayInputStream>>> namesAndRequests = nodeStream
            .map(node -> Tuples.of(node.getName(), prepareSosRequestApi(node, tmpDir, since)))
            .collect(Collectors.toList());

        return Flux.fromIterable(namesAndRequests)
            .flatMap(
                nameAndRequest -> nameAndRequest.getT2()
                    .map(byteStream -> Tuples.of(nameAndRequest.getT1(), byteStream))
            );
    }

    private Flux<ByteArrayInputStream> prepareSosRequestApi(Node node, Path tmpDir, Date since)
    {
        Peer peer = getPeer(node);
        Flux<ByteArrayInputStream> fluxReturn = Flux.empty();
        if (peer != null)
        {
            // already create the extTools file here, because you need the peer
            createExtToolsInfo(tmpDir.resolve("tmp/" + node.getName().displayValue), peer);
            byte[] msg = stltComSerializer.headerlessBuilder().requestSosReport(since).build();
            fluxReturn = peer.apiCall(ApiConsts.API_REQ_SOS_REPORT, msg)
                .onErrorResume(PeerNotConnectedException.class, ignored -> Flux.empty());
        }
        return fluxReturn;
    }

    private Peer getPeer(Node node)
    {
        Peer peer;
        try
        {
            peer = node.getPeer(peerAccCtx.get());
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "access peer for node '" + node.getName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        return peer;
    }

    private Flux<String> finishReport(
        Set<String> nodes,
        Path tmpDir,
        Date since
    )
        throws IOException, AccessDeniedException, ChildProcessTimeoutException, ExtCmdFailedException
    {
        if (containsController(nodes))
        {
            createControllerFilesInto(tmpDir, since);
        }

        String fileName = errorReporter.getLogDirectory() + "/" + SOS_PREFIX + sdfUtc.format(new Date()) + SOS_SUFFIX;
        HashSet<String> copyNodes = new HashSet<>();
        nodes.forEach(node -> copyNodes.add(node.toLowerCase()));
        Stream<Node> nodeStream = nodeRepository.getMapForView(peerAccCtx.get()).values().stream()
            .filter(
                node -> copyNodes.isEmpty() ||
                    copyNodes.contains(node.getName().getDisplayName().toLowerCase())
            );
        List<String> names = nodeStream.map(node -> node.getName().displayValue).collect(Collectors.toList());
        if (containsController(nodes))
        {
            names.add("_" + LinStor.CONTROLLER_MODULE);
        }
        createTar(tmpDir, fileName, names);
        return Flux.just(fileName);
    }

    private void createExtToolsInfo(Path dirPath, Peer peer)
    {
        try
        {
            makeDir(dirPath);
            StringBuilder sb = new StringBuilder();
            ExtTools[] extTools = ExtTools.values();
            for (ExtTools tool : extTools)
            {
                ExtToolsInfo info = peer.getExtToolsManager().getExtToolInfo(tool);
                sb.append(tool.name()).append("\n");
                if (info.isSupported())
                {
                    sb.append("\tSupported\n");
                    sb.append("\tVersion: ")
                        .append(info.getVersionMajor())
                        .append(".")
                        .append(info.getVersionMinor())
                        .append(".")
                        .append(info.getVersionPatch())
                        .append("\n");
                }
                else
                {
                    sb.append("\tNot Supported\n");
                    sb.append("\tReason:\n");
                    for (String reason : info.getNotSupportedReasons())
                    {
                        sb.append("\t\t").append(reason).append("\n");
                    }
                }
            }
            Files.write(dirPath.resolve("extTools"), sb.toString().getBytes());
        }
        catch (IOException exc)
        {
            errorReporter.reportError(exc);
        }
    }

    private void createControllerFilesInto(Path tmpDir, Date since) throws IOException
    {
        long nowMillis = System.currentTimeMillis();
        Date now = new Date(nowMillis);

        String nodeName = "_" + LinStor.CONTROLLER_MODULE;
        Path sosDir = tmpDir.resolve("tmp/" + nodeName);
        String infoContent = LinStor.linstorInfo() + "\n\nuname -a:           " + LinStor.getUname("-a");
        append(sosDir.resolve("linstorInfo"), infoContent.getBytes(), nowMillis);

        String timeContent = "Local Time: " + sdf.format(now) + "\nUTC Time:   " + sdfUtc.format(now);
        append(sosDir.resolve("timeInfo"), timeContent.getBytes(), nowMillis);
        getDbDump(sosDir);

        String tomlPath = ctrlCfg.getConfigDir() + LinstorConfig.LINSTOR_CTRL_CONFIG;
        CommandHelper[] commands = new CommandHelper[]
        {
            new CommandHelper(
                sosDir.resolve(LinstorConfig.LINSTOR_CTRL_CONFIG),
                new String[]
                {
                    "cp", "-p", tomlPath, sosDir.toString()
                }
            ),
            new CommandHelper(
                sosDir.resolve("journalctl"),
                new String[]
                {
                    "journalctl", "-u", "linstor-controller", "--since", LinStor.JOURNALCTL_DF.format(since)
                }
            ),
            new CommandHelper(
                sosDir.resolve("ip-a"),
                new String[]
                {
                    "ip", "a"
                }
            ),
            new CommandHelper(
                sosDir.resolve("log-syslog"),
                new String[]
                {
                    "cp", "-p", "/var/log/syslog", sosDir.toString() + "/log-syslog"
                }
            ),
            new CommandHelper(
                sosDir.resolve("log-kern.log"),
                new String[]
                {
                    "cp", "-p", "/var/log/kern.log", sosDir.toString() + "/log-kern.log"
                }
            ),
            new CommandHelper(
                sosDir.resolve("log-messages"),
                new String[]
                {
                    "cp", "-p", "/var/log/messages", sosDir.toString() + "/log-messages"
                }
            ),
            new CommandHelper(
                sosDir.resolve("release"),
                new String[]
                {
                    "cat", "/etc/redhat-release", "/etc/lsb-release", "/etc/os-release"
                }
            ),
        };
        for (CommandHelper cmd : commands)
        {
            makeFileFromCmdNoFailed(cmd.file, nowMillis, cmd.cmd);
        }

        FileCollector collector = new FileCollector(errorReporter.getLogDirectory());
        Files.walkFileTree(errorReporter.getLogDirectory(), collector);
        Set<SosReportType> errorReports = collector.getFiles();
        for (SosReportType err : errorReports)
        {
            Path erroReportPath = sosDir.resolve("logs/" + Paths.get(err.getRelativeFileName()).getFileName());
            makeFileFromCmdNoFailed(
                addExtension(erroReportPath, ".out"),
                nowMillis,
                "cp", "-p",
                errorReporter.getLogDirectory().resolve(err.getRelativeFileName()).toString(),
                erroReportPath.toString()
            );
        }
    }

    private void deserializeErrorReportsInto(InputStream msgDataIn, Path tmpDir) throws IOException
    {
        MsgSosReportOuterClass.MsgSosReport msgSosReport = MsgSosReportOuterClass.MsgSosReport
            .parseDelimitedFrom(msgDataIn);
        while (msgSosReport != null)
        {
            String nodeName = msgSosReport.getNodeName();
            Path sosDir = tmpDir.resolve("tmp/" + nodeName);
            FileOuterClass.File file = msgSosReport.getFile();
            append(
                sosDir.resolve(file.getRelativeTitle()),
                file.getContent().toByteArray(),
                file.getTime()
            );
            msgSosReport = MsgSosReportOuterClass.MsgSosReport.parseDelimitedFrom(msgDataIn);
        }
    }

    private void makeDir(Path dirPath) throws IOException
    {
        if (!Files.exists(dirPath))
        {
            Files.createDirectory(dirPath);
        }
    }

    // make file with stderr & stdout if failed, but don't mark it as failed, make file with stdout if successful
    private void makeFileFromCmdNoFailed(Path filePath, long timestamp, String... command)
    {
        LinkedBlockingDeque<Event> deque = new LinkedBlockingDeque<>(DFLT_DEQUE_CAPACITY);
        DaemonHandler dh = new DaemonHandler(deque, command);

        Path outFile = filePath;
        Path errFile = addExtension(filePath, ".err");

        boolean running;
        try
        {
            dh.startUndelimited();
            running = true;
        }
        catch (IOException exc)
        {
            running = false;

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            exc.printStackTrace(new PrintWriter(baos));
            byte[] data = baos.toByteArray();

            append(addExtension(filePath, ".ioexc"), data, timestamp, command);
        }

        while (running)
        {
            Event event;

            byte[] data = null;
            Path file = null;
            try
            {
                event = deque.take();

                if (event instanceof StdOutEvent)
                {
                    StdOutEvent stdOutEvent = (StdOutEvent) event;
                    data = stdOutEvent.data;
                    file = outFile;
                }
                else if (event instanceof StdErrEvent)
                {
                    StdErrEvent stdErrEvent = (StdErrEvent) event;
                    data = stdErrEvent.data;
                    file = errFile;
                }
                else if (event instanceof EOFEvent)
                {
                    running = false;
                }
                else if (event instanceof ExceptionEvent)
                {
                    running = false;

                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    ((ExceptionEvent) event).exc.printStackTrace(new PrintWriter(baos));
                    data = baos.toByteArray();

                    file = addExtension(filePath, ".exc");
                }
                else
                {
                    errorReporter.logError(
                        "Unknown event type during SOS report: %s while processing command %s",
                        event.getClass().getCanonicalName(),
                        StringUtils.join(" ", command)
                    );
                }
            }
            catch (InterruptedException exc)
            {
                running = false;
                data = "Interrupted".getBytes();
                file = outFile;

                Thread.currentThread().interrupt();
            }
            if (file != null && data != null)
            {
                append(file, data, timestamp, command);
            }
        }
    }

    private Path addExtension(Path pathRef, String extensionRef)
    {
        return pathRef.getParent().resolve(pathRef.getFileName() + extensionRef);
    }

    private void append(Path file, byte[] data, long timestampRef, String... command)
    {
        try
        {
            if (!Files.exists(file.getParent()))
            {
                Files.createDirectories(file.getParent());
            }
            Files.write(file, data, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
            errorReporter.logTrace("Written %d bytes to file %s", data.length, file.toString());
            Files.setAttribute(
                file,
                "lastModifiedTime",
                FileTime.fromMillis(timestampRef)
            );
        }
        catch (IOException exc)
        {
            if (command != null && command.length > 0)
            {
                errorReporter.logError(
                    "IOException occured while writing to %s from the command '%s'",
                    file.toString(),
                    StringUtils.join(" ", command)
                );
            }
            else
            {
                errorReporter.logError(
                    "IOException occured while writing to %s",
                    file.toString()
                );
            }
            errorReporter.reportError(exc);
        }

    }

    private void getDbDump(Path dirPath) throws IOException
    {
        try
        {
            Path dbDump = dirPath.resolve("dbDump");
            Files.write(dbDump, db.getDbDump().getBytes());
        }
        catch (DatabaseException exc)
        {
            String reportName = errorReporter.reportError(exc);
            append(
                dirPath.resolve("dbDump.failed"),
                ("ErrorReport-" + reportName).getBytes(),
                System.currentTimeMillis()
            );
        }
    }

    private void createTar(Path source, String fileName, List<String> names)
            throws IOException, ExtCmdFailedException, ChildProcessTimeoutException
    {
        ExtCmd extCommand = extCmdFactory.create();
        List<String> cmd = new ArrayList<>();
        cmd.add("tar");
        cmd.add("-C");
        cmd.add(source.toString() + "/tmp");
        cmd.add("-czvf");
        cmd.add(fileName);
        cmd.addAll(names);
        String[] command = cmd.toArray(new String[0]);

        OutputData output = extCommand.exec(command);
        if (output.exitCode != 0)
        {
            throw new ExtCmdFailedException(command, output);
        }
        command = new String[]
        {
            "rm", "-rf", source.toString()
        };
        output = extCmdFactory.create().exec(command);
        if (output.exitCode != 0)
        {
            throw new ExtCmdFailedException(command, output);
        }
    }

    private static class CommandHelper
    {
        private final Path file;
        private final String[] cmd;

        private CommandHelper(
            Path fileRef,
            String[] cmdRef
        )
        {
            file = fileRef;
            cmd = cmdRef;
        }
    }

    private boolean containsController(Set<String> nodes)
    {
        return nodes.isEmpty() || nodes.stream().anyMatch(LinStor.CONTROLLER_MODULE::equalsIgnoreCase);
    }
}
