package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.extproc.ExtCmdFailedException;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.logging.LinstorFile;
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
import com.linbit.utils.BiExceptionThrowingBiConsumer;
import com.linbit.utils.FileCollector;

import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.inject.Inject;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

@Singleton
public class CtrlSosReportApiCallHandler
{
    private static final String SOS_PREFIX = "sos_";
    private static final String SOS_SUFFIX = ".tar.gz";
    private static final DateFormat SDF = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
    private static final DateFormat SDF_UTC = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
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
        SDF_UTC.setTimeZone(TimeZone.getTimeZone("UTC"));
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

            ret = scopeRunner
                .fluxInTransactionlessScope(
                    "Collect SOS Report", lockGuardFactory.buildDeferred(LockType.READ, LockObj.NODES_MAP),
                    () -> assembleRequests(nodes, tmpDir, since)
                ).collectList()
                .flatMapMany(
                    sosReportAnswers -> scopeRunner
                        .fluxInTransactionalScope(
                            "Assemble SOS Report", lockGuardFactory.buildDeferred(LockType.READ, LockObj.NODES_MAP),
                            () -> assembleReport(nodes, sosReportAnswers, tmpDir, since)
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

    private Flux<Tuple2<NodeName, ByteArrayInputStream>> assembleRequests(
        Set<String> nodes,
        Path tmpDir,
        Date since
    )
        throws AccessDeniedException
    {
        HashSet<String> copyNodes = new HashSet<>();
        nodes.forEach(node -> copyNodes.add(node.toLowerCase()));
        Stream<Node> nodeStream = nodeRepository.getMapForView(peerAccCtx.get()).values().stream()
            .filter(
                node -> copyNodes.isEmpty() ||
                    copyNodes.contains(node.getName().getDisplayName().toLowerCase())
            );

        List<Tuple2<NodeName, Flux<ByteArrayInputStream>>> namesAndRequests = nodeStream
            .map(node -> Tuples.of(node.getName(), prepareSosRequestApi(node, tmpDir, since))).collect(Collectors.toList());

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

    private Flux<String> assembleReport(
        Set<String> nodes,
        List<Tuple2<NodeName, ByteArrayInputStream>> sosReportAnswers,
        Path tmpDir,
        Date since
    )
        throws IOException, AccessDeniedException, ChildProcessTimeoutException, ExtCmdFailedException
    {
        if (containsController(nodes))
        {
            createControllerFilesInto(tmpDir, since);
        }
        for (Tuple2<NodeName, ByteArrayInputStream> sosReportAnswer : sosReportAnswers)
        {
            ByteArrayInputStream sosReportMsgDataIn = sosReportAnswer.getT2();
            deserializeErrorReportsInto(sosReportMsgDataIn, tmpDir);
        }

        String fileName = errorReporter.getLogDirectory() + "/" + SOS_PREFIX + SDF_UTC.format(new Date()) + SOS_SUFFIX;
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
        String nodeName = "_" + LinStor.CONTROLLER_MODULE;
        Path sosDir = tmpDir.resolve("tmp/" + nodeName);
        String infoContent = LinStor.linstorInfo() + "\n\nuname -a:           " + LinStor.getUname("-a");
        makeFile(sosDir.resolve("linstorInfo"), infoContent, System.currentTimeMillis());
        Date now = new Date();
        String timeContent = "Local Time: " + SDF.format(now) + "\nUTC Time:   " + SDF_UTC.format(now);
        makeFile(sosDir.resolve("timeInfo"), timeContent, now.getTime());
        getDbDump(sosDir);
        try
        {
            String tomlPath = ctrlCfg.getConfigDir() + CtrlConfig.LINSTOR_CTRL_CONFIG;
            CommandHelper[] commands = new CommandHelper[]
            {
                new CommandHelper(
                    sosDir.resolve(CtrlConfig.LINSTOR_CTRL_CONFIG),
                    new String[]
                    {
                        "cp", "-p", tomlPath, sosDir.toString()
                    },
                    this::makeFileFromCmdErrOnly
                ),
                new CommandHelper(
                    sosDir.resolve("journalctl"),
                    new String[]
                    {
                        "journalctl", "-u", "linstor-satellite", "--since", LinStor.JOURNALCTL_DF.format(since)
                    },
                    this::makeFileFromCmd
                ),
                new CommandHelper(
                    sosDir.resolve("ip-a"),
                    new String[]
                    {
                        "ip", "a"
                    },
                    this::makeFileFromCmd
                ),
                new CommandHelper(
                    sosDir.resolve("log-syslog"),
                    new String[]
                    {
                        "cp", "-p", "/var/log/syslog", sosDir.toString() + "/log-syslog"
                    },
                    this::makeFileFromCmdIfFailed
                ),
                new CommandHelper(
                    sosDir.resolve("log-kern.log"),
                    new String[]
                    {
                        "cp", "-p", "/var/log/kern.log", sosDir.toString() + "/log-kern.log"
                    },
                    this::makeFileFromCmdIfFailed
                ),
                new CommandHelper(
                    sosDir.resolve("log-messages"),
                    new String[]
                    {
                        "cp", "-p", "/var/log/messages", sosDir.toString() + "/log-messages"
                    },
                    this::makeFileFromCmdIfFailed
                ),
                new CommandHelper(
                    sosDir.resolve("release"),
                    new String[]
                    {
                        "cat", "/etc/redhat-release", "/etc/lsb-release", "/etc/os-release"
                    },
                    this::makeFileFromCmdNoFailed
                ),
            };
            for (CommandHelper cmd : commands)
            {
                cmd.handleExitCode.accept(cmd.file, cmd.cmd);
            }
        }
        catch (ChildProcessTimeoutException exc)
        {
            errorReporter.reportError(exc);
        }

        FileCollector collector = new FileCollector(nodeName, errorReporter.getLogDirectory());
        Files.walkFileTree(errorReporter.getLogDirectory(), collector);
        Set<LinstorFile> errorReports = collector.getFiles();
        for (LinstorFile err : errorReports)
        {
            makeFile(sosDir.resolve(err.getFileName()), err.getText(), err.getDateTime().getTime());
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
            makeFile(sosDir.resolve(file.getTitle()), file.getText(), file.getTime());
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

    private void makeFile(Path filePath, String text, long time) throws IOException
    {
        Files.createDirectories(filePath.getParent());
        Files.write(
            filePath,
            text.getBytes()
        );
        Files.setAttribute(
            filePath,
            "lastModifiedTime",
            FileTime.fromMillis(time)
        );
    }

    // make error report and write it in file if failed, no action if successful
    private void makeFileFromCmdErrOnly(Path filePath, String[] command)
        throws IOException, ChildProcessTimeoutException
    {
        OutputData output = extCmdFactory.create().exec(command);
        if (output.exitCode != 0)
        {
            String reportName = errorReporter.reportError(new ExtCmdFailedException(command, output));
            makeFile(
                Paths.get(filePath.toString() + ".failed"), "ErrorReport-" + reportName, System.currentTimeMillis()
            );
        }
    }

    // make error report and write it in file if failed, make file with stdout if successful
    private void makeFileFromCmd(Path filePath, String[] command)
        throws IOException, ChildProcessTimeoutException
    {
        OutputData output = extCmdFactory.create().exec(command);
        if (output.exitCode != 0)
        {
            String reportName = errorReporter.reportError(new ExtCmdFailedException(command, output));
            makeFile(
                Paths.get(filePath.toString() + ".failed"), "ErrorReport-" + reportName, System.currentTimeMillis()
            );
        }
        else
        {
            makeFile(filePath, new String(output.stdoutData), System.currentTimeMillis());
        }
    }

    // make file with stderr & stdout if failed, but don't mark it as failed, make file with stdout if successful
    private void makeFileFromCmdNoFailed(Path filePath, String[] command)
        throws IOException, ChildProcessTimeoutException
    {
        OutputData output = extCmdFactory.create().exec(command);
        if (output.exitCode != 0)
        {
            makeFile(
                filePath, new String(output.stdoutData) + "\n\n" + new String(output.stderrData), System.currentTimeMillis()
            );
        }
        else
        {
            makeFile(filePath, new String(output.stdoutData), System.currentTimeMillis());
        }
    }

    // make file with stderr & stdout if failed, no action if successful
    private void makeFileFromCmdIfFailed(Path filePath, String[] command)
        throws IOException, ChildProcessTimeoutException
    {
        OutputData output = extCmdFactory.create().exec(command);
        if (output.exitCode != 0)
        {
            makeFile(
                Paths.get(filePath.toString() + ".failed"), new String(output.stdoutData) + "\n\n" + new String(output.stderrData), System.currentTimeMillis()
            );
        }
    }

    private void getDbDump(Path dirPath) throws IOException
    {
        try
        {
            Files.write(Paths.get(dirPath + "/dbDump"), db.getDbDump().getBytes());
        }
        catch (DatabaseException exc)
        {
            String reportName = errorReporter.reportError(exc);
            makeFile(dirPath.resolve("dbDump.failed"), "ErrorReport-" + reportName, System.currentTimeMillis());
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
        Path file;
        String[] cmd;
        BiExceptionThrowingBiConsumer<Path, String[], IOException, ChildProcessTimeoutException> handleExitCode;

        CommandHelper(
            Path fileRef,
            String[] cmdRef,
            BiExceptionThrowingBiConsumer<Path, String[], IOException, ChildProcessTimeoutException> handleExitCodeRef
        )
        {
            file = fileRef;
            cmd = cmdRef;
            handleExitCode = handleExitCodeRef;
        }
    }

    private boolean containsController(Set<String> nodes)
    {
        return nodes.isEmpty() || nodes.stream().anyMatch(LinStor.CONTROLLER_MODULE::equalsIgnoreCase);
    }
}
