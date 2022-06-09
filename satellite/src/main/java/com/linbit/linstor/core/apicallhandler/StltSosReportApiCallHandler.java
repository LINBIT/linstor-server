package com.linbit.linstor.core.apicallhandler;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.SosReportType;
import com.linbit.linstor.SosReportType.SosCommandType;
import com.linbit.linstor.SosReportType.SosFileType;
import com.linbit.linstor.SosReportType.SosInfoType;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.pojo.FileInfoPojo;
import com.linbit.linstor.api.pojo.FilePojo;
import com.linbit.linstor.api.pojo.RequestFilePojo;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.cfg.LinstorConfig;
import com.linbit.linstor.core.cfg.StltConfig;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.utils.FileCollector;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

@Singleton
public class StltSosReportApiCallHandler
{
    private static final Path SOS_REPORTS_DIR = Paths.get(LinStor.CONFIG_PATH + "/sos-reports/");

    private static final String SUFFIX_CMD_STDERR = ".err";
    private static final String SUFFIX_FILE_NOT_FOUND = ".file_not_found";
    private static final String SUFFIX_IO_EXC = ".io_exc";

    private final ErrorReporter errorReporter;
    private final ControllerPeerConnector controllerPeerConnector;
    private final CtrlStltSerializer interComSerializer;
    private final Provider<Long> apiCallId;
    private final StltConfig stltCfg;

    @Inject
    public StltSosReportApiCallHandler(
        final ErrorReporter errorReporterRef,
        final ControllerPeerConnector controllerPeerConnectorRef,
        final CtrlStltSerializer interComSerializerRef,
        final @Named(ApiModule.API_CALL_ID) Provider<Long> apiCallIdRef,
        final StltConfig stltCfgRef
    )
    {
        errorReporter = errorReporterRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        interComSerializer = interComSerializerRef;
        apiCallId = apiCallIdRef;
        stltCfg = stltCfgRef;
    }

    /**
     * Collects a list of local reports, stores them in the linstor.d directory and replies the controller the list of
     * files with their sizes, but without their content. The files with content have to be requested separately.
     * The list of collected Reports can be found in {@link #listSosReport(Date)}
     *
     * @param since
     */
    public void handleSosReportRequestFileList(String sosReportName, Date since)
    {
        StringBuilder errors = new StringBuilder();
        List<FileInfoPojo> fileList = new ArrayList<>();

        Path sosReportDir = SOS_REPORTS_DIR.resolve(sosReportName);
        try
        {
            Files.createDirectories(sosReportDir);
        }
        catch (IOException exc)
        {
            appendExcToStringBuilder(errors, exc);
        }

        if (errors.length() == 0)
        {
            Set<SosReportType> reports = listSosReport(sosReportDir, since);

            for (SosReportType report : reports)
            {
                String fileName = report.getRelativeFileName();
                long timestamp = report.getTimestamp();
                if ((report instanceof SosReportType.SosFileType) || (report instanceof SosReportType.SosInfoType))
                {
                    Path targetPath = sosReportDir.resolve(fileName);
                    if (report instanceof SosReportType.SosInfoType)
                    {
                        try
                        {
                            Files.write(targetPath, ((SosInfoType) report).getInfo().getBytes());
                            fileList.add(new FileInfoPojo(fileName, targetPath.toFile().length()));
                        }
                        catch (IOException exc)
                        {
                            appendExcToStringBuilder(errors, exc);
                        }
                    }
                    else if (report instanceof SosReportType.SosFileType)
                    {
                        Path sourcePath = ((SosFileType) report).getPath();
                        try
                        {
                            if (Files.exists(sourcePath))
                            {
                                if (!Files.exists(targetPath.getParent()))
                                {
                                    Files.createDirectories(targetPath.getParent());
                                }
                                Files.copy(
                                    sourcePath,
                                    targetPath,
                                    StandardCopyOption.COPY_ATTRIBUTES
                                );
                                fileList.add(new FileInfoPojo(fileName, targetPath.toFile().length()));
                            }
                            else
                            {
                                String fileNameNotFound = fileName + SUFFIX_FILE_NOT_FOUND;
                                Files.createFile(sosReportDir.resolve(fileNameNotFound));
                                fileList.add(new FileInfoPojo(fileNameNotFound, 0));
                            }
                        }
                        catch (IOException exc)
                        {
                            byte[] exceptionData = exceptionToString(exc).getBytes();
                            try
                            {
                                String fileNameIoExc = fileName + SUFFIX_IO_EXC;
                                Files.write(sosReportDir.resolve(fileNameIoExc), exceptionData);
                                fileList.add(new FileInfoPojo(fileNameIoExc, exceptionData.length));
                            }
                            catch (IOException exc1)
                            {
                                appendExcToStringBuilder(errors, exc);
                                appendExcToStringBuilder(errors, exc1);
                            }
                        }
                    }
                    else
                    {
                        throw new ImplementationError("Unknown SosReportType: " + report.getClass().getCanonicalName());
                    }
                }
                else if (report instanceof SosReportType.SosCommandType)
                {
                    SosCommandType sosCommandType = (SosCommandType) report;
                    String[] command = sosCommandType.getCommand();

                    try
                    {
                        File outFile = sosReportDir.resolve(fileName).toFile();
                        String errFileName = fileName + SUFFIX_CMD_STDERR;
                        File errFile = sosReportDir.resolve(errFileName).toFile();

                        boolean hasErrFile = copy(
                            outFile,
                            errFile,
                            command,
                            timestamp
                        );
                        fileList.add(new FileInfoPojo(fileName, outFile.length()));

                        if (hasErrFile)
                        {
                            fileList.add(new FileInfoPojo(errFileName, errFile.length()));
                        }
                    }
                    catch (IOException | InterruptedException exc)
                    {
                        byte[] exceptionData = exceptionToString(exc).getBytes();
                        try
                        {
                            String fileNameIoExc = fileName + SUFFIX_IO_EXC;
                            Files.write(sosReportDir.resolve(fileNameIoExc), exceptionData);
                            fileList.add(new FileInfoPojo(fileNameIoExc, exceptionData.length));
                        }
                        catch (IOException exc1)
                        {
                            appendExcToStringBuilder(errors, exc);
                            appendExcToStringBuilder(errors, exc1);
                        }
                    }
                }
            }
        }

        controllerPeerConnector.getControllerPeer().sendMessage(
            interComSerializer.answerBuilder(InternalApiConsts.API_RSP_SOS_REPORT_FILE_LIST, apiCallId.get())
                .sosReportFileInfoList(
                    controllerPeerConnector.getLocalNodeName().displayValue,
                    sosReportName,
                    fileList,
                    errors.toString()
                )
                .build(),
            InternalApiConsts.API_RSP_SOS_REPORT_FILE_LIST
        );
    }

    private void appendExcToStringBuilder(StringBuilder errors, Exception exc)
    {
        errors.append(exceptionToString(exc));
    }

    private String exceptionToString(Exception exc)
    {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        exc.printStackTrace(pw);
        return sw.toString();
    }

    private boolean copy(
        final File outFileRef,
        final File errFileRef,
        final String[] commandRef,
        final long timepstampRef
    )
        throws IOException, InterruptedException
    {
        ProcessBuilder pb = new ProcessBuilder(commandRef);
        pb.redirectOutput(outFileRef);
        pb.redirectError(errFileRef);

        Process proc = pb.start();
        proc.waitFor();

        outFileRef.setLastModified(timepstampRef);
        boolean errFileExists;
        if (errFileRef.length() == 0)
        {
            Files.delete(errFileRef.toPath());
            errFileExists = false;
        }
        else
        {
            errFileRef.setLastModified(timepstampRef);
            errFileExists = true;
        }
        return errFileExists;
    }

    public void handleSosReportRequestedFiles(String sosReportName, List<RequestFilePojo> listRef)
    {
        Path sosReportDir = SOS_REPORTS_DIR.resolve(sosReportName);

        List<FilePojo> filesToRespond = new ArrayList<>();
        long now = System.currentTimeMillis();

        for (RequestFilePojo reqFile : listRef)
        {
            String relativeFileName = reqFile.name;
            String fileName = sosReportDir.resolve(relativeFileName).toString();
            File file = new File(fileName);

            try (RandomAccessFile raf = new RandomAccessFile(file, "r");)
            {
                if (reqFile.offset != 0)
                {
                    raf.seek(reqFile.offset);
                }
                int len = (int) reqFile.length;

                byte[] buf = new byte[len];
                errorReporter
                    .logTrace("Reading %d bytes from file %s from offset %d.", len, reqFile.name, reqFile.offset);
                raf.readFully(buf, 0, buf.length);

                filesToRespond.add(new FilePojo(relativeFileName, file.lastModified(), buf, reqFile.offset));
            }
            catch (IOException exc)
            {
                String errorSuffix = SUFFIX_IO_EXC;
                if (exc instanceof FileNotFoundException)
                {
                    errorSuffix = SUFFIX_FILE_NOT_FOUND;
                }
                filesToRespond.add(
                    new FilePojo(
                        relativeFileName + errorSuffix,
                        now,
                        exceptionToString(exc).getBytes(),
                        0
                    )
                );
            }
        }
        byte[] build = interComSerializer.answerBuilder(InternalApiConsts.API_RSP_SOS_REPORT_FILE_LIST, apiCallId.get())
            .sosReportFiles(controllerPeerConnector.getLocalNodeName().displayValue, sosReportName, filesToRespond)
            .build();
        errorReporter.logTrace("Responding (partial) sos-report %s, bytes: %d", sosReportName, build.length);
        controllerPeerConnector.getControllerPeer().sendMessage(
            build,
            InternalApiConsts.API_RSP_SOS_REPORT_FILE_LIST
        );
    }

    /**
     *
     * Returns a list of files to collect (does not collect anything, just builds and returns the list).
     * Collected reports:
     *
     * <table>
     * <style>table tr td { padding-right: 10px; }</style>
     * <tr><th>Filename</th><th>Content</th></tr>
     * <tr><td>linstorInfo</td><td>'uname -a' + Linstor internal information</td></tr>
     * <tr><td>drbd-status</td><td>'drbdset status -vvv'</td></tr>
     * <tr><td>drbd-events2</td><td>'drbdset status -vvv'</td></tr>
     * <tr><td>modinfo</td><td>'modinfo drbd'</td></tr>
     * <tr><td>proc-drbd</td><td>'cat /proc/drbd'</td></tr>
     * <tr><td>lvm.conf</td><td>'cat /etc/lvm/lvm.conf'</td></tr>
     * <tr><td>linstor_satellite.toml</td><td>'cat $configDir/lisntor_satellite.toml'</td></tr>
     * <tr><td>journalctl</td><td>'journalctl -u linstor-satellite --since $since'</td></tr>
     * <tr><td>ip-a</td><td>'ip a'</td></tr>
     * <tr><td>drbdadm-version</td><td>'drbdadm --version'</td></tr>
     * <tr><td>log-syslog</td><td>'cat /var/log/syslog'</td></tr>
     * <tr><td>log-kern.log</td><td>'cat /var/log/kern.log'</td></tr>
     * <tr><td>log-messages</td><td>'cat /var/log/messages'</td></tr>
     * <tr><td>release</td><td>'cat /etc/redhat-release /etc/lsb-release /etc/os-release'</td></tr>
     * <tr><td>res/*.res</td><td>All files from /var/lib/linstor.d/*.res</td></tr>
     * <tr><td>logs/*</td><td>All '*{mv.db,log}' from /var/log/linstor (unless overridden) </td></tr>
     * </table>
     * @param sosReportDir
     * @param since
     */
    private Set<SosReportType> listSosReport(Path sosReportDir, Date since)
    {
        Set<SosReportType> reportTypes = new HashSet<>();

        long now = System.currentTimeMillis();

        reportTypes.add(
            new SosInfoType(
                "linstorInfo",
                now,
                LinStor.linstorInfo() + "\n\nuname -a:           " + LinStor.getUname("-a")
            )
        );

        reportTypes.add(new SosCommandType("drbd-status", now,  "drbdsetup", "status", "-vvv"));
        reportTypes.add(new SosCommandType("drbd-events2", now,  "drbdsetup", "events2", "all", "--now"));
        reportTypes.add(new SosCommandType("modinfo", now,  "modinfo", "drbd"));
        reportTypes.add(new SosCommandType("proc-drbd", now, "cat", "/proc/drbd"));
        reportTypes.add(new SosCommandType("lvm.conf",  now, "cat", "/etc/lvm/lvm.conf"));
        reportTypes.add(
            new SosCommandType(
                LinstorConfig.LINSTOR_STLT_CONFIG,
                now,
                "cat",
                stltCfg.getConfigDir() + LinstorConfig.LINSTOR_STLT_CONFIG
            )
        );
        reportTypes.add(new SosCommandType("dmesg", now, "dmesg", "-H"));
        reportTypes.add(
            new SosCommandType(
                "journalctl",
                now,
                "journalctl",
                "-u",
                "linstor-satellite",
                "--since",
                LinStor.JOURNALCTL_DF.format(since)
            )
        );
        reportTypes.add(new SosCommandType("ip-a", now, "ip", "a"));
        reportTypes.add(new SosCommandType("drbdadm-version", now, "drbdadm", "--version"));
        reportTypes.add(new SosCommandType("log-syslog", now, "cat", "/var/log/syslog"));
        reportTypes.add(new SosCommandType("log-kern.log", now, "cat", "/var/log/kern.log"));
        reportTypes.add(new SosCommandType("log-messages", now, "cat", "/var/log/messages"));
        reportTypes.add(
            new SosCommandType(
                "release",
                now,
                "cat",
                "/etc/redhat-release",
                "/etc/lsb-release",
                "/etc/os-release"
            )
        );


        String linstorDDir = LinStor.CONFIG_PATH;
        try (
            Stream<Path> resFileStream = Files.list(Paths.get(linstorDDir)))
        {
            resFileStream
                .filter(file -> file.getFileName().toString().endsWith(".res"))
                .forEach(
                    file -> reportTypes.add(
                        new SosFileType(
                            "res/" + file.getFileName().toString(),
                            file.toString(),
                            file.toFile().lastModified()
                        )
                    )
                );

            FileCollector collector = new FileCollector(errorReporter.getLogDirectory());
            Files.walkFileTree(errorReporter.getLogDirectory(), collector);
            reportTypes.addAll(collector.getFiles());
        }
        catch (IOException ioExc)
        {
            try
            {
                String fileNameIoExc = "res/error_reports_list" + SUFFIX_IO_EXC;
                byte[] exceptionData = exceptionToString(ioExc).getBytes();
                Files.write(sosReportDir.resolve(fileNameIoExc), exceptionData);
                reportTypes.add(new SosFileType("error_reports_list_io_exc", fileNameIoExc, now));
                errorReporter.reportError(ioExc);
            }
            catch (IOException exc1)
            {
                // nothing more we can do here...
                errorReporter.reportError(exc1);
            }
        }
        return reportTypes;
    }
}
