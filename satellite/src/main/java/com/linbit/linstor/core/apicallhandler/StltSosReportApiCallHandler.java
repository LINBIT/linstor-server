package com.linbit.linstor.core.apicallhandler;

import com.linbit.ImplementationError;
import com.linbit.linstor.SosReportType;
import com.linbit.linstor.SosReportType.SosCommandType;
import com.linbit.linstor.SosReportType.SosFileType;
import com.linbit.linstor.SosReportType.SosInfoType;
import com.linbit.linstor.api.pojo.FileInfoPojo;
import com.linbit.linstor.api.pojo.FilePojo;
import com.linbit.linstor.api.pojo.RequestFilePojo;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.cfg.LinstorConfig;
import com.linbit.linstor.core.cfg.StltConfig;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.utils.FileUtils;
import com.linbit.utils.CommandExec;
import com.linbit.utils.FileCollector;
import com.linbit.utils.Pair;
import com.linbit.utils.TimeUtils;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

@Singleton
public class StltSosReportApiCallHandler
{
    private static final String SUFFIX_CMD_STDERR = ".err";
    private static final String SUFFIX_FILE_NOT_FOUND = ".file_not_found";
    private static final String SUFFIX_IO_EXC = ".io_exc";

    private final ErrorReporter errorReporter;
    private final StltConfig stltCfg;

    @Inject
    public StltSosReportApiCallHandler(
        final ErrorReporter errorReporterRef,
        final StltConfig stltCfgRef
    )
    {
        errorReporter = errorReporterRef;
        stltCfg = stltCfgRef;
    }

    /**
     * Collects a list of local reports, stores them in the linstor.d/sos/<sos-report-name> directory and replies the
     * controller the list of files with their sizes, but without their content. The files with content have to be
     * requested separately.
     * The list of collected Reports can be found in {@link #listSosReport(Date)}
     */
    public Pair<List<FileInfoPojo>, String> handleSosReportRequestFileList(String sosReportName, LocalDateTime since)
    {
        StringBuilder errors = new StringBuilder();
        List<FileInfoPojo> fileList = new ArrayList<>();

        Path sosReportDir = getSosReportDir(sosReportName);
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
                if (report instanceof SosReportType.SosInfoType)
                {
                    appendInfoType(sosReportDir, (SosInfoType) report, fileList, errors);
                }
                else if (report instanceof SosReportType.SosFileType)
                {
                    appendFileType(sosReportDir, (SosFileType) report, fileList, errors);
                }
                else if (report instanceof SosReportType.SosCommandType)
                {
                    appendCommandType(sosReportDir, (SosCommandType) report, fileList, errors);
                }
                else
                {
                    throw new ImplementationError("Unknown SosReportType: " + report.getClass().getCanonicalName());
                }
            }
        }
        return new Pair<>(fileList, errors.toString());
    }

    /**
     * Writes the content of the SosInfoType into the destination file and adds a corresponding entry to the list of
     * FileInfoPojos.
     */
    private void appendInfoType(
        final Path sosReportDirRef,
        final SosInfoType reportRef,
        final List<FileInfoPojo> fileListRef,
        final StringBuilder errorsRef
    )
    {
        final Path targetPath = sosReportDirRef.resolve(reportRef.getFileName());
        try
        {
            Files.write(targetPath, reportRef.getInfo().getBytes());
            fileListRef.add(
                new FileInfoPojo(targetPath.toString(), targetPath.toFile().length(), reportRef.getTimestamp())
            );
            reportRef.setSuccess();
        }
        catch (IOException exc)
        {
            appendExcToStringBuilder(errorsRef, exc);
        }
    }

    /**
     * If <code>reportRef.isCopyEnabled()</code> returns true, the source file is copied to the target and from the
     * target file an entry in the list of FileInfoPojos is created.
     * Otherwise the source file is added to the list of FileInfoPojos and will be read when the controller requests it.
     */
    private void appendFileType(
        final Path sosReportDirRef,
        final SosFileType reportRef,
        final List<FileInfoPojo> fileListRef,
        final StringBuilder errorsRef
    )
    {
        final String fileName = reportRef.getFileName();
        final long timestamp = reportRef.getTimestamp();

        final Path targetPath = sosReportDirRef.resolve(fileName);
        final Path sourcePath = reportRef.getSourcePath();
        try
        {
            if (Files.exists(sourcePath))
            {
                if (!Files.exists(targetPath.getParent()))
                {
                    Files.createDirectories(targetPath.getParent());
                }
                final Path pathToAdd;
                if (reportRef.isCopyEnabled())
                {
                    Files.copy(
                        sourcePath,
                        targetPath,
                        StandardCopyOption.COPY_ATTRIBUTES
                    );
                    pathToAdd = targetPath;
                }
                else
                {
                    pathToAdd = sourcePath;
                }
                fileListRef.add(
                    new FileInfoPojo(
                        pathToAdd.toString(),
                        pathToAdd.toFile().length(),
                        timestamp
                    )
                );
                reportRef.setSuccess();
            }
            else
            {
                Path fileNameNotFound = sosReportDirRef.resolve(fileName + SUFFIX_FILE_NOT_FOUND);
                Files.createFile(fileNameNotFound);
                fileListRef.add(new FileInfoPojo(fileNameNotFound.toString(), 0, timestamp));
            }
        }
        catch (IOException exc)
        {
            byte[] exceptionData = CommandExec.exceptionToString(exc).getBytes();
            try
            {
                Path fileNameIoExc = sosReportDirRef.resolve(fileName + SUFFIX_IO_EXC);
                Files.write(fileNameIoExc, exceptionData);
                fileListRef.add(
                    new FileInfoPojo(fileNameIoExc.toString(), exceptionData.length, timestamp)
                );
            }
            catch (IOException exc1)
            {
                appendExcToStringBuilder(errorsRef, exc);
                appendExcToStringBuilder(errorsRef, exc1);
            }
        }
    }

    /**
     * Executes the command from the SosCommandType, stores stdOut and stdErr in <code>fileName</code> and
     * <code>fileName + SUFFIX_CMD_STDERR</code> respectively. The stdErr file is deleted if still empty after the
     * command finished.
     * The resulting file(s) are added to the list of FileInfoPojos.
     *
     * @param sosReportDirRef
     * @param reportRef
     * @param fileListRef
     * @param errorsRef
     */
    private void appendCommandType(
        final Path sosReportDirRef,
        final SosCommandType reportRef,
        final List<FileInfoPojo> fileListRef,
        final StringBuilder errorsRef
    )
    {
        final String fileName = reportRef.getFileName();
        final long timestamp = reportRef.getTimestamp();

        try
        {
            File outFile = sosReportDirRef.resolve(fileName).toFile();
            String errFileName = fileName + SUFFIX_CMD_STDERR;
            File errFile = sosReportDirRef.resolve(errFileName).toFile();

            if (reportRef.shouldExecute())
            {
                boolean hasErrFile = CommandExec.executeCmd(
                    reportRef.getCommand(),
                    outFile,
                    errFile,
                    timestamp
                );
                fileListRef.add(new FileInfoPojo(outFile.toString(), outFile.length(), timestamp));

                if (hasErrFile)
                {
                    fileListRef.add(new FileInfoPojo(errFile.toString(), errFile.length(), timestamp));
                }
                else
                {
                    reportRef.setSuccess();
                }
            }
        }
        catch (IOException | InterruptedException exc)
        {
            byte[] exceptionData = CommandExec.exceptionToString(exc).getBytes();
            try
            {
                Path fileNameIoExc = sosReportDirRef.resolve(fileName + SUFFIX_IO_EXC);
                Files.write(fileNameIoExc, exceptionData);
                fileListRef.add(new FileInfoPojo(fileNameIoExc.toString(), exceptionData.length, timestamp));
            }
            catch (IOException exc1)
            {
                appendExcToStringBuilder(errorsRef, exc);
                appendExcToStringBuilder(errorsRef, exc1);
            }
        }
    }

    /**
     * Simple helper method that appends the exc.printStackTrace String to the given StringBuilder
     */
    private void appendExcToStringBuilder(StringBuilder errors, Exception exc)
    {
        errors.append(CommandExec.exceptionToString(exc));
    }

    /**
     * Returns the requested files, including their content.
     */
    public List<FilePojo> getRequestedSosReportFiles(List<RequestFilePojo> listRef)
    {
        List<FilePojo> filesToRespond = new ArrayList<>();
        long now = System.currentTimeMillis();

        final String errorLogdir = errorReporter.getLogDirectory().toString();
        for (RequestFilePojo reqFile : listRef)
        {
            String fileName = reqFile.name;
            if (!fileName.startsWith(LinStor.CONFIG_PATH) && !fileName.startsWith(errorLogdir))
            {
                errorReporter.logWarning("Skipped unexpected access to file: %s", reqFile.name);
            }
            else
            {
                File file = new File(fileName);

                try (RandomAccessFile raf = new RandomAccessFile(file, "r");)
                {
                    if (reqFile.offset != 0)
                    {
                        raf.seek(reqFile.offset);
                    }
                    int len = (int) reqFile.length;

                    byte[] buf = new byte[len];
                    errorReporter.logTrace(
                        "Reading %8d bytes from file %s from offset %d.",
                        len,
                        reqFile.name,
                        reqFile.offset
                    );
                    raf.readFully(buf, 0, buf.length);

                    filesToRespond.add(new FilePojo(fileName, file.lastModified(), buf, reqFile.offset));
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
                            fileName + errorSuffix,
                            now,
                            CommandExec.exceptionToString(exc).getBytes(),
                            0
                        )
                    );
                }
            }
        }
        return filesToRespond;
    }

    private Path getSosReportDir(String sosReportName)
    {
        return LinStor.SOS_REPORTS_DIR.resolve(sosReportName);
    }

    /**
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
     *
     * @param sosReportDir
     * @param sinceRef
     */
    private Set<SosReportType> listSosReport(Path sosReportDir, LocalDateTime sinceRef)
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
        reportTypes.add(new SosCommandType("proc-sys-kernel-tainted", now, "cat", "/proc/sys/kernel/tainted"));
        reportTypes.add(new SosCommandType("lvm.conf",  now, "lvmconfig", "--type", "full"));
        reportTypes.add(
            new SosCommandType(
                LinstorConfig.LINSTOR_STLT_CONFIG,
                now,
                "cat",
                stltCfg.getConfigDir() + LinstorConfig.LINSTOR_STLT_CONFIG
            )
        );
        reportTypes.add(new SosCommandType("dmesg", now, "dmesg", "-T", "-d"));
        reportTypes.add(
            new SosCommandType(
                "journalctl",
                now,
                "journalctl",
                "-u",
                "linstor-satellite",
                "--since",
                TimeUtils.JOURNALCTL_DF.format(sinceRef)
            )
        );
        reportTypes.add(
            new SosCommandType(
                "journalctl-kernel-log",
                now,
                "journalctl",
                "--dmesg", // "-k": show only kernel messages
                "-b", "all", // show messages from a specific boot, or "all" as in this case
                "--since",
                TimeUtils.JOURNALCTL_DF.format(sinceRef)
            )
        );
        reportTypes.add(new SosCommandType("ip-a", now, "ip", "a"));
        reportTypes.add(new SosCommandType("drbdadm-version", now, "drbdadm", "--version"));
        SosCommandType messageLog = new SosCommandType("log-messages", now, "cat", "/var/log/messages");
        reportTypes.add(messageLog);
        reportTypes.add(new SosCommandType("log-syslog", now, messageLog::hasFailed, "cat", "/var/log/syslog"));
        reportTypes.add(new SosCommandType("log-kern.log", now, "cat", "/var/log/kern.log"));
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
        reportTypes.add(new SosCommandType("daemon.log", now, "cat", "/var/log/daemon.log"));
        reportTypes.add(new SosCommandType("uptime", now, "uptime"));
        reportTypes.add(new SosCommandType("lsblk", now, "lsblk", "-O"));
        reportTypes.add(new SosCommandType("lsblk.json", now, "lsblk", "-O", "--json"));
        reportTypes.add(new SosCommandType("lvs", now, "lvs"));
        reportTypes.add(new SosCommandType("vgs", now, "vgs"));
        reportTypes.add(new SosCommandType("pvs", now, "pvs"));
        reportTypes.add(new SosCommandType("zfs-list", now, "zfs", "list", "-t", "snapshot,volume"));
        reportTypes.add(new SosCommandType("zpool-list", now, "zpool", "list"));
        reportTypes.add(new SosCommandType("zpool-status", now, "zpool", "status"));


        String linstorDDir = LinStor.CONFIG_PATH;
        try (
            Stream<Path> resFileStream = Files.list(Paths.get(linstorDDir)))
        {
            resFileStream
                .filter(file -> file.getFileName().toString().endsWith(".res"))
                .forEach(
                    file -> reportTypes.add(
                        new SosFileType(
                            file.toString(),
                            true,
                            file.toFile().lastModified()
                        )
                    )
                );

            FileCollector collector = new FileCollector();
            Files.walkFileTree(errorReporter.getLogDirectory(), collector);
            reportTypes.addAll(collector.getFiles());
        }
        catch (IOException ioExc)
        {
            try
            {
                String fileNameIoExc = "error_reports_list" + SUFFIX_IO_EXC;
                byte[] exceptionData = CommandExec.exceptionToString(ioExc).getBytes();
                Files.write(sosReportDir.resolve(fileNameIoExc), exceptionData);
                reportTypes.add(new SosFileType(fileNameIoExc, false, now));
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

    /**
     * Basically performs a java-based <code>'rm -rf ..../linstor.d/$sosReportNameRef'</code>
     *
     * @param sosReportNameRef
     */
    public void handleSosReportCleanup(String sosReportNameRef)
    {
        Path sosReportDir = getSosReportDir(sosReportNameRef);
        try
        {
            FileUtils.deleteDirectoryWithContent(sosReportDir, errorReporter);
        }
        catch (IOException exc)
        {
            errorReporter.reportError(exc);
        }
    }
}
