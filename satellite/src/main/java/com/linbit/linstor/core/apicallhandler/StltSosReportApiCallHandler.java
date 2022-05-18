package com.linbit.linstor.core.apicallhandler;

import com.linbit.ImplementationError;
import com.linbit.extproc.DaemonHandler;
import com.linbit.extproc.OutputProxy.EOFEvent;
import com.linbit.extproc.OutputProxy.Event;
import com.linbit.extproc.OutputProxy.ExceptionEvent;
import com.linbit.extproc.OutputProxy.StdErrEvent;
import com.linbit.extproc.OutputProxy.StdOutEvent;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.SosReportType;
import com.linbit.linstor.SosReportType.SosCommandType;
import com.linbit.linstor.SosReportType.SosFileType;
import com.linbit.linstor.SosReportType.SosInfoType;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.cfg.StltConfig;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.utils.FileCollector;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Stream;

@Singleton
public class StltSosReportApiCallHandler
{
    private static final int DFLT_DEQUE_CAPACITY = 10;
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
     * Collects a list of local reports and sends them to the controller. <br />
     * Collected reports:
     * <ul>
     * <li>test</li>
     * </ul>
     *
     * @param since
     */
    public void handleSosReportRequest(Date since)
    {
        Set<SosReportType> reports = listSosReport(since);

        final int splitAfterBytes = 10 * 1024 * 1024; // 10MiB

        for (SosReportType report : reports)
        {
            String fileName = report.getRelativeFileName();
            long now = report.getTimestamp();
            InputStream is;
            if ((report instanceof SosReportType.SosFileType) || (report instanceof SosReportType.SosInfoType))
            {
                if (report instanceof SosReportType.SosInfoType)
                {
                    is = new ByteArrayInputStream(((SosInfoType) report).getInfo().getBytes());
                }
                else if (report instanceof SosReportType.SosFileType)
                {
                    try
                    {
                        is = new FileInputStream(((SosFileType) report).getPath().toFile());
                    }
                    catch (FileNotFoundException notFoundExc)
                    {
                        is = new ByteArrayInputStream(new byte[0]);
                        fileName += ".file_not_found";
                    }
                }
                else
                {
                    throw new ImplementationError("Unknown SosReportType: " + report.getClass().getCanonicalName());
                }

                sendReportByInputStream(splitAfterBytes, fileName, is, now);
            }
            else if (report instanceof SosReportType.SosCommandType)
            {
                SosCommandType sosCommandType = (SosCommandType) report;
                String[] command = sosCommandType.getCommand();

                // limits the deque to max 10MiB of contents of the events in the deque
                BlockingDeque<Event> deque = new LinkedBlockingDeque<>(DFLT_DEQUE_CAPACITY);
                DaemonHandler dh = new DaemonHandler(deque, command);
                try
                {
                    dh.startUndelimited();
                    sendReportByOutputProxyDeque(fileName, splitAfterBytes, deque, now);
                }
                catch (IOException exc)
                {
                    exc.printStackTrace();
                }
            }
        }
    }

    private void sendReportByInputStream(
        final int splitAfterBytes,
        final String fileName,
        final InputStream is,
        final long timepstampRef
    )
    {
        int offset = 0; // global offset so the controller knows which offset to append the attached data within the
                        // target file
        byte[] buffer = new byte[splitAfterBytes]; // the data (-chunk) to send
        int bytesToSend = 0; // the used length of buffer (everything after should be considered garbage)
        int lastRead; // byte count of the last inputstream.read(..) call

        boolean keepReading = true;
        try
        {
            do
            {
                lastRead = is.read(buffer, bytesToSend, splitAfterBytes - bytesToSend);
                keepReading = lastRead >= 0;
                if (keepReading)
                {
                    bytesToSend += lastRead;
                    offset += splitAfterBytes - bytesToSend;
                }
                if (bytesToSend == splitAfterBytes)
                {
                    sendReport(fileName, timepstampRef, offset, bytesToSend, buffer, !keepReading);
                    bytesToSend = 0;
                }
            }
            while (keepReading);
            if (bytesToSend > 0)
            {
                sendReport(fileName, timepstampRef, offset, bytesToSend, buffer, !keepReading);
            }
        }
        catch (IOException exc)
        {
            StringBuilder errMsgBuilder = new StringBuilder(exc.getLocalizedMessage()).append("\n\n");

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            exc.printStackTrace(pw);

            errMsgBuilder.append(sw.getBuffer());

            byte[] errMsgData = errMsgBuilder.toString().getBytes();

            sendReport(fileName + ".ioexc", timepstampRef, 0, errMsgData.length, errMsgData, true);
        }
    }

    private void sendReportByOutputProxyDeque(
        final String fileName,
        final int splitAfterBytes,
        final BlockingDeque<Event> deque,
        final long timepstampRef
    )
    {
        boolean running = true;
        DequeDataHolder stdOutHolder = new DequeDataHolder(fileName, timepstampRef, splitAfterBytes, true);
        DequeDataHolder stdErrHolder = new DequeDataHolder(fileName + ".err", timepstampRef, splitAfterBytes, false);

        while (running)
        {
            Event event;
            try
            {
                event = deque.take();

                if (event instanceof StdOutEvent)
                {
                    StdOutEvent stdOutEvent = (StdOutEvent) event;
                    stdOutHolder.consumeEvent(stdOutEvent.data);
                }
                else if (event instanceof StdErrEvent)
                {
                    StdErrEvent stdErrEvent = (StdErrEvent) event;
                    stdErrHolder.consumeEvent(stdErrEvent.data);
                }
                else if (event instanceof EOFEvent)
                {
                    running = false;
                    stdOutHolder.flush(true);
                    stdErrHolder.flush(true);
                }
                else if (event instanceof ExceptionEvent)
                {
                    running = false;
                }
            }
            catch (InterruptedException exc)
            {
                running = false;
                byte[] data = "Interrupted".getBytes();
                stdOutHolder.consumeEvent(data);
                stdOutHolder.flush(true);

                Thread.currentThread().interrupt();
            }
        }
    }

    private class DequeDataHolder
    {
        private final long timestamp;
        private final String relativeFileName;

        private final boolean sendEmptyFile;
        private final byte[] buffer;

        private int offset;

        private int targetOffset;

        private boolean sent = false;

        private DequeDataHolder(
            String relativeFileNameRef,
            long timestampRef,
            int bufferSize,
            boolean sendEmptyFileRef
        )
        {
            relativeFileName = relativeFileNameRef;
            timestamp = timestampRef;
            sendEmptyFile = sendEmptyFileRef;
            buffer = new byte[bufferSize];
            offset = 0;
            targetOffset = 0;
        }

        private void consumeEvent(
            byte[] eventDataRef
        )
        {
            int bytesNotCopied = eventDataRef.length;
            do
            {
                bytesNotCopied = copy(eventDataRef, eventDataRef.length - bytesNotCopied);

                if (offset == buffer.length)
                {
                    flush(false);
                    offset = 0;
                    targetOffset += buffer.length;
                }
            }
            while (bytesNotCopied > 0);
        }

        public int copy(byte[] data, int dataOffset)
        {
            int len = Math.min(data.length, buffer.length - offset);
            System.arraycopy(data, dataOffset, buffer, offset, len);
            offset += len;
            return data.length - len;
        }

        public void flush(boolean eof)
        {
            System.out.println(
                "flushing " + relativeFileName + ": eof: " + eof + ", offset: " + offset + ", sent: " + sent +
                    ", sendEmptyFile: " + sendEmptyFile
            );
            if (offset > 0 || (!sent && sendEmptyFile))
            {
                sendReport(relativeFileName, timestamp, targetOffset, offset, buffer, eof);
                sent = true;
            }
        }
    }

    private void sendReport(
        String relativeFileNameRef,
        long timestampRef,
        int offsetRef,
        int bytesToSendRef,
        byte[] dataRef,
        boolean eofRef
    )
    {
        byte[] trimmedData;
        if (bytesToSendRef == dataRef.length)
        {
            trimmedData = dataRef;
        }
        else
        {
            trimmedData = new byte[bytesToSendRef];
            System.arraycopy(dataRef, 0, trimmedData, 0, bytesToSendRef);
        }
        errorReporter.logTrace(
            "Sending %d bytes for relative file: %s %s",
            trimmedData.length,
            relativeFileNameRef,
            eofRef ? " (EOF)" : ""
        );
        controllerPeerConnector.getControllerPeer().sendMessage(
            interComSerializer.answerBuilder(InternalApiConsts.API_RSP_SOS_REPORT, apiCallId.get())
                .sosReport(
                    controllerPeerConnector.getLocalNodeName().displayValue,
                    relativeFileNameRef,
                    timestampRef,
                    offsetRef,
                    trimmedData,
                    eofRef
                )
                .build(),
            InternalApiConsts.API_RSP_SOS_REPORT
        );
    }
    private Set<SosReportType> listSosReport(Date since)
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
                StltConfig.LINSTOR_STLT_CONFIG,
                now,
                "cat",
                stltCfg.getConfigDir() + StltConfig.LINSTOR_STLT_CONFIG
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

        reportTypes.add(
            new SosInfoType(
                "linstorInfo",
                now,
                LinStor.linstorInfo() + "\n\nuname -a:           " + LinStor.getUname("-a")
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
        catch (IOException ignored)
        {
        }
        return reportTypes;
    }
}
