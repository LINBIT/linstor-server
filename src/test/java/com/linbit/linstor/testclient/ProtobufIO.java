package com.linbit.linstor.testclient;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiRcUtils;
import com.linbit.linstor.proto.MsgHeaderOuterClass.MsgHeader;
import com.linbit.linstor.proto.common.ApiCallResponseOuterClass.ApiCallResponse;

import static com.linbit.linstor.api.ApiConsts.MASK_ERROR;
import static com.linbit.linstor.api.ApiConsts.MASK_INFO;
import static com.linbit.linstor.api.ApiConsts.MASK_WARN;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import com.google.protobuf.MessageLite;

public class ProtobufIO
{
    public static final Object CALLBACK_LOCK = new Object();

    public interface MessageCallback
    {
        void error(long apiCallId, long retCode, String message, String cause, String correction,
            String details, Map<String, String> objRefsMap);
        void warn(long apiCallId, long retCode, String message, String cause, String correction,
            String details, Map<String, String> objRefsMap);
        void info(long apiCallId, long retCode, String message, String cause, String correction,
            String details, Map<String, String> objRefsMap);
        void success(long apiCallId, long retCode, String message, String cause, String correction,
            String details, Map<String, String> objRefsMap);
    }

    protected Socket sock;
    protected InputStream inputStream;
    protected OutputStream outputStream;

    protected Thread thread;
    protected boolean shutdown;

    private int sentCount;
    private int successCount;
    private int infoCount;
    private int warnCount;
    private int errorCount;
    private PrintStream outStream;

    private List<MessageCallback> callbacks = new ArrayList<>();

    protected AtomicLong apiCallIdGen = new AtomicLong(0);


    public ProtobufIO(
        String host,
        int port,
        PrintStream out,
        String threadName
    )
        throws UnknownHostException, IOException
    {
        outStream = out;
        sock = new Socket(host, port);
        inputStream = sock.getInputStream();
        outputStream = sock.getOutputStream();
        resetAllCounts();

        shutdown = false;
        thread = new Thread(new ProtobufIOWorker(), threadName);
        thread.start();
    }

    public long send(String apiCall, MessageLite msg) throws IOException
    {
        return send(
            getNextApiCallId(),
            apiCall,
            msg
        );
    }

    public long send(long apiCallIdRef, String apiCallRef, MessageLite... messages) throws IOException
    {
        MsgHeader headerMsg = MsgHeader.newBuilder()
            .setMsgType(MsgHeader.MsgType.API_CALL)
            .setMsgContent(apiCallRef)
            .setApiCallId(apiCallIdRef)
            .build();

        ByteArrayOutputStream baos;
        baos = new ByteArrayOutputStream();
        headerMsg.writeDelimitedTo(baos);

        for (MessageLite msg : messages)
        {
            msg.writeDelimitedTo(baos);
        }
        byte[] protoData = baos.toByteArray();
        baos.close();

        send(protoData);

        return apiCallIdRef;
    }

    @SuppressWarnings("checkstyle:magicnumber")
    public void send(byte[] data) throws IOException
    {
        byte[] header = new byte[16];
        ByteBuffer byteBuffer = ByteBuffer.wrap(header);
        byteBuffer.putInt(0, 0); // message type
        byteBuffer.putInt(4, data.length);

        outputStream.write(header);
        outputStream.write(data);

        sentCount++;
    }

    public void setPrintStream(PrintStream printStream)
    {
        outStream = printStream;
    }

    public void resetAllCounts()
    {
        resetSuccessCount();
        resetInfoCount();
        resetWarnCount();
        resetErrorCount();
    }

    public void resetSentCount()
    {
        sentCount = 0;
    }

    public void resetSuccessCount()
    {
        successCount = 0;
    }

    public void resetInfoCount()
    {
        infoCount = 0;
    }

    public void resetWarnCount()
    {
        warnCount = 0;
    }

    public void resetErrorCount()
    {
        errorCount = 0;
    }

    public void registerCallback(MessageCallback callback)
    {
        callbacks.add(callback);
    }

    public void unregisterCallback(MessageCallback callback)
    {
        callbacks.remove(callback);
    }

    public long getNextApiCallId()
    {
        return apiCallIdGen.incrementAndGet();
    }

    public void formatMessage(
        StringBuilder sbRef,
        long apiCallId,
        int responseIdx,
        long retCode,
        String message,
        String cause,
        String correction,
        String details,
        Map<String, String> objRefsMap
    )
    {
        StringBuilder sb;
        if (sbRef == null)
        {
            sb = new StringBuilder();
        }
        else
        {
            sb = sbRef;
        }
        sb.append("ApiCallId: ")
            .append(apiCallId)
            .append("\n")
            .append("   Response ")
            .append(responseIdx)
            .append(": \n      RetCode   : ");
        decodeRetValue(sb, retCode);
        if (message != null && !"".equals(message))
        {
            sb.append("\n      Message   : ").append(format(message));
        }
        if (details != null && !"".equals(details))
        {
            sb.append("\n      Details   : ").append(format(details));
        }
        if (cause != null && !"".equals(cause))
        {
            sb.append("\n      Cause     : ").append(format(cause));
        }
        if (correction != null && !"".equals(correction))
        {
            sb.append("\n      Correction: ").append(format(correction));
        }
        if (objRefsMap != null && !objRefsMap.isEmpty())
        {
            sb.append("\n      ObjRefs: ");
            for (Entry<String, String> entry : objRefsMap.entrySet())
            {
                sb.append("\n         ").append(entry.getKey()).append("=").append(entry.getValue());
            }
        }
        else
        {
            if (retCode == ApiConsts.UNKNOWN_API_CALL)
            {
                sb.append("\n      No ObjRefs");
            }
            else
            {
                sb.append("\n      No ObjRefs defined! Report this to the dev - this should not happen!");
            }

        }
    }

    private String format(String msg)
    {
        return msg.replaceAll("\\n", "\n               ");
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private void decodeRetValue(StringBuilder sb, long retCode)
    {
        if ((retCode & MASK_ERROR) == MASK_ERROR)
        {
            errorCount++;
        }
        else
        if ((retCode & MASK_WARN) == MASK_WARN)
        {
            warnCount++;
        }
        else
        if ((retCode & MASK_INFO) == MASK_INFO)
        {
            infoCount++;
        }
        else
        {
            successCount++;
        }

        ApiRcUtils.appendReadableRetCode(sb, retCode);
        if ((retCode & 0x00FFFFFFFFFFFFFFL) == 0)
        {
            sb.append(" This looks wrong - please report to developer");
        }
    }

    private void callback(
        long apiCallId,
        long retCode,
        String message,
        String cause,
        String correction,
        String details,
        Map<String, String> objRefsMap
    )
    {
        synchronized (CALLBACK_LOCK)
        {
            if ((retCode & MASK_ERROR) == MASK_ERROR)
            {
                for (MessageCallback cb : callbacks)
                {
                    cb.error(apiCallId, retCode, message, cause, correction, details, objRefsMap);
                }
            }
            else
            if ((retCode & MASK_WARN) == MASK_WARN)
            {
                for (MessageCallback cb : callbacks)
                {
                    cb.warn(apiCallId, retCode, message, cause, correction, details, objRefsMap);
                }
            }
            else
            if ((retCode & MASK_INFO) == MASK_INFO)
            {
                for (MessageCallback cb : callbacks)
                {
                    cb.info(apiCallId, retCode, message, cause, correction, details, objRefsMap);
                }
            }
            else
            {
                for (MessageCallback cb : callbacks)
                {
                    cb.success(apiCallId, retCode, message, cause, correction, details, objRefsMap);
                }
            }
        }
    }

    private void println(String str)
    {
        if (outStream != null)
        {
            outStream.println(str);
        }
    }

    public void shutdown() throws IOException
    {
        shutdown = true;
        sock.close();
        thread.interrupt();
    }

    private class ProtobufIOWorker implements Runnable
    {
        @Override
        @SuppressWarnings({"DescendantToken", "checkstyle:magicnumber"})
        public void run()
        {
            StringBuilder sb = new StringBuilder();
            byte[] header = new byte[16];
            int read;
            int offset = 0;
            int protoLen;
            while (!shutdown)
            {
                try
                {
                    offset = 0;
                    while (offset != header.length)
                    {
                        read = inputStream.read(header, offset, header.length - offset);
                        if (read == -1)
                        {
                            return; // not very clean, but enough for this prototype
                        }
                        offset += read;
                    }

                    protoLen = (header[4] & 0xFF) << 24 |
                               (header[5] & 0xFF) << 16 |
                               (header[6] & 0xFF) << 8  |
                               (header[7] & 0xFF);
                    offset = 0;
                    byte[] data = new byte[protoLen];

                    while (offset != protoLen)
                    {
                        read = inputStream.read(data, offset, protoLen - offset);
                        if (read == -1)
                        {
                            return; // not very clean, but enough for this prototype
                        }
                        offset += read;
                    }

                    ByteArrayInputStream bais = new ByteArrayInputStream(data);

                    MsgHeader protoHeader = MsgHeader.parseDelimitedFrom(bais);

                    sb.setLength(0);
                    int responseIdx = 1;

                    String apiCall = protoHeader.getMsgContent();
                    if (bais.available() == 0)
                    {
                        // maybe a pong or a header-only answer
                        sb.append("ApiCallId: ")
                            .append(protoHeader.getApiCallId())
                            .append("\n")
                            .append(apiCall)
                            .append("\n");
                    }
                    if (!apiCall.equals(ApiConsts.API_VERSION))
                    {
                        while (bais.available() > 0)
                        {
                            ApiCallResponse response = ApiCallResponse.parseDelimitedFrom(bais);
                            long retCode = response.getRetCode();
                            String message = response.getMessage();
                            String cause = response.getCause();
                            String correction = response.getCorrection();
                            String details = response.getDetails();
                            Map<String, String> objRefsMap = response.getObjRefsMap();

                            callback(protoHeader.getApiCallId(), retCode, message, cause, correction,
                                details, objRefsMap);

                            formatMessage(
                                sb,
                                protoHeader.getApiCallId(),
                                responseIdx++,
                                retCode,
                                message,
                                cause,
                                correction,
                                details,
                                objRefsMap
                            );
                            sb.append("\n");
                        }
                        println(sb.toString());
                    }
                }
                catch (IOException ioExc)
                {
                    if (!shutdown)
                    {
                        ioExc.printStackTrace();
                    }
                }
            }
            println("clientprotobuf shutting down");

            println("");
            println("Sent messages: " + sentCount);
            println("Responses    : " + (infoCount + warnCount + errorCount + successCount));
            println("   success : " + successCount);
            println("   info    : " + infoCount);
            println("   warn    : " + warnCount);
            println("   error   : " + errorCount);
        }
    }
}
