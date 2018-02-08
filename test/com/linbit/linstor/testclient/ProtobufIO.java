package com.linbit.linstor.testclient;

import static com.linbit.linstor.api.ApiConsts.MASK_ERROR;
import static com.linbit.linstor.api.ApiConsts.MASK_INFO;
import static com.linbit.linstor.api.ApiConsts.MASK_WARN;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.proto.LinStorMapEntryOuterClass.LinStorMapEntry;
import com.linbit.linstor.proto.MsgApiCallResponseOuterClass.MsgApiCallResponse;
import com.linbit.linstor.proto.MsgHeaderOuterClass.MsgHeader;
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
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.Message;

public class ProtobufIO
{
    public static final Object callbackLock = new Object();

    public static interface MessageCallback
    {
        public void error(int msgId, long retCode, String message, String cause, String correction,
            String details, Map<String, String> objRefsMap, Map<String, String> variablesMap);
        public void warn(int msgId, long retCode, String message, String cause, String correction,
            String details, Map<String, String> objRefsMap, Map<String, String> variablesMap);
        public void info(int msgId, long retCode, String message, String cause, String correction,
            String details, Map<String, String> objRefsMap, Map<String, String> variablesMap);
        public void success(int msgId, long retCode, String message, String cause, String correction,
            String details, Map<String, String> objRefsMap, Map<String, String> variablesMap);
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

    protected AtomicInteger msgId = new AtomicInteger(0);


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

    public int send(String apiCall, Message msg) throws IOException
    {
        return send(
            getNextMsgId(),
            apiCall,
            msg
        );
    }

    public int send(int msgId, String apiCall, Message... messages) throws IOException
    {
        MsgHeader headerMsg = MsgHeader.newBuilder().
            setApiCall(apiCall).
            setMsgId(msgId).
            build();

        ByteArrayOutputStream baos;
        baos = new ByteArrayOutputStream();
        headerMsg.writeDelimitedTo(baos);

        for (Message msg : messages)
        {
            msg.writeDelimitedTo(baos);
        }
        byte[] protoData = baos.toByteArray();
        baos.close();

        send(protoData);

        return msgId;
    }

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

    public int getNextMsgId()
    {
        return msgId.incrementAndGet();
    }

    public void formatMessage(
        StringBuilder sb,
        int msgId,
        int responseIdx,
        long retCode,
        String message,
        String cause,
        String correction,
        String details,
        Map<String, String> objRefsMap,
        Map<String, String> variablesMap
    )
    {
        if (sb == null)
        {
            sb = new StringBuilder();
        }
        sb.append("MsgId: ")
            .append(msgId)
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
        if (variablesMap != null && !variablesMap.isEmpty())
        {
            sb.append("\n      Variables: ");
            for (Entry<String, String> entry : variablesMap.entrySet())
            {
                sb.append("\n         ").append(entry.getKey()).append("=").append(entry.getValue());
            }
        }
        else
        {
            sb.append("\n      No Variables");
        }
    }

    private String format(String msg)
    {
        return msg.replaceAll("\\n", "\n               ");
    }

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

        ApiRCUtils.appendReadableRetCode(sb, retCode);
        if ((retCode & 0x00FFFFFFFFFFFFFFL) == 0)
        {
            sb.append(" This looks wrong - please report to developer");
        }
    }

    private void callback(
        int msgId,
        long retCode,
        String message,
        String cause,
        String correction,
        String details,
        Map<String, String> objRefsMap,
        Map<String, String> variablesMap
    )
    {
        synchronized (callbackLock)
        {
            if ((retCode & MASK_ERROR) == MASK_ERROR)
            {
                for (MessageCallback cb : callbacks)
                {
                    cb.error(msgId, retCode, message, cause, correction, details, objRefsMap, variablesMap);
                }
            }
            else
            if ((retCode & MASK_WARN) == MASK_WARN)
            {
                for (MessageCallback cb : callbacks)
                {
                    cb.warn(msgId, retCode, message, cause, correction, details, objRefsMap, variablesMap);
                }
            }
            else
            if ((retCode & MASK_INFO) == MASK_INFO)
            {
                for (MessageCallback cb : callbacks)
                {
                    cb.info(msgId, retCode, message, cause, correction, details, objRefsMap, variablesMap);
                }
            }
            else
            {
                for (MessageCallback cb : callbacks)
                {
                    cb.success(msgId, retCode, message, cause, correction, details, objRefsMap, variablesMap);
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


    protected Iterable<LinStorMapEntry> asLinStorMapEntryList(Map<String, String> map)
    {
        List<LinStorMapEntry> list = new ArrayList<>(map.size());
        for (Map.Entry<String, String> entry : map.entrySet())
        {
            list.add(
                LinStorMapEntry.newBuilder()
                    .setKey(entry.getKey())
                    .setValue(entry.getValue())
                    .build()
            );
        }
        return list;
    }

    protected Map<String, String> asMap(List<LinStorMapEntry> list)
    {
        Map<String, String> map = new TreeMap<>();
        for (LinStorMapEntry entry : list)
        {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
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
        @SuppressWarnings("DescendantToken")
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
                            println("End of stream.");
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
                            println("End of stream.");
                            return; // not very clean, but enough for this prototype
                        }
                        offset += read;
                    }

                    ByteArrayInputStream bais = new ByteArrayInputStream(data);

                    MsgHeader protoHeader = MsgHeader.parseDelimitedFrom(bais);

                    sb.setLength(0);
                    int responseIdx = 1;

                    String apiCall = protoHeader.getApiCall();
                    if (bais.available() == 0)
                    {
                        // maybe a pong or a header-only answer
                        sb.append("MsgId: ")
                            .append(protoHeader.getMsgId())
                            .append("\n")
                            .append(apiCall)
                            .append("\n");
                    }
                    if (!apiCall.equals(ApiConsts.API_VERSION))
                    {
                        while (bais.available() > 0)
                        {
                            MsgApiCallResponse response = MsgApiCallResponse.parseDelimitedFrom(bais);
                            long retCode = response.getRetCode();
                            String message = response.getMessageFormat();
                            String cause = response.getCauseFormat();
                            String correction = response.getCorrectionFormat();
                            String details = response.getDetailsFormat();
                            Map<String, String> objRefsMap = asMap(response.getObjRefsList());
                            Map<String, String> variablesMap = asMap(response.getVariablesList());

                            callback(protoHeader.getMsgId(), retCode, message, cause, correction,
                                details, objRefsMap, variablesMap);

                            formatMessage(
                                sb,
                                protoHeader.getMsgId(),
                                responseIdx++,
                                retCode,
                                message,
                                cause,
                                correction,
                                details,
                                objRefsMap,
                                variablesMap
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
