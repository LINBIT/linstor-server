package com.linbit.linstor.testclient;

import static com.linbit.linstor.api.ApiConsts.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.Message;
import com.linbit.linstor.NetInterface;
import com.linbit.linstor.NetInterface.NetInterfaceApi;
import com.linbit.linstor.SatelliteConnection.SatelliteConnectionApi;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.NetInterfacePojo;
import com.linbit.linstor.api.pojo.SatelliteConnectionPojo;
import com.linbit.linstor.proto.LinStorMapEntryOuterClass.LinStorMapEntry;
import com.linbit.linstor.proto.MsgApiCallResponseOuterClass.MsgApiCallResponse;
import com.linbit.linstor.proto.MsgCrtNodeConnOuterClass.MsgCrtNodeConn;
import com.linbit.linstor.proto.MsgCrtNodeOuterClass.MsgCrtNode;
import com.linbit.linstor.proto.MsgCrtRscConnOuterClass.MsgCrtRscConn;
import com.linbit.linstor.proto.MsgCrtRscDfnOuterClass.MsgCrtRscDfn;
import com.linbit.linstor.proto.MsgCrtRscOuterClass.MsgCrtRsc;
import com.linbit.linstor.proto.MsgCrtStorPoolDfnOuterClass.MsgCrtStorPoolDfn;
import com.linbit.linstor.proto.MsgCrtStorPoolOuterClass.MsgCrtStorPool;
import com.linbit.linstor.proto.MsgCrtVlmConnOuterClass.MsgCrtVlmConn;
import com.linbit.linstor.proto.MsgCrtVlmDfnOuterClass.MsgCrtVlmDfn;
import com.linbit.linstor.proto.MsgDelNodeConnOuterClass.MsgDelNodeConn;
import com.linbit.linstor.proto.MsgDelNodeOuterClass.MsgDelNode;
import com.linbit.linstor.proto.MsgDelRscConnOuterClass.MsgDelRscConn;
import com.linbit.linstor.proto.MsgDelRscDfnOuterClass.MsgDelRscDfn;
import com.linbit.linstor.proto.MsgDelRscOuterClass.MsgDelRsc;
import com.linbit.linstor.proto.MsgDelStorPoolDfnOuterClass.MsgDelStorPoolDfn;
import com.linbit.linstor.proto.MsgDelStorPoolOuterClass.MsgDelStorPool;
import com.linbit.linstor.proto.MsgDelVlmConnOuterClass.MsgDelVlmConn;
import com.linbit.linstor.proto.MsgHeaderOuterClass.MsgHeader;
import com.linbit.linstor.proto.MsgModNodeConnOuterClass.MsgModNodeConn;
import com.linbit.linstor.proto.MsgModNodeOuterClass.MsgModNode;
import com.linbit.linstor.proto.MsgModRscConnOuterClass.MsgModRscConn;
import com.linbit.linstor.proto.MsgModRscDfnOuterClass.MsgModRscDfn;
import com.linbit.linstor.proto.MsgModRscOuterClass.MsgModRsc;
import com.linbit.linstor.proto.MsgModStorPoolDfnOuterClass.MsgModStorPoolDfn;
import com.linbit.linstor.proto.MsgModStorPoolOuterClass.MsgModStorPool;
import com.linbit.linstor.proto.MsgModVlmConnOuterClass.MsgModVlmConn;
import com.linbit.linstor.proto.MsgModVlmDfnOuterClass.MsgModVlmDfn;
import com.linbit.linstor.proto.NetInterfaceOuterClass;
import com.linbit.linstor.proto.NodeOuterClass;
import com.linbit.linstor.proto.SatelliteConnectionOuterClass;
import com.linbit.linstor.proto.VlmDfnOuterClass.VlmDfn;
import com.linbit.linstor.proto.VlmOuterClass.Vlm;

public class ClientProtobuf implements Runnable
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

    public static final Map<Long, String> RET_CODES_TYPE;
    public static final Map<Long, String> RET_CODES_OP;
    public static final Map<Long, String> RET_CODES_OBJ;
    public static final Map<Long, String> RET_CODES_ACTION;

    static
    {
        HashMap<Long, String> tmpMap = new HashMap<>();
        tmpMap.put(MASK_ERROR, "Error");
        tmpMap.put(MASK_WARN, "Warn");
        tmpMap.put(MASK_INFO, "Info");
        tmpMap.put(MASK_SUCCESS, "Success");
        RET_CODES_TYPE = Collections.unmodifiableMap(tmpMap);

        tmpMap = new HashMap<>();
        tmpMap.put(MASK_CRT, "Create");
        tmpMap.put(MASK_MOD, "Modify");
        tmpMap.put(MASK_DEL, "Delete");
        RET_CODES_OP = Collections.unmodifiableMap(tmpMap);

        tmpMap = new HashMap<>();
        tmpMap.put(MASK_NODE, "Node");
        tmpMap.put(MASK_RSC_DFN, "RscDfn");
        tmpMap.put(MASK_RSC, "Rsc");
        tmpMap.put(MASK_VLM_DFN, "VlmDfn");
        tmpMap.put(MASK_VLM, "Vlm");
        tmpMap.put(MASK_STOR_POOL_DFN, "StorPoolDfn");
        tmpMap.put(MASK_STOR_POOL, "StorPool");
        tmpMap.put(MASK_NODE_CONN, "NodeConn");
        tmpMap.put(MASK_RSC_CONN, "RscConn");
        tmpMap.put(MASK_VLM_CONN, "VlmConn");
        tmpMap.put(MASK_NET_IF, "NetIf");
        RET_CODES_OBJ = Collections.unmodifiableMap(tmpMap);

        tmpMap = new HashMap<>();
        tmpMap.put(CREATED, "created" );
        tmpMap.put(DELETED, "deleted");
        tmpMap.put(MODIFIED, "modified");

        tmpMap.put(FAIL_SQL, "FAIL_SQL");
        tmpMap.put(FAIL_SQL_ROLLBACK, "FAIL_SQL_ROLLBACK");
        tmpMap.put(FAIL_INVLD_NODE_NAME, "FAIL_INVLD_NODE_NAME");
        tmpMap.put(FAIL_INVLD_NODE_TYPE, "FAIL_INVLD_NODE_TYPE");
        tmpMap.put(FAIL_INVLD_RSC_NAME, "FAIL_INVLD_RSC_NAME");
        tmpMap.put(FAIL_INVLD_RSC_PORT, "FAIL_INVLD_RSC_PORT");
        tmpMap.put(FAIL_INVLD_NODE_ID, "FAIL_INVLD_NODE_ID");
        tmpMap.put(FAIL_INVLD_VLM_NR, "FAIL_INVLD_VLM_NR");
        tmpMap.put(FAIL_INVLD_VLM_SIZE, "FAIL_INVLD_VLM_SIZE");
        tmpMap.put(FAIL_INVLD_MINOR_NR, "FAIL_INVLD_MINOR_NR");
        tmpMap.put(FAIL_INVLD_STOR_POOL_NAME, "FAIL_INVLD_STOR_POOL_NAME");
        tmpMap.put(FAIL_INVLD_NET_NAME, "FAIL_INVLD_NET_NAME");
        tmpMap.put(FAIL_INVLD_NET_ADDR, "FAIL_INVLD_NET_ADDR");
        tmpMap.put(FAIL_INVLD_NET_PORT, "FAIL_INVLD_NET_PORT");
        tmpMap.put(FAIL_INVLD_NET_TYPE, "FAIL_INVLD_NET_TYPE");
        tmpMap.put(FAIL_INVLD_PROP, "FAIL_INVLD_PROP");
        tmpMap.put(FAIL_NOT_FOUND_NODE, "FAIL_NOT_FOUND_NODE");
        tmpMap.put(FAIL_NOT_FOUND_RSC_DFN, "FAIL_NOT_FOUND_RSC_DFN");
        tmpMap.put(FAIL_NOT_FOUND_RSC, "FAIL_NOT_FOUND_RSC");
        tmpMap.put(FAIL_NOT_FOUND_VLM_DFN, "FAIL_NOT_FOUND_VLM_DFN");
        tmpMap.put(FAIL_NOT_FOUND_VLM, "FAIL_NOT_FOUND_VLM");
        tmpMap.put(FAIL_NOT_FOUND_NET_IF, "FAIL_NOT_FOUND_NET_IF");
        tmpMap.put(FAIL_NOT_FOUND_NODE_CONN, "FAIL_NOT_FOUND_NODE_CONN");
        tmpMap.put(FAIL_NOT_FOUND_RSC_CONN, "FAIL_NOT_FOUND_RSC_CONN");
        tmpMap.put(FAIL_NOT_FOUND_VLM_CONN, "FAIL_NOT_FOUND_VLM_CONN");
        tmpMap.put(FAIL_NOT_FOUND_STOR_POOL_DFN, "FAIL_NOT_FOUND_STOR_POOL_DFN");
        tmpMap.put(FAIL_NOT_FOUND_STOR_POOL, "FAIL_NOT_FOUND_STOR_POOL");
        tmpMap.put(FAIL_NOT_FOUND_DFLT_STOR_POOL, "FAIL_NOT_FOUND_DFLT_STOR_POOL");
        tmpMap.put(FAIL_ACC_DENIED_NODE, "FAIL_ACC_DENIED_NODE");
        tmpMap.put(FAIL_ACC_DENIED_RSC_DFN, "FAIL_ACC_DENIED_RSC_DFN");
        tmpMap.put(FAIL_ACC_DENIED_RSC, "FAIL_ACC_DENIED_RSC");
        tmpMap.put(FAIL_ACC_DENIED_VLM_DFN, "FAIL_ACC_DENIED_VLM_DFN");
        tmpMap.put(FAIL_ACC_DENIED_VLM, "FAIL_ACC_DENIED_VLM");
        tmpMap.put(FAIL_ACC_DENIED_STOR_POOL_DFN, "FAIL_ACC_DENIED_STOR_POOL_DFN");
        tmpMap.put(FAIL_ACC_DENIED_STOR_POOL, "FAIL_ACC_DENIED_STOR_POOL");
        tmpMap.put(FAIL_ACC_DENIED_NODE_CONN, "FAIL_ACC_DENIED_NODE_CONN");
        tmpMap.put(FAIL_ACC_DENIED_RSC_CONN, "FAIL_ACC_DENIED_RSC_CONN");
        tmpMap.put(FAIL_ACC_DENIED_VLM_CONN, "FAIL_ACC_DENIED_VLM_CONN");
        tmpMap.put(FAIL_ACC_DENIED_STLT_CONN, "FAIL_ACC_DENIED_STLT_CONN");
        tmpMap.put(FAIL_EXISTS_NODE, "FAIL_EXISTS_NODE");
        tmpMap.put(FAIL_EXISTS_RSC_DFN, "FAIL_EXISTS_RSC_DFN");
        tmpMap.put(FAIL_EXISTS_RSC, "FAIL_EXISTS_RSC");
        tmpMap.put(FAIL_EXISTS_VLM_DFN, "FAIL_EXISTS_VLM_DFN");
        tmpMap.put(FAIL_EXISTS_VLM, "FAIL_EXISTS_VLM");
        tmpMap.put(FAIL_EXISTS_NET_IF, "FAIL_EXISTS_NET_IF");
        tmpMap.put(FAIL_EXISTS_NODE_CONN, "FAIL_EXISTS_NODE_CONN");
        tmpMap.put(FAIL_EXISTS_RSC_CONN, "FAIL_EXISTS_RSC_CONN");
        tmpMap.put(FAIL_EXISTS_VLM_CONN, "FAIL_EXISTS_VLM_CONN");
        tmpMap.put(FAIL_EXISTS_STOR_POOL_DFN, "FAIL_EXISTS_STOR_POOL_DFN");
        tmpMap.put(FAIL_EXISTS_STOR_POOL, "FAIL_EXISTS_STOR_POOL");
        tmpMap.put(FAIL_MISSING_PROPS, "FAIL_MISSING_PROPS");
        tmpMap.put(FAIL_MISSING_PROPS_NETCOM_TYPE, "FAIL_MISSING_PROPS_NETCOM_TYPE");
        tmpMap.put(FAIL_MISSING_PROPS_NETCOM_PORT, "FAIL_MISSING_PROPS_NETCOM_PORT");
        tmpMap.put(FAIL_MISSING_NETCOM, "FAIL_MISSING_NETCOM");
        tmpMap.put(FAIL_MISSING_PROPS_NETIF_NAME, "FAIL_MISSING_PROPS_NETIF_NAME");
        tmpMap.put(FAIL_MISSING_STLT_CONN, "FAIL_MISSING_STLT_CONN");
        tmpMap.put(FAIL_UUID_NODE, "FAIL_UUID_NODE");
        tmpMap.put(FAIL_UUID_RSC_DFN, "FAIL_UUID_RSC_DFN");
        tmpMap.put(FAIL_UUID_RSC, "FAIL_UUID_RSC");
        tmpMap.put(FAIL_UUID_VLM_DFN, "FAIL_UUID_VLM_DFN");
        tmpMap.put(FAIL_UUID_VLM, "FAIL_UUID_VLM");
        tmpMap.put(FAIL_UUID_NET_IF, "FAIL_UUID_NET_IF");
        tmpMap.put(FAIL_UUID_NODE_CONN, "FAIL_UUID_NODE_CONN");
        tmpMap.put(FAIL_UUID_RSC_CONN, "FAIL_UUID_RSC_CONN");
        tmpMap.put(FAIL_UUID_VLM_CONN, "FAIL_UUID_VLM_CONN");
        tmpMap.put(FAIL_UUID_STOR_POOL_DFN, "FAIL_UUID_STOR_POOL_DFN");
        tmpMap.put(FAIL_UUID_STOR_POOL, "FAIL_UUID_STOR_POOL");
        tmpMap.put(FAIL_IN_USE, "FAIL_IN_USE");
        tmpMap.put(FAIL_UNKNOWN_ERROR, "FAIL_UNKNOWN_ERROR");
        tmpMap.put(FAIL_IMPL_ERROR, "FAIL_IMPL_ERROR");
        tmpMap.put(WARN_INVLD_OPT_PROP_NETCOM_ENABLED, "WARN_INVLD_OPT_PROP_NETCOM_ENABLED");
        tmpMap.put(WARN_NOT_CONNECTED, "WARN_NOT_CONNECTED");
        tmpMap.put(WARN_NOT_FOUND, "WARN_NOT_FOUND");
        RET_CODES_ACTION = Collections.unmodifiableMap(tmpMap);
    }

    private Socket sock;
    private InputStream inputStream;
    private OutputStream outputStream;
    private AtomicInteger msgId = new AtomicInteger(0);

    private Thread thread;
    private boolean shutdown;

    private int sentCount;
    private int successCount;
    private int infoCount;
    private int warnCount;
    private int errorCount;
    private PrintStream outStream;

    private List<MessageCallback> callbacks = new ArrayList<>();

    public ClientProtobuf(int port) throws UnknownHostException, IOException
    {
        this("localhost", port, System.out);
    }

    public ClientProtobuf(String host, int port) throws UnknownHostException, IOException
    {
        this(host, port, System.out);
    }

    public ClientProtobuf(String host, int port, PrintStream out) throws UnknownHostException, IOException
    {
        outStream = out;
        sock = new Socket(host, port);
        inputStream = sock.getInputStream();
        outputStream = sock.getOutputStream();
        resetAllCounts();

        shutdown = false;
        thread = new Thread(this, "ClientProtobuf");
        thread.start();
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

    public void shutdown() throws IOException
    {
        shutdown = true;
        sock.close();
        thread.interrupt();
    }

    @Override
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
                if (bais.available() == 0)
                {
                    // maybe a pong or a header-only answer
                    sb.append("MsgId: ")
                        .append(protoHeader.getMsgId())
                        .append("\n")
                        .append(protoHeader.getApiCall())
                        .append("\n");
                }
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

    public void registerCallback(MessageCallback callback)
    {
        callbacks.add(callback);
    }

    public void unregisterCallback(MessageCallback callback)
    {
        callbacks.remove(callback);
    }

    private void callback(
        int msgId, long retCode, String message, String cause, String correction, String details,
        Map<String, String> objRefsMap, Map<String, String> variablesMap
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

        appendReadableRetCode(sb, retCode);
        if ((retCode & 0x00FFFFFFFFFFFFFFL) == 0)
        {
            sb.append(" This looks wrong - please report to developer");
        }
    }

    public static void appendReadableRetCode(StringBuilder sb, long retCode)
    {
        appendReadableRetCode(sb, retCode, 0xC000000000000000L, RET_CODES_TYPE);
        appendReadableRetCode(sb, retCode, 0x0000000003000000L, RET_CODES_OP);
        appendReadableRetCode(sb, retCode, 0x00000000003C0000L, RET_CODES_OBJ);
        appendReadableRetCode(sb, retCode, 0xC00000000000FFFFL, RET_CODES_ACTION);
        sb.append(Long.toHexString(retCode));
    }

    public static void appendReadableRetCode(
        StringBuilder sb,
        long retCode,
        long mask,
        Map<Long, String> retCodes
    )
    {
        for (Entry<Long, String> entry : retCodes.entrySet())
        {
            if ((retCode & mask) == entry.getKey())
            {
                sb.append(entry.getValue()).append(" ");
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

    public int sendPing() throws IOException
    {
        int msgId = this.msgId.incrementAndGet();
        send(msgId, API_PING, null);
        return msgId;
    }

    /*
     * Node messages
     */

    public int sendCreateNode(
        String nodeName,
        String nodeType,
        Map<String, String> props,
        List<? extends NetInterface.NetInterfaceApi> netIfs,
        List<? extends SatelliteConnectionApi> stltConns
    )
        throws IOException
    {
        int msgId = this.msgId.incrementAndGet();
        NodeOuterClass.Node.Builder nodeBuilder = NodeOuterClass.Node.newBuilder()
            .setName(nodeName)
            .setType(nodeType);
        if (props != null)
        {
            nodeBuilder.addAllProps(asLinStorMapEntryList(props));
        }

        for (NetInterfaceApi netIf : netIfs)
        {
            nodeBuilder.addNetInterfaces(
                NetInterfaceOuterClass.NetInterface.newBuilder()
                    .setName(netIf.getName())
                    .setAddress(netIf.getAddress())
                    .build()
            );
        }

        MsgCrtNode.Builder msgCrtNodeBuilder = MsgCrtNode.newBuilder();
        msgCrtNodeBuilder.setNode(nodeBuilder.build());

        for (SatelliteConnectionApi stltConnApi : stltConns)
        {
            msgCrtNodeBuilder.addSatelliteConnections(
                SatelliteConnectionOuterClass.SatelliteConnection.newBuilder()
                    .setNetInterfaceName(stltConnApi.getNetInterfaceName())
                    .setPort(stltConnApi.getPort())
                    .setEncryptionType(stltConnApi.getEncryptionType())
                    .build()
            );
        }

        send(
            msgId,
            API_CRT_NODE,
            msgCrtNodeBuilder.build()
        );
        return msgId;
    }

    public int sendModifyNode(
        UUID uuid,
        String nodeName,
        String typeName,
        Map<String, String> overrideProps,
        Set<String> delProps
    )
        throws IOException
    {
        int msgId = this.msgId.incrementAndGet();
        MsgModNode.Builder msgBuilder = MsgModNode.newBuilder()
            .setNodeName(nodeName);
        if (uuid != null)
        {
            msgBuilder.setNodeUuid(uuid.toString());
        }
        if (typeName != null)
        {
            msgBuilder.setNodeType(typeName);
        }
        if (overrideProps != null)
        {
            msgBuilder.addAllOverrideNodeProps(asLinStorMapEntryList(overrideProps));
        }
        if (delProps != null)
        {
            msgBuilder.addAllDeletePropKeys(delProps);
        }

        send(
            msgId,
            API_MOD_NODE,
            msgBuilder.build()
        );
        return msgId;
    }

    public int sendDeleteNode(String nodeName) throws IOException
    {
        int msgId = this.msgId.incrementAndGet();
        send(
            msgId,
            API_DEL_NODE,
            MsgDelNode.newBuilder().
                setNodeName(nodeName).
                build()
        );
        return msgId;
    }


    /*
     * Resource definition messages
     */


    public int sendCreateRscDfn(
        String resName,
        Integer port,
        Map<String, String> resDfnProps,
        Iterable<? extends VlmDfn> vlmDfn
    )
        throws IOException
    {
        int msgId = this.msgId.incrementAndGet();
        MsgCrtRscDfn.Builder msgBuilder = MsgCrtRscDfn.newBuilder().
            setRscName(resName);
        if (port != null)
        {
            msgBuilder.setRscPort(port);
        }
        if (resDfnProps != null)
        {
            msgBuilder.addAllRscProps(asLinStorMapEntryList(resDfnProps));
        }
        if (vlmDfn != null)
        {
            msgBuilder.addAllVlmDfns(vlmDfn);
        }
        send(
            msgId,
            API_CRT_RSC_DFN,
            msgBuilder.build()
        );
        return msgId;
    }

    public int sendModifyRscDfn(
        UUID rscDfnUuid,
        String rscName,
        Integer port,
        Map<String, String> overrideProps,
        Set<String> delProps
    )
        throws IOException
    {
        int msgId = this.msgId.incrementAndGet();
        MsgModRscDfn.Builder msgBuilder = MsgModRscDfn.newBuilder()
            .setRscName(rscName);
        if (rscDfnUuid != null)
        {
            msgBuilder.setRscDfnUuid(rscDfnUuid.toString());
        }
        if (port != null) {
            msgBuilder.setRscDfnPort(port);
        }
        if (overrideProps != null)
        {
            msgBuilder.addAllOverrideRscDfnProps(asLinStorMapEntryList(overrideProps));
        }
        if (delProps != null)
        {
            msgBuilder.addAllDeleteRscDfnPropKeys(delProps);
        }
        send(
            msgId,
            API_MOD_RSC_DFN,
            msgBuilder.build()
        );
        return msgId;
    }

    public int sendDeleteRscDfn(String resName) throws IOException
    {
        int msgId = this.msgId.incrementAndGet();
        send(
            msgId,
            API_DEL_RSC_DFN,
            MsgDelRscDfn.newBuilder().
                setRscName(resName).
                build()
        );
        return msgId;
    }

    /*
     * StorPoolDefinition messages
     */

    public int sendCreateStorPoolDfn(String storPoolName) throws IOException
    {
        int msgId = this.msgId.incrementAndGet();
        send(
            msgId,
            API_CRT_STOR_POOL_DFN,
            MsgCrtStorPoolDfn.newBuilder().
                setStorPoolName(storPoolName).
                build()
        );
        return msgId;
    }

    public int sendModifyStorPoolDfn(
        UUID storPoolUuid,
        String storPoolName,
        Map<String, String> overrideProps,
        Set<String> delPropKeys
    )
        throws IOException
    {
        int msgId = this.msgId.incrementAndGet();
        MsgModStorPoolDfn.Builder builder = MsgModStorPoolDfn.newBuilder()
            .setStorPoolName(storPoolName);
        if (storPoolUuid != null)
        {
            builder.setStorPoolDfnUuid(storPoolUuid.toString());
        }
        if (overrideProps != null)
        {
            builder.addAllOverrideProps(asLinStorMapEntryList(overrideProps));
        }
        if (delPropKeys != null)
        {
            builder.addAllDeletePropKeys(delPropKeys);
        }
        send(
            msgId,
            API_MOD_STOR_POOL_DFN,
            builder.build()
        );
        return msgId;
    }

    public int sendDeleteStorPoolDfn(String storPoolName) throws IOException
    {
        int msgId = this.msgId.incrementAndGet();
        send(
            msgId,
            API_DEL_STOR_POOL_DFN,
            MsgDelStorPoolDfn.newBuilder().
                setStorPoolName(storPoolName).
                build()
        );
        return msgId;
    }

    /*
     * StorPool messages
     */

    public int sendCreateStorPool(String nodeName, String storPoolName, String driver) throws IOException
    {
        int msgId = this.msgId.incrementAndGet();
        send(
            msgId,
            API_CRT_STOR_POOL,
            MsgCrtStorPool.newBuilder().
                setNodeName(nodeName).
                setStorPoolName(storPoolName).
                setDriver(driver).
                build()
        );
        return msgId;
    }

    public int sendModifyStorPool(
        UUID uuid,
        String nodeName,
        String storPoolName,
        Map<String, String> overrideProps,
        Set<String> delPropKeys
    )
        throws IOException
    {
        int msgId = this.msgId.incrementAndGet();

        MsgModStorPool.Builder builder = MsgModStorPool.newBuilder()
            .setNodeName(nodeName)
            .setStorPoolName(storPoolName);

        if (uuid != null)
        {
            builder.setStorPoolUuid(uuid.toString());
        }
        if (overrideProps != null)
        {
            builder.addAllOverrideProps(asLinStorMapEntryList(overrideProps));
        }
        if (delPropKeys != null)
        {
            builder.addAllDeletePropKeys(delPropKeys);
        }

        send(
            msgId,
            API_MOD_STOR_POOL,
            builder.build()
        );
        return msgId;
    }


    public int sendDeleteStorPool(String nodeName, String storPoolName) throws IOException
    {
        int msgId = this.msgId.incrementAndGet();
        send(
            msgId,
            API_DEL_STOR_POOL,
            MsgDelStorPool.newBuilder().
                setNodeName(nodeName).
                setStorPoolName(storPoolName).
                build()
        );
        return msgId;
    }

    /*
     * Resource messages
     */

    public int sendCreateRsc(
        String nodeName,
        String resName,
        Map<String, String> resProps,
        Iterable<? extends Vlm> vlms
    )
        throws IOException
    {
        int msgId = this.msgId.incrementAndGet();
        MsgCrtRsc.Builder msgBuilder = MsgCrtRsc.newBuilder().
            setNodeName(nodeName).
            setRscName(resName);
        if (resProps != null)
        {
            msgBuilder.addAllRscProps(asLinStorMapEntryList(resProps));
        }
        if (vlms != null)
        {
            msgBuilder.addAllVlms(vlms);
        }
        send(
            msgId,
            API_CRT_RSC,
            msgBuilder.build()
        );
        return msgId;
    }

    public int sendModifyRsc(
        UUID uuid,
        String nodeName,
        String rscName,
        Map<String, String> overrideProps,
        Set<String> delPropKeys
    )
        throws IOException
    {
        int msgId = this.msgId.incrementAndGet();
        MsgModRsc.Builder builder = MsgModRsc.newBuilder()
            .setNodeName(nodeName)
            .setRscName(rscName);

        if (uuid != null)
        {
            builder.setRscUuid(uuid.toString());
        }
        if (overrideProps != null)
        {
            builder.addAllOverrideProps(asLinStorMapEntryList(overrideProps));
        }
        if (delPropKeys != null)
        {
            builder.addAllDeletePropKeys(delPropKeys);
        }
        send
        (
            msgId,
            API_MOD_RSC,
            builder.build()
        );
        return msgId;
    }

    public int sendDeleteRsc(String nodeName, String resName) throws IOException
    {
        int msgId = this.msgId.incrementAndGet();
        send(
            msgId,
            API_DEL_RSC,
            MsgDelRsc.newBuilder().
                setNodeName(nodeName).
                setRscName(resName).
                build()
        );
        return msgId;
    }

    /*
     * Volume definition messages
     */

    public int sendCreateVlmDfn(String rscName, List<? extends VlmDfn> vlmDfns) throws IOException
    {
        int msgId = this.msgId.incrementAndGet();
        send(
            msgId,
            API_CRT_VLM_DFN,
            MsgCrtVlmDfn.newBuilder()
                .setRscName(rscName)
                .addAllVlmDfns(vlmDfns)
                .build()
        );
        return msgId;
    }

    public int sendModifyVlmDfn(
        UUID uuid,
        String rscName,
        int vlmNr,
        Integer minorNr,
        Long size,
        Map<String, String> overrideProps,
        Set<String> delPropKeys
    )
        throws IOException
    {
        int msgId = this.msgId.incrementAndGet();
        MsgModVlmDfn.Builder builder = MsgModVlmDfn.newBuilder()
            .setRscName(rscName)
            .setVlmNr(vlmNr);

        if (uuid != null)
        {
            builder.setVlmDfnUuid(uuid.toString());
        }
        if (minorNr != null)
        {
            builder.setVlmMinor(minorNr);
        }
        if (size != null)
        {
            builder.setVlmSize(size);
        }
        if (overrideProps != null)
        {
            builder.addAllOverrideProps(asLinStorMapEntryList(overrideProps));
        }
        if (delPropKeys != null)
        {
            builder.addAllDeletePropKeys(delPropKeys);
        }
        send
        (
            msgId,
            API_MOD_VLM_DFN,
            builder.build()
        );
        return msgId;
    }

    /*
     * Node connection messages
     */

    public int sendCreateNodeConn(String nodeName1, String nodeName2, Map<String, String> props)
        throws IOException
    {
        int msgId = this.msgId.incrementAndGet();
        MsgCrtNodeConn.Builder msgBuilder = MsgCrtNodeConn.newBuilder().
            setNodeName1(nodeName1).
            setNodeName2(nodeName2);
        if (props != null)
        {
            msgBuilder.addAllNodeConnProps(asLinStorMapEntryList(props));
        }
        send(
            msgId,
            API_CRT_NODE_CONN,
            msgBuilder.build()
        );
        return msgId;
    }

    public int sendModifyNodeConn(
        UUID uuid,
        String nodeName1,
        String nodeName2,
        Map<String, String> overrideProps,
        Set<String> delPropKeys
    )
        throws IOException
    {
        int msgId = this.msgId.incrementAndGet();
        MsgModNodeConn.Builder builder = MsgModNodeConn.newBuilder()
            .setNode1Name(nodeName1)
            .setNode2Name(nodeName2);

        if (uuid != null)
        {
            builder.setNodeConnUuid(uuid.toString());
        }
        if (overrideProps != null)
        {
            builder.addAllOverrideProps(asLinStorMapEntryList(overrideProps));
        }
        if (delPropKeys != null)
        {
            builder.addAllDeletePropKeys(delPropKeys);
        }
        send
        (
            msgId,
            API_MOD_NODE_CONN,
            builder.build()
        );
        return msgId;
    }

    public int sendDeleteNodeConn(String nodeName1, String nodeName2)
        throws IOException
    {
        int msgId = this.msgId.incrementAndGet();
        send(
            msgId,
            API_DEL_NODE_CONN,
            MsgDelNodeConn.newBuilder().
                setNodeName1(nodeName1).
                setNodeName2(nodeName2).
                build()
        );
        return msgId;
    }

    /*
     * Resuorce connection messages
     */

    public int sendCreateRscConn(String nodeName1, String NodeName2, String rscName, Map<String, String> props)
        throws IOException
    {
        int msgId = this.msgId.incrementAndGet();
        MsgCrtRscConn.Builder msgBuilder = MsgCrtRscConn.newBuilder().
            setNodeName1(nodeName1).
            setNodeName2(NodeName2).
            setRscName(rscName);
        if (props != null)
        {
            msgBuilder.addAllRscConnProps(asLinStorMapEntryList(props));
        }
        send(
            msgId,
            API_CRT_RSC_CONN,
            msgBuilder.build()
        );
        return msgId;
    }

    public int sendModifyRscConn(
        UUID uuid,
        String nodeName1,
        String nodeName2,
        String rscName,
        Map<String, String> overrideProps,
        Set<String> delPropKeys
    )
        throws IOException
    {
        int msgId = this.msgId.incrementAndGet();
        MsgModRscConn.Builder builder = MsgModRscConn.newBuilder()
            .setNode1Name(nodeName1)
            .setNode2Name(nodeName2)
            .setRscName(rscName);

        if (uuid != null)
        {
            builder.setRscConnUuid(uuid.toString());
        }
        if (overrideProps != null)
        {
            builder.addAllOverrideProps(asLinStorMapEntryList(overrideProps));
        }
        if (delPropKeys != null)
        {
            builder.addAllDeletePropKeys(delPropKeys);
        }
        send
        (
            msgId,
            API_MOD_RSC_CONN,
            builder.build()
        );
        return msgId;
    }

    public int sendDeleteRscConn(String nodeName1, String NodeName2, String rscName)
        throws IOException
    {
        int msgId = this.msgId.incrementAndGet();
        send(
            msgId,
            API_DEL_RSC_CONN,
            MsgDelRscConn.newBuilder().
                setNodeName1(nodeName1).
                setNodeName2(NodeName2).
                setResourceName(rscName).
                build()
        );
        return msgId;
    }

    /*
     * Volume connection message
     */

    public int sendCreateVlmConn(
        String nodeName1,
        String NodeName2,
        String rscName,
        int vlmNr,
        Map<String, String> props
    )
        throws IOException
    {
        int msgId = this.msgId.incrementAndGet();
        MsgCrtVlmConn.Builder msgBuilder = MsgCrtVlmConn.newBuilder().
            setNodeName1(nodeName1).
            setNodeName2(NodeName2).
            setResourceName(rscName).
            setVolumeNr(vlmNr);
        if (props != null)
        {
            msgBuilder.addAllVolumeConnProps(asLinStorMapEntryList(props));
        }
        send(
            msgId,
            API_CRT_VLM_CONN,
            msgBuilder.build()
        );
        return msgId;
    }

    public int sendModifyVlmConn(
        UUID uuid,
        String nodeName1,
        String nodeName2,
        String rscName,
        int vlmNr,
        Map<String, String> overrideProps,
        Set<String> delPropKeys
    )
        throws IOException
    {
        int msgId = this.msgId.incrementAndGet();
        MsgModVlmConn.Builder builder = MsgModVlmConn.newBuilder()
            .setNode1Name(nodeName1)
            .setNode2Name(nodeName2)
            .setRscName(rscName)
            .setVlmNr(vlmNr);

        if (uuid != null)
        {
            builder.setVlmConnUuid(uuid.toString());
        }
        if (overrideProps != null)
        {
            builder.addAllOverrideProps(asLinStorMapEntryList(overrideProps));
        }
        if (delPropKeys != null)
        {
            builder.addAllDeletePropKeys(delPropKeys);
        }
        send
        (
            msgId,
            API_MOD_VLM_CONN,
            builder.build()
        );
        return msgId;
    }

    public int sendDeleteVlmConn(String nodeName1, String NodeName2, String rscName, int vlmNr)
        throws IOException
    {
        int msgId = this.msgId.incrementAndGet();
        send(
            msgId,
            API_DEL_VLM_CONN,
            MsgDelVlmConn.newBuilder().
                setNodeName1(nodeName1).
                setNodeName2(NodeName2).
                setResourceName(rscName).
                setVolumeNr(vlmNr).
                build()
        );
        return msgId;
    }

    public void send(int msgId, String apiCall, Message msg) throws IOException
    {
        MsgHeader headerMsg = MsgHeader.newBuilder().
            setApiCall(apiCall).
            setMsgId(msgId).
            build();

        ByteArrayOutputStream baos;
        baos = new ByteArrayOutputStream();
        headerMsg.writeDelimitedTo(baos);
        byte[] protoHeader = baos.toByteArray();
        baos.close();
        baos = new ByteArrayOutputStream();
        if (msg != null)
        {
            msg.writeDelimitedTo(baos);
        }
        byte[] protoData = baos.toByteArray();
        baos.close();

        byte[] header = new byte[16];
        ByteBuffer byteBuffer = ByteBuffer.wrap(header);
        byteBuffer.putInt(0, 0); // message type
        byteBuffer.putInt(4, protoHeader.length + protoData.length);

//        println("sending header: " + header.length);
        outputStream.write(header);
//        println("sending protoHeader:" + protoHeader.length);
        outputStream.write(protoHeader);
//        println("sending protoData:" + protoData.length);
        outputStream.write(protoData);

        sentCount++;
    }

    public VlmDfn createVlmDfn(Integer vlmNr, Integer minor, long vlmSize)
    {
        VlmDfn.Builder builder = VlmDfn.newBuilder()
            .setVlmSize(vlmSize);
        if (vlmNr != null)
        {
            builder.setVlmNr(vlmNr);
        }
        if (minor != null)
        {
            builder.setVlmMinor(minor);
        }
        return builder.build();
    }

    public Vlm createVlm(int vlmNr, String storPoolName, String blockDevice, String metaDisk)
    {
        return
            Vlm.newBuilder().
                setVlmNr(vlmNr).
                setStorPoolName(storPoolName).
                setBlockDevice(blockDevice).
                setMetaDisk(metaDisk).
                build();
    }

    public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException
    {
        ClientProtobuf client = new ClientProtobuf(9500);
//         creaeNodeRscVlmDelete(client);
        createNodeConnRscConnVlmConnDelete(client);
    }

    private static void createNodeConnRscConnVlmConnDelete(ClientProtobuf client)
        throws IOException, InterruptedException
    {
        String nodeName1 = "TestNode1";
        String nodeName2 = "TestNode2";
        String rscName = "TestRes";
        String storPoolName = "TestStorPool";
        int vlmNr = 13;

        String nodeConnTestKey = "TestKeyNodeConn";
        String nodeconnTestValue = "TestValueNodeConn";
        String rscConnTestKey = "TestKeyRscConn";
        String rscConnTestValue = "TestValueRscConn";
        String vlmConnTestKey = "TestKeyVlmConn";
        String vlmConnTestValue = "TestValueVlmConn";

        int msgId = 0;

        msgId = client.sendCreateNode(nodeName1, "satellite", null,
            Arrays.asList(
                new NetInterfacePojo(null, "tcp0", "10.0.0.1")
            ),
            Arrays.asList(
                new SatelliteConnectionPojo(
                    "tcp0",
                    ApiConsts.DFLT_STLT_PORT_PLAIN,
                    ApiConsts.VAL_NETCOM_TYPE_PLAIN
                )
            )
        );
        client.println(msgId + " create first node");
        Thread.sleep(500);

        msgId = client.sendCreateNode(nodeName2, "satellite", null,
            Arrays.asList(
                new NetInterfacePojo(null, "tcp0", "10.0.0.2")
            ),
            Arrays.asList(
                new SatelliteConnectionPojo(
                    "tcp0",
                    ApiConsts.DFLT_STLT_PORT_PLAIN,
                    ApiConsts.VAL_NETCOM_TYPE_PLAIN
                )
            )
        );
        client.println(msgId + " create second node");
        Thread.sleep(500);

        List<VlmDfn> vlmDfns = new ArrayList<>();
        vlmDfns.add(client.createVlmDfn(vlmNr, 1, 1_000_000));
        msgId = client.sendCreateRscDfn(rscName, 9001, null, vlmDfns);
        client.println(msgId + " create rscDfn");
        Thread.sleep(500);

        msgId = client.sendCreateStorPoolDfn(storPoolName);
        client.println(msgId + " create storPoolDfn");
        Thread.sleep(500);

        msgId = client.sendCreateStorPool(nodeName1, storPoolName, "LvmDriver");
        client.println(msgId + " create first storPool");
        Thread.sleep(500);

        msgId = client.sendCreateStorPool(nodeName2, storPoolName, "LvmDriver");
        client.println(msgId + " create second storPool");
        Thread.sleep(500);

        List<Vlm> vlms = new ArrayList<>();
        vlms.add(client.createVlm(vlmNr, storPoolName, "blockDevice", "internal"));
        msgId = client.sendCreateRsc(nodeName1, rscName, null, vlms);
        client.println(msgId + " create first rsc");
        Thread.sleep(500);

        msgId = client.sendCreateRsc(nodeName2, rscName, null, vlms);
        client.println(msgId + " create second rsc");
        Thread.sleep(500);



        Map<String, String> nodeConnProps = new HashMap<>();
        nodeConnProps.put(nodeConnTestKey, nodeconnTestValue);
        msgId = client.sendCreateNodeConn(nodeName1, nodeName2, nodeConnProps);
        client.println(msgId + " create node connection");
        Thread.sleep(500);

        Map<String, String> rscConnProps = new HashMap<>();
        rscConnProps.put(rscConnTestKey, rscConnTestValue);
        msgId = client.sendCreateRscConn(nodeName1, nodeName2, rscName, rscConnProps);
        client.println(msgId + " create rsc connection");
        Thread.sleep(500);

        Map<String, String> vlmConnProps = new HashMap<>();
        vlmConnProps.put(vlmConnTestKey, vlmConnTestValue);
        msgId = client.sendCreateVlmConn(nodeName1, nodeName2, rscName, vlmNr, vlmConnProps);
        client.println(msgId + " create vlm connection");
        Thread.sleep(500);





        msgId = client.sendDeleteVlmConn(nodeName1, nodeName2, rscName, vlmNr);
        client.println(msgId + " delete vlm connection");
        Thread.sleep(500);

        msgId = client.sendDeleteRscConn(nodeName1, nodeName2, rscName);
        client.println(msgId + " delete rsc connection");
        Thread.sleep(500);

        msgId = client.sendDeleteNodeConn(nodeName1, nodeName2);
        client.println(msgId + " delete node connection");
        Thread.sleep(500);



        msgId = client.sendDeleteNode(nodeName1);
        client.println(msgId + " delete first node");
        Thread.sleep(500);

        msgId = client.sendDeleteNode(nodeName2);
        client.println(msgId + " delete second node");

        client.outputStream.flush();
        Thread.sleep(1000);
        client.shutdown();
    }

    private static void createNodeRscVlmDelete(ClientProtobuf client)
        throws UnknownHostException, IOException, InterruptedException
    {
        String nodePropsTestKey = "TestNodeKey";
        String nodePropsTestValue = "TestNodeValue";
        String nodeName = "TestNode";

        String resName = "TestRes";
        String resPropsTestKey = "TestResKey";
        String resPropsTestValue = "TestResValue";

        String storPoolName = "TestStorPool";

        Map<String, String> nodeProps = new HashMap<>();
        nodeProps.put(nodePropsTestKey, nodePropsTestValue);

        int msgId = 0;

        msgId = client.sendCreateNode(nodeName, "satellite", nodeProps,
            Arrays.asList(
                new NetInterfacePojo(null, "tcp0", "10.0.0.1")
            ),
            Arrays.asList(
                new SatelliteConnectionPojo(
                    "tcp0",
                    ApiConsts.DFLT_STLT_PORT_PLAIN,
                    ApiConsts.VAL_NETCOM_TYPE_PLAIN
                )
            )
        );
        client.println(msgId + " create node");
        Thread.sleep(500);


        Map<String, String> resDfnProps = new HashMap<>();
        resDfnProps.put(resPropsTestKey, resPropsTestValue);
        List<VlmDfn> vlmDfn = new ArrayList<>();
        vlmDfn.add(client.createVlmDfn(1, 1, 1_000_000));
        msgId = client.sendCreateRscDfn(resName, 9001, resDfnProps, vlmDfn);
        client.println(msgId + " create rscDfn");
        Thread.sleep(500);


        msgId = client.sendCreateStorPoolDfn(storPoolName);
        client.println(msgId + " create storPoolDfn");
        Thread.sleep(500);


        msgId = client.sendCreateStorPool(nodeName, storPoolName, "LvmDriver");
        client.println(msgId + " create storPool");
        Thread.sleep(500);


        Map<String, String> resProps = new HashMap<>();
        List<Vlm> vlms = new ArrayList<>();
        vlms.add(client.createVlm(1, storPoolName, "blockDevice", "internal"));
        msgId = client.sendCreateRsc(nodeName, resName, resProps, vlms);
        client.println(msgId + " create rsc");
        Thread.sleep(500);


        msgId = client.sendDeleteRsc(nodeName, resName);
        client.println(msgId + " delete rsc");
        Thread.sleep(500);


        msgId = client.sendDeleteStorPool(nodeName, storPoolName);
        client.println(msgId + " delete storPool");
        Thread.sleep(500);


        msgId = client.sendDeleteStorPoolDfn(storPoolName);
        client.println(msgId + " delete storPoolDfn");
        Thread.sleep(500);


        msgId = client.sendDeleteRscDfn(resName);
        client.println(msgId + " delete rscDfn");
        Thread.sleep(500);


        msgId = client.sendDeleteNode(nodeName);
        client.println(msgId + " delete node");


        client.outputStream.flush();
        Thread.sleep(1000);
        client.shutdown();
    }

    private Iterable<LinStorMapEntry> asLinStorMapEntryList(Map<String, String> map)
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

    private Map<String, String> asMap(List<LinStorMapEntry> list)
    {
        Map<String, String> map = new TreeMap<>();
        for (LinStorMapEntry entry : list)
        {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }
}
