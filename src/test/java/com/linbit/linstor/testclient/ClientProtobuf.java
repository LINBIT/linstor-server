package com.linbit.linstor.testclient;
//
//import java.io.ByteArrayInputStream;
//import java.io.ByteArrayOutputStream;
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.OutputStream;
//import java.io.PrintStream;
//import java.net.Socket;
//import java.net.UnknownHostException;
//import java.nio.ByteBuffer;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.Map.Entry;
//import java.util.TreeMap;
//import java.util.UUID;
//import java.util.concurrent.atomic.AtomicLong;
//
//import com.google.protobuf.Message;
//import com.linbit.linstor.NetInterface;
//import com.linbit.linstor.Resource.RscFlags;
//import com.linbit.linstor.api.ApiConsts;
//import com.linbit.linstor.proto.MsgHeaderOuterClass.MsgHeader;
//import com.linbit.linstor.proto.responses.MsgGenericResponseOuterClass.MsgGenericResponse;
//import com.linbit.linstor.proto.common.ApiCallResponseOuterClass.ApiCallResponse;
//import com.linbit.linstor.proto.common.LinStorMapEntryOuterClass.LinStorMapEntry;
//import com.linbit.linstor.proto.common.NodeConnOuterClass.NodeConn;
//import com.linbit.linstor.proto.common.NodeOuterClass;
//import com.linbit.linstor.proto.common.RscConnOuterClass.RscConn;
//import com.linbit.linstor.proto.common.RscDfnOuterClass.RscDfn;
//import com.linbit.linstor.proto.common.RscOuterClass.Rsc;
//import com.linbit.linstor.proto.common.StorPoolDfnOuterClass.StorPoolDfn;
//import com.linbit.linstor.proto.common.StorPoolOuterClass.StorPool;
//import com.linbit.linstor.proto.common.VlmConnOuterClass.VlmConn;
//import com.linbit.linstor.proto.common.VlmDfnOuterClass.VlmDfn;
//import com.linbit.linstor.proto.common.VlmOuterClass.Vlm;
//import com.linbit.linstor.proto.requests.MsgCrtNodeConnOuterClass.MsgCrtNodeConn;
//import com.linbit.linstor.proto.requests.MsgCrtNodeOuterClass.MsgCrtNode;
//import com.linbit.linstor.proto.requests.MsgCrtRscConnOuterClass.MsgCrtRscConn;
//import com.linbit.linstor.proto.requests.MsgCrtRscDfnOuterClass.MsgCrtRscDfn;
//import com.linbit.linstor.proto.requests.MsgCrtRscOuterClass.MsgCrtRsc;
//import com.linbit.linstor.proto.requests.MsgCrtStorPoolDfnOuterClass.MsgCrtStorPoolDfn;
//import com.linbit.linstor.proto.requests.MsgCrtStorPoolOuterClass.MsgCrtStorPool;
//import com.linbit.linstor.proto.requests.MsgCrtVlmConnOuterClass.MsgCrtVlmConn;
//import com.linbit.linstor.proto.requests.MsgCrtVlmDfnOuterClass.MsgCrtVlmDfn;
//import com.linbit.linstor.proto.requests.MsgDelNodeConnOuterClass.MsgDelNodeConn;
//import com.linbit.linstor.proto.requests.MsgDelNodeOuterClass.MsgDelNode;
//import com.linbit.linstor.proto.requests.MsgDelRscConnOuterClass.MsgDelRscConn;
//import com.linbit.linstor.proto.requests.MsgDelRscDfnOuterClass.MsgDelRscDfn;
//import com.linbit.linstor.proto.requests.MsgDelRscOuterClass.MsgDelRsc;
//import com.linbit.linstor.proto.requests.MsgDelStorPoolDfnOuterClass.MsgDelStorPoolDfn;
//import com.linbit.linstor.proto.requests.MsgDelStorPoolOuterClass.MsgDelStorPool;
//import com.linbit.linstor.proto.requests.MsgDelVlmConnOuterClass.MsgDelVlmConn;
//import com.linbit.linstor.proto.requests.MsgDelVlmDfnOuterClass.MsgDelVlmDfn;
//import com.linbit.linstor.proto.requests.MsgModNodeConnOuterClass.MsgModNodeConn;
//import com.linbit.linstor.proto.requests.MsgModNodeOuterClass.MsgModNode;
//import com.linbit.linstor.proto.requests.MsgModRscConnOuterClass.MsgModRscConn;
//import com.linbit.linstor.proto.requests.MsgModRscDfnOuterClass.MsgModRscDfn;
//import com.linbit.linstor.proto.requests.MsgModRscOuterClass.MsgModRsc;
//import com.linbit.linstor.proto.requests.MsgModStorPoolDfnOuterClass.MsgModStorPoolDfn;
//import com.linbit.linstor.proto.requests.MsgModStorPoolOuterClass.MsgModStorPool;
//import com.linbit.linstor.proto.requests.MsgModVlmConnOuterClass.MsgModVlmConn;
//import com.linbit.linstor.proto.requests.MsgModVlmDfnOuterClass.MsgModVlmDfn;
//import com.linbit.linstor.stateflags.FlagsHelper;
//
//public class ClientProtobuf implements Runnable
//{
//    public static final Object CALLBACK_LOCK = new Object();
//
//    public interface MessageCallback
//    {
//        void error(long apiCallId, long retCode, String message, String cause, String correction,
//            String details, Map<String, String> objRefsMap);
//        void warn(long apiCallId, long retCode, String message, String cause, String correction,
//            String details, Map<String, String> objRefsMap);
//        void info(long apiCallId, long retCode, String message, String cause, String correction,
//            String details, Map<String, String> objRefsMap);
//        void success(long apiCallId, long retCode, String message, String cause, String correction,
//            String details, Map<String, String> objRefsMap);
//    }
//
//    public static final Map<Long, String> RET_CODES_TYPE;
//    public static final Map<Long, String> RET_CODES_OP;
//    public static final Map<Long, String> RET_CODES_OBJ;
//    public static final Map<Long, String> RET_CODES_ACTION;
//
//    static
//    {
//        HashMap<Long, String> tmpMap = new HashMap<>();
//        tmpMap.put(ApiConsts.MASK_ERROR, "Error");
//        tmpMap.put(ApiConsts.MASK_WARN, "Warn");
//        tmpMap.put(ApiConsts.MASK_INFO, "Info");
//        tmpMap.put(ApiConsts.MASK_SUCCESS, "Success");
//        RET_CODES_TYPE = Collections.unmodifiableMap(tmpMap);
//
//        tmpMap = new HashMap<>();
//        tmpMap.put(ApiConsts.MASK_CRT, "Create");
//        tmpMap.put(ApiConsts.MASK_MOD, "Modify");
//        tmpMap.put(ApiConsts.MASK_DEL, "Delete");
//        RET_CODES_OP = Collections.unmodifiableMap(tmpMap);
//
//        tmpMap = new HashMap<>();
//        tmpMap.put(ApiConsts.MASK_NODE, "Node");
//        tmpMap.put(ApiConsts.MASK_RSC_DFN, "RscDfn");
//        tmpMap.put(ApiConsts.MASK_RSC, "Rsc");
//        tmpMap.put(ApiConsts.MASK_VLM_DFN, "VlmDfn");
//        tmpMap.put(ApiConsts.MASK_VLM, "Vlm");
//        tmpMap.put(ApiConsts.MASK_STOR_POOL_DFN, "StorPoolDfn");
//        tmpMap.put(ApiConsts.MASK_STOR_POOL, "StorPool");
//        tmpMap.put(ApiConsts.MASK_NODE_CONN, "NodeConn");
//        tmpMap.put(ApiConsts.MASK_RSC_CONN, "RscConn");
//        tmpMap.put(ApiConsts.MASK_VLM_CONN, "VlmConn");
//        tmpMap.put(ApiConsts.MASK_NET_IF, "NetIf");
//        RET_CODES_OBJ = Collections.unmodifiableMap(tmpMap);
//
//        tmpMap = new HashMap<>();
//        tmpMap.put(ApiConsts.CREATED, "Created");
//        tmpMap.put(ApiConsts.DELETED, "Deleted");
//        tmpMap.put(ApiConsts.MODIFIED, "Modified");
//
//        tmpMap.put(ApiConsts.FAIL_SQL, "FAIL_SQL");
//        tmpMap.put(ApiConsts.FAIL_SQL_ROLLBACK, "FAIL_SQL_ROLLBACK");
//        tmpMap.put(ApiConsts.FAIL_INVLD_NODE_NAME, "FAIL_INVLD_NODE_NAME");
//        tmpMap.put(ApiConsts.FAIL_INVLD_NODE_TYPE, "FAIL_INVLD_NODE_TYPE");
//        tmpMap.put(ApiConsts.FAIL_INVLD_RSC_NAME, "FAIL_INVLD_RSC_NAME");
//        tmpMap.put(ApiConsts.FAIL_INVLD_RSC_PORT, "FAIL_INVLD_RSC_PORT");
//        tmpMap.put(ApiConsts.FAIL_INVLD_NODE_ID, "FAIL_INVLD_NODE_ID");
//        tmpMap.put(ApiConsts.FAIL_INVLD_VLM_NR, "FAIL_INVLD_VLM_NR");
//        tmpMap.put(ApiConsts.FAIL_INVLD_VLM_SIZE, "FAIL_INVLD_VLM_SIZE");
//        tmpMap.put(ApiConsts.FAIL_INVLD_MINOR_NR, "FAIL_INVLD_MINOR_NR");
//        tmpMap.put(ApiConsts.FAIL_INVLD_STOR_POOL_NAME, "FAIL_INVLD_STOR_POOL_NAME");
//        tmpMap.put(ApiConsts.FAIL_INVLD_NET_NAME, "FAIL_INVLD_NET_NAME");
//        tmpMap.put(ApiConsts.FAIL_INVLD_NET_ADDR, "FAIL_INVLD_NET_ADDR");
//        tmpMap.put(ApiConsts.FAIL_INVLD_NET_PORT, "FAIL_INVLD_NET_PORT");
//        tmpMap.put(ApiConsts.FAIL_INVLD_NET_TYPE, "FAIL_INVLD_NET_TYPE");
//        tmpMap.put(ApiConsts.FAIL_INVLD_PROP, "FAIL_INVLD_PROP");
//        tmpMap.put(ApiConsts.FAIL_NOT_FOUND_NODE, "FAIL_NOT_FOUND_NODE");
//        tmpMap.put(ApiConsts.FAIL_NOT_FOUND_RSC_DFN, "FAIL_NOT_FOUND_RSC_DFN");
//        tmpMap.put(ApiConsts.FAIL_NOT_FOUND_RSC, "FAIL_NOT_FOUND_RSC");
//        tmpMap.put(ApiConsts.FAIL_NOT_FOUND_VLM_DFN, "FAIL_NOT_FOUND_VLM_DFN");
//        tmpMap.put(ApiConsts.FAIL_NOT_FOUND_VLM, "FAIL_NOT_FOUND_VLM");
//        tmpMap.put(ApiConsts.FAIL_NOT_FOUND_NET_IF, "FAIL_NOT_FOUND_NET_IF");
//        tmpMap.put(ApiConsts.FAIL_NOT_FOUND_NODE_CONN, "FAIL_NOT_FOUND_NODE_CONN");
//        tmpMap.put(ApiConsts.FAIL_NOT_FOUND_RSC_CONN, "FAIL_NOT_FOUND_RSC_CONN");
//        tmpMap.put(ApiConsts.FAIL_NOT_FOUND_VLM_CONN, "FAIL_NOT_FOUND_VLM_CONN");
//        tmpMap.put(ApiConsts.FAIL_NOT_FOUND_STOR_POOL_DFN, "FAIL_NOT_FOUND_STOR_POOL_DFN");
//        tmpMap.put(ApiConsts.FAIL_NOT_FOUND_STOR_POOL, "FAIL_NOT_FOUND_STOR_POOL");
//        tmpMap.put(ApiConsts.FAIL_NOT_FOUND_DFLT_STOR_POOL, "FAIL_NOT_FOUND_DFLT_STOR_POOL");
//        tmpMap.put(ApiConsts.FAIL_ACC_DENIED_NODE, "FAIL_ACC_DENIED_NODE");
//        tmpMap.put(ApiConsts.FAIL_ACC_DENIED_RSC_DFN, "FAIL_ACC_DENIED_RSC_DFN");
//        tmpMap.put(ApiConsts.FAIL_ACC_DENIED_RSC, "FAIL_ACC_DENIED_RSC");
//        tmpMap.put(ApiConsts.FAIL_ACC_DENIED_VLM_DFN, "FAIL_ACC_DENIED_VLM_DFN");
//        tmpMap.put(ApiConsts.FAIL_ACC_DENIED_VLM, "FAIL_ACC_DENIED_VLM");
//        tmpMap.put(ApiConsts.FAIL_ACC_DENIED_STOR_POOL_DFN, "FAIL_ACC_DENIED_STOR_POOL_DFN");
//        tmpMap.put(ApiConsts.FAIL_ACC_DENIED_STOR_POOL, "FAIL_ACC_DENIED_STOR_POOL");
//        tmpMap.put(ApiConsts.FAIL_ACC_DENIED_NODE_CONN, "FAIL_ACC_DENIED_NODE_CONN");
//        tmpMap.put(ApiConsts.FAIL_ACC_DENIED_RSC_CONN, "FAIL_ACC_DENIED_RSC_CONN");
//        tmpMap.put(ApiConsts.FAIL_ACC_DENIED_VLM_CONN, "FAIL_ACC_DENIED_VLM_CONN");
//        tmpMap.put(ApiConsts.FAIL_ACC_DENIED_STLT_CONN, "FAIL_ACC_DENIED_STLT_CONN");
//        tmpMap.put(ApiConsts.FAIL_EXISTS_NODE, "FAIL_EXISTS_NODE");
//        tmpMap.put(ApiConsts.FAIL_EXISTS_RSC_DFN, "FAIL_EXISTS_RSC_DFN");
//        tmpMap.put(ApiConsts.FAIL_EXISTS_RSC, "FAIL_EXISTS_RSC");
//        tmpMap.put(ApiConsts.FAIL_EXISTS_VLM_DFN, "FAIL_EXISTS_VLM_DFN");
//        tmpMap.put(ApiConsts.FAIL_EXISTS_VLM, "FAIL_EXISTS_VLM");
//        tmpMap.put(ApiConsts.FAIL_EXISTS_NET_IF, "FAIL_EXISTS_NET_IF");
//        tmpMap.put(ApiConsts.FAIL_EXISTS_NODE_CONN, "FAIL_EXISTS_NODE_CONN");
//        tmpMap.put(ApiConsts.FAIL_EXISTS_RSC_CONN, "FAIL_EXISTS_RSC_CONN");
//        tmpMap.put(ApiConsts.FAIL_EXISTS_VLM_CONN, "FAIL_EXISTS_VLM_CONN");
//        tmpMap.put(ApiConsts.FAIL_EXISTS_STOR_POOL_DFN, "FAIL_EXISTS_STOR_POOL_DFN");
//        tmpMap.put(ApiConsts.FAIL_EXISTS_STOR_POOL, "FAIL_EXISTS_STOR_POOL");
//        tmpMap.put(ApiConsts.FAIL_MISSING_PROPS, "FAIL_MISSING_PROPS");
//        tmpMap.put(ApiConsts.FAIL_MISSING_PROPS_NETCOM_TYPE, "FAIL_MISSING_PROPS_NETCOM_TYPE");
//        tmpMap.put(ApiConsts.FAIL_MISSING_PROPS_NETCOM_PORT, "FAIL_MISSING_PROPS_NETCOM_PORT");
//        tmpMap.put(ApiConsts.FAIL_MISSING_NETCOM, "FAIL_MISSING_NETCOM");
//        tmpMap.put(ApiConsts.FAIL_MISSING_PROPS_NETIF_NAME, "FAIL_MISSING_PROPS_NETIF_NAME");
//        tmpMap.put(ApiConsts.FAIL_MISSING_STLT_CONN, "FAIL_MISSING_STLT_CONN");
//        tmpMap.put(ApiConsts.FAIL_UUID_NODE, "FAIL_UUID_NODE");
//        tmpMap.put(ApiConsts.FAIL_UUID_RSC_DFN, "FAIL_UUID_RSC_DFN");
//        tmpMap.put(ApiConsts.FAIL_UUID_RSC, "FAIL_UUID_RSC");
//        tmpMap.put(ApiConsts.FAIL_UUID_VLM_DFN, "FAIL_UUID_VLM_DFN");
//        tmpMap.put(ApiConsts.FAIL_UUID_VLM, "FAIL_UUID_VLM");
//        tmpMap.put(ApiConsts.FAIL_UUID_NET_IF, "FAIL_UUID_NET_IF");
//        tmpMap.put(ApiConsts.FAIL_UUID_NODE_CONN, "FAIL_UUID_NODE_CONN");
//        tmpMap.put(ApiConsts.FAIL_UUID_RSC_CONN, "FAIL_UUID_RSC_CONN");
//        tmpMap.put(ApiConsts.FAIL_UUID_VLM_CONN, "FAIL_UUID_VLM_CONN");
//        tmpMap.put(ApiConsts.FAIL_UUID_STOR_POOL_DFN, "FAIL_UUID_STOR_POOL_DFN");
//        tmpMap.put(ApiConsts.FAIL_UUID_STOR_POOL, "FAIL_UUID_STOR_POOL");
//        tmpMap.put(ApiConsts.FAIL_IN_USE, "FAIL_IN_USE");
//        tmpMap.put(ApiConsts.FAIL_UNKNOWN_ERROR, "FAIL_UNKNOWN_ERROR");
//        tmpMap.put(ApiConsts.FAIL_IMPL_ERROR, "FAIL_IMPL_ERROR");
//        tmpMap.put(ApiConsts.WARN_INVLD_OPT_PROP_NETCOM_ENABLED, "WARN_INVLD_OPT_PROP_NETCOM_ENABLED");
//        tmpMap.put(ApiConsts.WARN_NOT_CONNECTED, "WARN_NOT_CONNECTED");
//        tmpMap.put(ApiConsts.WARN_NOT_FOUND, "WARN_NOT_FOUND");
//        RET_CODES_ACTION = Collections.unmodifiableMap(tmpMap);
//    }
//
//    private Socket sock;
//    private InputStream inputStream;
//    private OutputStream outputStream;
//    private AtomicLong apiCallIdGen = new AtomicLong(0);
//
//    private Thread thread;
//    private boolean shutdown;
//
//    private int sentCount;
//    private int successCount;
//    private int infoCount;
//    private int warnCount;
//    private int errorCount;
//    private PrintStream outStream;
//
//    private List<MessageCallback> callbacks = new ArrayList<>();
//
//    public ClientProtobuf(int port) throws UnknownHostException, IOException
//    {
//        this("localhost", port, System.out);
//    }
//
//    public ClientProtobuf(String host, int port) throws UnknownHostException, IOException
//    {
//        this(host, port, System.out);
//    }
//
//    public ClientProtobuf(String host, int port, PrintStream out) throws UnknownHostException, IOException
//    {
//        outStream = out;
//        sock = new Socket(host, port);
//        inputStream = sock.getInputStream();
//        outputStream = sock.getOutputStream();
//        resetAllCounts();
//
//        shutdown = false;
//        thread = new Thread(this, "ClientProtobuf");
//        thread.start();
//    }
//
//    public void setPrintStream(PrintStream printStream)
//    {
//        outStream = printStream;
//    }
//
//    public void resetAllCounts()
//    {
//        resetSuccessCount();
//        resetInfoCount();
//        resetWarnCount();
//        resetErrorCount();
//    }
//
//    public void resetSentCount()
//    {
//        sentCount = 0;
//    }
//
//    public void resetSuccessCount()
//    {
//        successCount = 0;
//    }
//
//    public void resetInfoCount()
//    {
//        infoCount = 0;
//    }
//
//    public void resetWarnCount()
//    {
//        warnCount = 0;
//    }
//
//    public void resetErrorCount()
//    {
//        errorCount = 0;
//    }
//
//    public void shutdown() throws IOException
//    {
//        shutdown = true;
//        sock.close();
//        thread.interrupt();
//    }
//
//    @Override
//    @SuppressWarnings({"DescendantToken", "checkstyle:magicnumber"})
//    // multiple returns - only for this prototype
//    public void run()
//    {
//        StringBuilder sb = new StringBuilder();
//        byte[] header = new byte[16];
//        int read;
//        int offset = 0;
//        int protoLen;
//        while (!shutdown)
//        {
//            try
//            {
//                offset = 0;
//                while (offset != header.length)
//                {
//                    read = inputStream.read(header, offset, header.length - offset);
//                    if (read == -1)
//                    {
//                        return; // not very clean, but enough for this prototype
//                    }
//                    offset += read;
//                }
//
//                protoLen = (header[4] & 0xFF) << 24 |
//                           (header[5] & 0xFF) << 16 |
//                           (header[6] & 0xFF) << 8  |
//                           (header[7] & 0xFF);
//                offset = 0;
//                byte[] data = new byte[protoLen];
//
//                while (offset != protoLen)
//                {
//                    read = inputStream.read(data, offset, protoLen - offset);
//                    if (read == -1)
//                    {
//                        return; // not very clean, but enough for this prototype
//                    }
//                    offset += read;
//                }
//
//                ByteArrayInputStream bais = new ByteArrayInputStream(data);
//
//                MsgHeader protoHeader = MsgHeader.parseDelimitedFrom(bais);
//
//                sb.setLength(0);
//                int responseIdx = 1;
//
//                String apiCall = protoHeader.getMsgContent();
//                if (bais.available() == 0)
//                {
//                    // maybe a pong or a header-only answer
//                    sb.append("ApiCallId: ")
//                        .append(protoHeader.getApiCallId())
//                        .append("\n")
//                        .append(apiCall)
//                        .append("\n");
//                }
//                if (!apiCall.equals(ApiConsts.API_VERSION))
//                {
//                    while (bais.available() > 0)
//                    {
//                        MsgGenericResponse genericResponse = MsgGenericResponse.parseDelimitedFrom(bais);
//                        ApiCallResponse response = genericResponse.getResponse();
//                        long retCode = response.getRetCode();
//                        String message = response.getMessage();
//                        String cause = response.getCause();
//                        String correction = response.getCorrection();
//                        String details = response.getDetails();
//                        Map<String, String> objRefsMap = asMap(response.getObjRefsList());
//
//                        callback(protoHeader.getApiCallId(), retCode, message, cause, correction,
//                            details, objRefsMap);
//
//                        formatMessage(
//                            sb,
//                            protoHeader.getApiCallId(),
//                            responseIdx++,
//                            retCode,
//                            message,
//                            cause,
//                            correction,
//                            details,
//                            objRefsMap
//                        );
//                        sb.append("\n");
//                    }
//                    println(sb.toString());
//                }
//            }
//            catch (IOException ioExc)
//            {
//                if (!shutdown)
//                {
//                    ioExc.printStackTrace();
//                }
//            }
//        }
//        println("clientprotobuf shutting down");
//
//        println("");
//        println("Sent messages: " + sentCount);
//        println("Responses    : " + (infoCount + warnCount + errorCount + successCount));
//        println("   success : " + successCount);
//        println("   info    : " + infoCount);
//        println("   warn    : " + warnCount);
//        println("   error   : " + errorCount);
//    }
//
//    public void formatMessage(
//        StringBuilder sbRef,
//        long apiCallId,
//        int responseIdx,
//        long retCode,
//        String message,
//        String cause,
//        String correction,
//        String details,
//        Map<String, String> objRefsMap
//    )
//    {
//        StringBuilder sb;
//        if (sbRef == null)
//        {
//            sb = new StringBuilder();
//        }
//        else
//        {
//            sb = sbRef;
//        }
//        sb.append("ApiCallId: ")
//            .append(apiCallId)
//            .append("\n")
//            .append("   Response ")
//            .append(responseIdx)
//            .append(": \n      RetCode   : ");
//        decodeRetValue(sb, retCode);
//        if (message != null && !"".equals(message))
//        {
//            sb.append("\n      Message   : ").append(format(message));
//        }
//        if (details != null && !"".equals(details))
//        {
//            sb.append("\n      Details   : ").append(format(details));
//        }
//        if (cause != null && !"".equals(cause))
//        {
//            sb.append("\n      Cause     : ").append(format(cause));
//        }
//        if (correction != null && !"".equals(correction))
//        {
//            sb.append("\n      Correction: ").append(format(correction));
//        }
//        if (objRefsMap != null && !objRefsMap.isEmpty())
//        {
//            sb.append("\n      ObjRefs: ");
//            for (Entry<String, String> entry : objRefsMap.entrySet())
//            {
//                sb.append("\n         ").append(entry.getKey()).append("=").append(entry.getValue());
//            }
//        }
//        else
//        {
//            if (retCode == ApiConsts.UNKNOWN_API_CALL)
//            {
//                sb.append("\n      No ObjRefs");
//            }
//            else
//            {
//                sb.append("\n      No ObjRefs defined! Report this to the dev - this should not happen!");
//            }
//
//        }
//    }
//
//    public void registerCallback(MessageCallback callback)
//    {
//        callbacks.add(callback);
//    }
//
//    public void unregisterCallback(MessageCallback callback)
//    {
//        callbacks.remove(callback);
//    }
//
//    private void callback(
//        long apiCallId, long retCode, String message, String cause, String correction, String details,
//        Map<String, String> objRefsMap
//    )
//    {
//        synchronized (CALLBACK_LOCK)
//        {
//            if ((retCode & ApiConsts.MASK_ERROR) == ApiConsts.MASK_ERROR)
//            {
//                for (MessageCallback cb : callbacks)
//                {
//                    cb.error(apiCallId, retCode, message, cause, correction, details, objRefsMap);
//                }
//            }
//            else
//            if ((retCode & ApiConsts.MASK_WARN) == ApiConsts.MASK_WARN)
//            {
//                for (MessageCallback cb : callbacks)
//                {
//                    cb.warn(apiCallId, retCode, message, cause, correction, details, objRefsMap);
//                }
//            }
//            else
//            if ((retCode & ApiConsts.MASK_INFO) == ApiConsts.MASK_INFO)
//            {
//                for (MessageCallback cb : callbacks)
//                {
//                    cb.info(apiCallId, retCode, message, cause, correction, details, objRefsMap);
//                }
//            }
//            else
//            {
//                for (MessageCallback cb : callbacks)
//                {
//                    cb.success(apiCallId, retCode, message, cause, correction, details, objRefsMap);
//                }
//            }
//        }
//    }
//
//    private String format(String msg)
//    {
//        return msg.replaceAll("\\n", "\n               ");
//    }
//
//    @SuppressWarnings("checkstyle:magicnumber")
//    private void decodeRetValue(StringBuilder sb, long retCode)
//    {
//        if ((retCode & ApiConsts.MASK_ERROR) == ApiConsts.MASK_ERROR)
//        {
//            errorCount++;
//        }
//        else
//        if ((retCode & ApiConsts.MASK_WARN) == ApiConsts.MASK_WARN)
//        {
//            warnCount++;
//        }
//        else
//        if ((retCode & ApiConsts.MASK_INFO) == ApiConsts.MASK_INFO)
//        {
//            infoCount++;
//        }
//        else
//        {
//            successCount++;
//        }
//
//        appendReadableRetCode(sb, retCode);
//        if ((retCode & 0x00FFFFFFFFFFFFFFL) == 0)
//        {
//            sb.append(" This looks wrong - please report to developer");
//        }
//    }
//
//    @SuppressWarnings("checkstyle:magicnumber")
//    public static void appendReadableRetCode(StringBuilder sb, long retCode)
//    {
//        appendReadableRetCode(sb, retCode, 0xC000000000000000L, RET_CODES_TYPE);
//        appendReadableRetCode(sb, retCode, 0x0000000003000000L, RET_CODES_OP);
//        appendReadableRetCode(sb, retCode, 0x00000000003C0000L, RET_CODES_OBJ);
//        appendReadableRetCode(sb, retCode, 0xC00000000000FFFFL, RET_CODES_ACTION);
//        sb.append(Long.toHexString(retCode));
//    }
//
//    public static void appendReadableRetCode(
//        StringBuilder sb,
//        long retCode,
//        long mask,
//        Map<Long, String> retCodes
//    )
//    {
//        for (Entry<Long, String> entry : retCodes.entrySet())
//        {
//            if ((retCode & mask) == entry.getKey())
//            {
//                sb.append(entry.getValue()).append(" ");
//            }
//        }
//    }
//
//    private void println(String str)
//    {
//        if (outStream != null)
//        {
//            outStream.println(str);
//        }
//    }
//
//    public long sendPing() throws IOException
//    {
//        long apiCallId = apiCallIdGen.incrementAndGet();
//        send(apiCallId, ApiConsts.API_PING, null);
//        return apiCallId;
//    }
//
//    /*
//     * Node messages
//     */
//
//    public long sendCreateNode(
//        String nodeName,
//        String nodeType,
//        Map<String, String> props,
//        List<? extends NetInterface.NetInterfaceApi> netIfs
//    )
//        throws IOException
//    {
//        long apiCallId = apiCallIdGen.incrementAndGet();
//        NodeOuterClass.Node.Builder nodeBuilder = NodeOuterClass.Node.newBuilder()
//            .setName(nodeName)
//            .setType(nodeType);
//        if (props != null)
//        {
//            nodeBuilder.addAllProps(asLinStorMapEntryList(props));
//        }
//
//        MsgCrtNode.Builder msgCrtNodeBuilder = MsgCrtNode.newBuilder();
//        msgCrtNodeBuilder.setNode(nodeBuilder.build());
//
//        send(
//            apiCallId,
//            ApiConsts.API_CRT_NODE,
//            msgCrtNodeBuilder.build()
//        );
//        return apiCallId;
//    }
//
//    public long sendModifyNode(
//        UUID uuid,
//        String nodeName,
//        String typeName,
//        Map<String, String> overrideProps,
//        Set<String> delProps
//    )
//        throws IOException
//    {
//        long apiCallId = apiCallIdGen.incrementAndGet();
//        MsgModNode.Builder msgBuilder = MsgModNode.newBuilder()
//            .setNodeName(nodeName);
//        if (uuid != null)
//        {
//            msgBuilder.setNodeUuid(uuid.toString());
//        }
//        if (typeName != null)
//        {
//            msgBuilder.setNodeType(typeName);
//        }
//        if (overrideProps != null)
//        {
//            msgBuilder.addAllOverrideProps(asLinStorMapEntryList(overrideProps));
//        }
//        if (delProps != null)
//        {
//            msgBuilder.addAllDeletePropKeys(delProps);
//        }
//
//        send(
//            apiCallId,
//            ApiConsts.API_MOD_NODE,
//            msgBuilder.build()
//        );
//        return apiCallId;
//    }
//
//    public long sendDeleteNode(String nodeName) throws IOException
//    {
//        long apiCallId = apiCallIdGen.incrementAndGet();
//        send(
//            apiCallId,
//            ApiConsts.API_DEL_NODE,
//            MsgDelNode.newBuilder()
//                .setNodeName(nodeName)
//                .build()
//        );
//        return apiCallId;
//    }
//
//
//    /*
//     * Resource definition messages
//     */
//
//
//    public long sendCreateRscDfn(
//        String resName,
//        Integer port,
//        Map<String, String> resDfnProps,
//        Iterable<? extends VlmDfn> vlmDfn
//    )
//        throws IOException
//    {
//        long apiCallId = apiCallIdGen.incrementAndGet();
//        RscDfn.Builder rscDfnBuilder = RscDfn.newBuilder();
//        rscDfnBuilder.setRscName(resName);
//        if (port != null)
//        {
//            rscDfnBuilder.setRscDfnPort(port);
//        }
//        if (resDfnProps != null)
//        {
//            rscDfnBuilder.addAllRscDfnProps(asLinStorMapEntryList(resDfnProps));
//        }
//        if (vlmDfn != null)
//        {
//            rscDfnBuilder.addAllVlmDfns(vlmDfn);
//        }
//        send(
//            apiCallId,
//            ApiConsts.API_CRT_RSC_DFN,
//            MsgCrtRscDfn.newBuilder().setRscDfn(
//                rscDfnBuilder.build()
//            ).build()
//        );
//        return apiCallId;
//    }
//
//    public long sendModifyRscDfn(
//        UUID rscDfnUuid,
//        String rscName,
//        Integer port,
//        Map<String, String> overrideProps,
//        Set<String> delProps
//    )
//        throws IOException
//    {
//        long apiCallId = apiCallIdGen.incrementAndGet();
//        MsgModRscDfn.Builder msgBuilder = MsgModRscDfn.newBuilder()
//            .setRscName(rscName);
//        if (rscDfnUuid != null)
//        {
//            msgBuilder.setRscDfnUuid(rscDfnUuid.toString());
//        }
//        if (port != null)
//        {
//            msgBuilder.setRscDfnPort(port);
//        }
//        if (overrideProps != null)
//        {
//            msgBuilder.addAllOverrideProps(asLinStorMapEntryList(overrideProps));
//        }
//        if (delProps != null)
//        {
//            msgBuilder.addAllDeletePropKeys(delProps);
//        }
//        send(
//            apiCallId,
//            ApiConsts.API_MOD_RSC_DFN,
//            msgBuilder.build()
//        );
//        return apiCallId;
//    }
//
//    public long sendDeleteRscDfn(String resName) throws IOException
//    {
//        long apiCallId = apiCallIdGen.incrementAndGet();
//        send(
//            apiCallId,
//            ApiConsts.API_DEL_RSC_DFN,
//            MsgDelRscDfn.newBuilder()
//                .setRscName(resName)
//                .build()
//        );
//        return apiCallId;
//    }
//
//    /*
//     * StorPoolDefinition messages
//     */
//
//    public long sendCreateStorPoolDfn(String storPoolName) throws IOException
//    {
//        long apiCallId = apiCallIdGen.incrementAndGet();
//        send(
//            apiCallId,
//            ApiConsts.API_CRT_STOR_POOL_DFN,
//            MsgCrtStorPoolDfn.newBuilder()
//                .setStorPoolDfn(
//                    StorPoolDfn.newBuilder()
//                        .setStorPoolName(storPoolName)
//                        .build()
//                )
//                .build()
//        );
//        return apiCallId;
//    }
//
//    public long sendModifyStorPoolDfn(
//        UUID storPoolUuid,
//        String storPoolName,
//        Map<String, String> overrideProps,
//        Set<String> delPropKeys
//    )
//        throws IOException
//    {
//        long apiCallId = apiCallIdGen.incrementAndGet();
//        MsgModStorPoolDfn.Builder builder = MsgModStorPoolDfn.newBuilder()
//            .setStorPoolName(storPoolName);
//        if (storPoolUuid != null)
//        {
//            builder.setStorPoolDfnUuid(storPoolUuid.toString());
//        }
//        if (overrideProps != null)
//        {
//            builder.addAllOverrideProps(asLinStorMapEntryList(overrideProps));
//        }
//        if (delPropKeys != null)
//        {
//            builder.addAllDeletePropKeys(delPropKeys);
//        }
//        send(
//            apiCallId,
//            ApiConsts.API_MOD_STOR_POOL_DFN,
//            builder.build()
//        );
//        return apiCallId;
//    }
//
//    public long sendDeleteStorPoolDfn(String storPoolName) throws IOException
//    {
//        long apiCallId = apiCallIdGen.incrementAndGet();
//        send(
//            apiCallId,
//            ApiConsts.API_DEL_STOR_POOL_DFN,
//            MsgDelStorPoolDfn.newBuilder()
//                .setStorPoolName(storPoolName)
//                .build()
//        );
//        return apiCallId;
//    }
//
//    /*
//     * StorPool messages
//     */
//
//    public long sendCreateStorPool(String nodeName, String storPoolName, String driver) throws IOException
//    {
//        long apiCallId = apiCallIdGen.incrementAndGet();
//        send(
//            apiCallId,
//            ApiConsts.API_CRT_STOR_POOL,
//            MsgCrtStorPool.newBuilder()
//                .setStorPool(
//                    StorPool.newBuilder()
//                        .setNodeName(nodeName)
//                        .setStorPoolName(storPoolName)
//                        .setDriver(driver)
//                        .build()
//                )
//                .build()
//        );
//        return apiCallId;
//    }
//
//    public long sendModifyStorPool(
//        UUID uuid,
//        String nodeName,
//        String storPoolName,
//        Map<String, String> overrideProps,
//        Set<String> delPropKeys
//    )
//        throws IOException
//    {
//        long apiCallId = apiCallIdGen.incrementAndGet();
//
//        MsgModStorPool.Builder builder = MsgModStorPool.newBuilder()
//            .setNodeName(nodeName)
//            .setStorPoolName(storPoolName);
//
//        if (uuid != null)
//        {
//            builder.setStorPoolUuid(uuid.toString());
//        }
//        if (overrideProps != null)
//        {
//            builder.addAllOverrideProps(asLinStorMapEntryList(overrideProps));
//        }
//        if (delPropKeys != null)
//        {
//            builder.addAllDeletePropKeys(delPropKeys);
//        }
//
//        send(
//            apiCallId,
//            ApiConsts.API_MOD_STOR_POOL,
//            builder.build()
//        );
//        return apiCallId;
//    }
//
//
//    public long sendDeleteStorPool(String nodeName, String storPoolName) throws IOException
//    {
//        long apiCallId = apiCallIdGen.incrementAndGet();
//        send(
//            apiCallId,
//            ApiConsts.API_DEL_STOR_POOL,
//            MsgDelStorPool.newBuilder()
//                .setNodeName(nodeName)
//                .setStorPoolName(storPoolName)
//                .build()
//        );
//        return apiCallId;
//    }
//
//    /*
//     * Resource messages
//     */
//
//    public long sendCreateRsc(
//        String nodeName,
//        String resName,
//        RscFlags[] flags,
//        Map<String, String> resProps,
//        Iterable<? extends Vlm> vlms
//    )
//        throws IOException
//    {
//        long apiCallId = apiCallIdGen.incrementAndGet();
//        Rsc.Builder rscBuilder = Rsc.newBuilder()
//            .setNodeName(nodeName)
//            .setName(resName);
//        if (resProps != null)
//        {
//            rscBuilder.addAllProps(asLinStorMapEntryList(resProps));
//        }
//        if (vlms != null)
//        {
//            rscBuilder.addAllVlms(vlms);
//        }
//        if (flags != null)
//        {
//            long flagMask = 0;
//            for (RscFlags flag : flags)
//            {
//                flagMask |= flag.flagValue;
//            }
//            rscBuilder.addAllRscFlags(
//                FlagsHelper.toStringList(
//                    RscFlags.class,
//                    flagMask
//                )
//            );
//        }
//        send(
//            apiCallId,
//            ApiConsts.API_CRT_RSC,
//            MsgCrtRsc.newBuilder()
//                .addRscs(
//                    rscBuilder.build()
//                )
//                .build()
//        );
//        return apiCallId;
//    }
//
//    public long sendModifyRsc(
//        UUID uuid,
//        String nodeName,
//        String rscName,
//        Map<String, String> overrideProps,
//        Set<String> delPropKeys
//    )
//        throws IOException
//    {
//        long apiCallId = apiCallIdGen.incrementAndGet();
//        MsgModRsc.Builder builder = MsgModRsc.newBuilder()
//            .setNodeName(nodeName)
//            .setRscName(rscName);
//
//        if (uuid != null)
//        {
//            builder.setRscUuid(uuid.toString());
//        }
//        if (overrideProps != null)
//        {
//            builder.addAllOverrideProps(asLinStorMapEntryList(overrideProps));
//        }
//        if (delPropKeys != null)
//        {
//            builder.addAllDeletePropKeys(delPropKeys);
//        }
//        send(
//            apiCallId,
//            ApiConsts.API_MOD_RSC,
//            builder.build()
//        );
//        return apiCallId;
//    }
//
//    public long sendDeleteRsc(String nodeName, String resName) throws IOException
//    {
//        long apiCallId = apiCallIdGen.incrementAndGet();
//        send(
//            apiCallId,
//            ApiConsts.API_DEL_RSC,
//            MsgDelRsc.newBuilder()
//                .setNodeName(nodeName)
//                .setRscName(resName)
//                .build()
//        );
//        return apiCallId;
//    }
//
//    /*
//     * Volume definition messages
//     */
//
//    public long sendModifyVlmDfn(
//        UUID uuid,
//        String rscName,
//        int vlmNr,
//        Integer minorNr,
//        Long size,
//        Map<String, String> overrideProps,
//        Set<String> delPropKeys
//    )
//        throws IOException
//    {
//        long apiCallId = apiCallIdGen.incrementAndGet();
//        MsgModVlmDfn.Builder builder = MsgModVlmDfn.newBuilder()
//            .setRscName(rscName)
//            .setVlmNr(vlmNr);
//
//        if (uuid != null)
//        {
//            builder.setVlmDfnUuid(uuid.toString());
//        }
//        if (minorNr != null)
//        {
//            builder.setVlmMinor(minorNr);
//        }
//        if (size != null)
//        {
//            builder.setVlmSize(size);
//        }
//        if (overrideProps != null)
//        {
//            builder.addAllOverrideProps(asLinStorMapEntryList(overrideProps));
//        }
//        if (delPropKeys != null)
//        {
//            builder.addAllDeletePropKeys(delPropKeys);
//        }
//        send(
//            apiCallId,
//            ApiConsts.API_MOD_VLM_DFN,
//            builder.build()
//        );
//        return apiCallId;
//    }
//
//    public long sendDeleteVlmDfn(
//        UUID uuid,
//        String rscName,
//        int vlmNr
//    )
//        throws IOException
//    {
//        long apiCallId = apiCallIdGen.incrementAndGet();
//        MsgDelVlmDfn.Builder builder = MsgDelVlmDfn.newBuilder()
//            .setRscName(rscName)
//            .setVlmNr(vlmNr);
//
//        if (uuid != null)
//        {
//            builder.setVlmDfnUuid(uuid.toString());
//        }
//        send(
//            apiCallId,
//            ApiConsts.API_DEL_VLM_DFN,
//            builder.build()
//        );
//        return apiCallId;
//    }
//
//    /*
//     * Node connection messages
//     */
//
//    public long sendCreateNodeConn(String nodeName1, String nodeName2, Map<String, String> props)
//        throws IOException
//    {
//        long apiCallId = apiCallIdGen.incrementAndGet();
//        NodeConn.Builder msgBuilder = NodeConn.newBuilder()
//            .setNodeName1(nodeName1)
//            .setNodeName2(nodeName2);
//        if (props != null)
//        {
//            msgBuilder.addAllNodeConnProps(asLinStorMapEntryList(props));
//        }
//        send(
//            apiCallId,
//            ApiConsts.API_CRT_NODE_CONN,
//            MsgCrtNodeConn.newBuilder()
//                .setNodeConn(
//                    msgBuilder.build()
//                )
//                .build()
//        );
//        return apiCallId;
//    }
//
//    public long sendModifyNodeConn(
//        UUID uuid,
//        String nodeName1,
//        String nodeName2,
//        Map<String, String> overrideProps,
//        Set<String> delPropKeys
//    )
//        throws IOException
//    {
//        long apiCallId = apiCallIdGen.incrementAndGet();
//        MsgModNodeConn.Builder builder = MsgModNodeConn.newBuilder()
//            .setNode1Name(nodeName1)
//            .setNode2Name(nodeName2);
//
//        if (uuid != null)
//        {
//            builder.setNodeConnUuid(uuid.toString());
//        }
//        if (overrideProps != null)
//        {
//            builder.addAllOverrideProps(asLinStorMapEntryList(overrideProps));
//        }
//        if (delPropKeys != null)
//        {
//            builder.addAllDeletePropKeys(delPropKeys);
//        }
//        send(
//            apiCallId,
//            ApiConsts.API_MOD_NODE_CONN,
//            builder.build()
//        );
//        return apiCallId;
//    }
//
//    public long sendDeleteNodeConn(String nodeName1, String nodeName2)
//        throws IOException
//    {
//        long apiCallId = apiCallIdGen.incrementAndGet();
//        send(
//            apiCallId,
//            ApiConsts.API_DEL_NODE_CONN,
//            MsgDelNodeConn.newBuilder()
//                .setNodeName1(nodeName1)
//                .setNodeName2(nodeName2)
//                .build()
//        );
//        return apiCallId;
//    }
//
//    /*
//     * Resuorce connection messages
//     */
//
//    public long sendCreateRscConn(String nodeName1, String nodeName2, String rscName, Map<String, String> props)
//        throws IOException
//    {
//        long apiCallId = apiCallIdGen.incrementAndGet();
//        RscConn.Builder msgBuilder = RscConn.newBuilder()
//            .setNodeName1(nodeName1)
//            .setNodeName2(nodeName2)
//            .setRscName(rscName);
//        if (props != null)
//        {
//            msgBuilder.addAllRscConnProps(asLinStorMapEntryList(props));
//        }
//        send(
//            apiCallId,
//            ApiConsts.API_CRT_RSC_CONN,
//            MsgCrtRscConn.newBuilder()
//                .setRscConn(
//                    msgBuilder.build()
//                )
//                .build()
//        );
//        return apiCallId;
//    }
//
//    public long sendModifyRscConn(
//        UUID uuid,
//        String nodeName1,
//        String nodeName2,
//        String rscName,
//        Map<String, String> overrideProps,
//        Set<String> delPropKeys
//    )
//        throws IOException
//    {
//        long apiCallId = apiCallIdGen.incrementAndGet();
//        MsgModRscConn.Builder builder = MsgModRscConn.newBuilder()
//            .setNode1Name(nodeName1)
//            .setNode2Name(nodeName2)
//            .setRscName(rscName);
//
//        if (uuid != null)
//        {
//            builder.setRscConnUuid(uuid.toString());
//        }
//        if (overrideProps != null)
//        {
//            builder.addAllOverrideProps(asLinStorMapEntryList(overrideProps));
//        }
//        if (delPropKeys != null)
//        {
//            builder.addAllDeletePropKeys(delPropKeys);
//        }
//        send(
//            apiCallId,
//            ApiConsts.API_MOD_RSC_CONN,
//            builder.build()
//        );
//        return apiCallId;
//    }
//
//    public long sendDeleteRscConn(String nodeName1, String nodeName2, String rscName)
//        throws IOException
//    {
//        long apiCallId = apiCallIdGen.incrementAndGet();
//        send(
//            apiCallId,
//            ApiConsts.API_DEL_RSC_CONN,
//            MsgDelRscConn.newBuilder()
//                .setNodeName1(nodeName1)
//                .setNodeName2(nodeName2)
//                .setResourceName(rscName)
//                .build()
//        );
//        return apiCallId;
//    }
//
//    /*
//     * Volume connection message
//     */
//
//    public long sendCreateVlmConn(
//        String nodeName1,
//        String nodeName2,
//        String rscName,
//        int vlmNr,
//        Map<String, String> props
//    )
//        throws IOException
//    {
//        long apiCallId = apiCallIdGen.incrementAndGet();
//        VlmConn.Builder msgBuilder = VlmConn.newBuilder()
//            .setNodeName1(nodeName1)
//            .setNodeName2(nodeName2)
//            .setResourceName(rscName)
//            .setVolumeNr(vlmNr);
//        if (props != null)
//        {
//            msgBuilder.addAllVolumeConnProps(asLinStorMapEntryList(props));
//        }
//        send(
//            apiCallId,
//            ApiConsts.API_CRT_VLM_CONN,
//            MsgCrtVlmConn.newBuilder()
//                .setVlmConn(
//                    msgBuilder.build()
//                )
//                .build()
//        );
//        return apiCallId;
//    }
//
//    public long sendModifyVlmConn(
//        UUID uuid,
//        String nodeName1,
//        String nodeName2,
//        String rscName,
//        int vlmNr,
//        Map<String, String> overrideProps,
//        Set<String> delPropKeys
//    )
//        throws IOException
//    {
//        long apiCallId = apiCallIdGen.incrementAndGet();
//        MsgModVlmConn.Builder builder = MsgModVlmConn.newBuilder()
//            .setNode1Name(nodeName1)
//            .setNode2Name(nodeName2)
//            .setRscName(rscName)
//            .setVlmNr(vlmNr);
//
//        if (uuid != null)
//        {
//            builder.setVlmConnUuid(uuid.toString());
//        }
//        if (overrideProps != null)
//        {
//            builder.addAllOverrideProps(asLinStorMapEntryList(overrideProps));
//        }
//        if (delPropKeys != null)
//        {
//            builder.addAllDeletePropKeys(delPropKeys);
//        }
//        send(
//            apiCallId,
//            ApiConsts.API_MOD_VLM_CONN,
//            builder.build()
//        );
//        return apiCallId;
//    }
//
//    public long sendDeleteVlmConn(String nodeName1, String nodeName2, String rscName, int vlmNr)
//        throws IOException
//    {
//        long apiCallId = apiCallIdGen.incrementAndGet();
//        send(
//            apiCallId,
//            ApiConsts.API_DEL_VLM_CONN,
//            MsgDelVlmConn.newBuilder()
//                .setNodeName1(nodeName1)
//                .setNodeName2(nodeName2)
//                .setResourceName(rscName)
//                .setVolumeNr(vlmNr)
//                .build()
//        );
//        return apiCallId;
//    }
//
//    @SuppressWarnings("checkstyle:magicnumber")
//    public void send(long apiCallId, String apiCall, Message msg) throws IOException
//    {
//        MsgHeader headerMsg = MsgHeader.newBuilder()
//            .setMsgType(MsgHeader.MsgType.API_CALL)
//            .setMsgContent(apiCall)
//            .setApiCallId(apiCallId)
//            .build();
//
//        ByteArrayOutputStream baos;
//        baos = new ByteArrayOutputStream();
//        headerMsg.writeDelimitedTo(baos);
//        byte[] protoHeader = baos.toByteArray();
//        baos.close();
//        baos = new ByteArrayOutputStream();
//        if (msg != null)
//        {
//            msg.writeDelimitedTo(baos);
//        }
//        byte[] protoData = baos.toByteArray();
//        baos.close();
//
//        byte[] header = new byte[16];
//        ByteBuffer byteBuffer = ByteBuffer.wrap(header);
//        byteBuffer.putInt(0, 0); // message type
//        byteBuffer.putInt(4, protoHeader.length + protoData.length);
//
//        outputStream.write(header);
//        outputStream.write(protoHeader);
//        outputStream.write(protoData);
//
//        sentCount++;
//    }
//
//    public VlmDfn createVlmDfn(Integer vlmNr, Integer minor, long vlmSize)
//    {
//        VlmDfn.Builder builder = VlmDfn.newBuilder()
//            .setVlmSize(vlmSize);
//        if (vlmNr != null)
//        {
//            builder.setVlmNr(vlmNr);
//        }
//        if (minor != null)
//        {
//            builder.setVlmMinor(minor);
//        }
//        return builder.build();
//    }
//
//    public Vlm createVlm(int vlmNr, String storPoolName, String blockDevice, String metaDisk)
//    {
//        return
//            Vlm.newBuilder()
//                .setVlmNr(vlmNr)
//                .setStorPoolName(storPoolName)
//                .setBackingDisk(blockDevice)
//                .setMetaDisk(metaDisk)
//                .build();
//    }
//
//    private Iterable<LinStorMapEntry> asLinStorMapEntryList(Map<String, String> map)
//    {
//        List<LinStorMapEntry> list = new ArrayList<>(map.size());
//        for (Map.Entry<String, String> entry : map.entrySet())
//        {
//            list.add(
//                LinStorMapEntry.newBuilder()
//                    .setKey(entry.getKey())
//                    .setValue(entry.getValue())
//                    .build()
//            );
//        }
//        return list;
//    }
//
//    private Map<String, String> asMap(List<LinStorMapEntry> list)
//    {
//        Map<String, String> map = new TreeMap<>();
//        for (LinStorMapEntry entry : list)
//        {
//            map.put(entry.getKey(), entry.getValue());
//        }
//        return map;
//    }
//}
