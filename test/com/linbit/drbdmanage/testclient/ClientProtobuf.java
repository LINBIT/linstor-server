package com.linbit.drbdmanage.testclient;

import static com.linbit.drbdmanage.ApiCallRcConstants.*;
import static com.linbit.drbdmanage.api.ApiConsts.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.protobuf.Message;
import com.linbit.drbdmanage.proto.MsgApiCallResponseOuterClass.MsgApiCallResponse;
import com.linbit.drbdmanage.proto.MsgCrtNodeOuterClass.MsgCrtNode;
import com.linbit.drbdmanage.proto.MsgCrtRscDfnOuterClass.MsgCrtRscDfn;
import com.linbit.drbdmanage.proto.MsgCrtRscOuterClass.MsgCrtRsc;
import com.linbit.drbdmanage.proto.MsgCrtRscOuterClass.Vlm;
import com.linbit.drbdmanage.proto.MsgCrtStorPoolDfnOuterClass.MsgCrtStorPoolDfn;
import com.linbit.drbdmanage.proto.MsgCrtStorPoolOuterClass.MsgCrtStorPool;
import com.linbit.drbdmanage.proto.MsgCrtVlmDfnOuterClass.VlmDfn;
import com.linbit.drbdmanage.proto.MsgDelNodeOuterClass.MsgDelNode;
import com.linbit.drbdmanage.proto.MsgDelRscDfnOuterClass.MsgDelRscDfn;
import com.linbit.drbdmanage.proto.MsgDelRscOuterClass.MsgDelRsc;
import com.linbit.drbdmanage.proto.MsgDelStorPoolDfnOuterClass.MsgDelStorPoolDfn;
import com.linbit.drbdmanage.proto.MsgDelStorPoolOuterClass.MsgDelStorPool;
import com.linbit.drbdmanage.proto.MsgHeaderOuterClass.MsgHeader;

public class ClientProtobuf implements Runnable
{
    private static final Map<Long, String> RET_CODES_TYPE = new HashMap<>();
    private static final Map<Long, String> RET_CODES_OBJ = new HashMap<>();

    static
    {
        RET_CODES_TYPE.put(MASK_ERROR, "Error");
        RET_CODES_TYPE.put(MASK_WARN, "Warn");
        RET_CODES_TYPE.put(MASK_INFO, "Info");

        RET_CODES_OBJ.put(MASK_NODE, "Node");
        RET_CODES_OBJ.put(MASK_RSC_DFN, "RscDfn");
        RET_CODES_OBJ.put(MASK_RSC, "Rsc");
        RET_CODES_OBJ.put(MASK_VLM_DFN, "VlmDfn");
        RET_CODES_OBJ.put(MASK_VLM, "Vlm");
        RET_CODES_OBJ.put(MASK_STOR_POOL_DFN, "StorPoolDfn");
        RET_CODES_OBJ.put(MASK_STOR_POOL, "StorPool");
        RET_CODES_OBJ.put(MASK_NODE_CONN, "NodeConn");
        RET_CODES_OBJ.put(MASK_RSC_CONN, "RscConn");
        RET_CODES_OBJ.put(MASK_VLM_CONN, "VlmConn");
        RET_CODES_OBJ.put(MASK_NET_IF, "NetIf");
    }

    private Socket sock;
    private InputStream inputStream;
    private OutputStream outputStream;
    private int msgId = 0;

    private Thread thread;
    private boolean shutdown;

    public ClientProtobuf(int port) throws UnknownHostException, IOException
    {
        this("127.0.0.1", port);
    }

    public ClientProtobuf(String host, int port) throws UnknownHostException, IOException
    {
        sock = new Socket(host, port);
        inputStream = sock.getInputStream();
        outputStream = sock.getOutputStream();

        shutdown = false;
        thread = new Thread(this, "ClientProtobuf");
        thread.start();
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
                sb.append("MsgId: ")
                    .append(protoHeader.getMsgId())
                    .append("\n");
                int responseIdx = 1;
                while (bais.available() > 0)
                {
                    MsgApiCallResponse response = MsgApiCallResponse.parseDelimitedFrom(bais);
                    long retCode = response.getRetCode();
                    String message = response.getMessageFormat();
                    String cause = response.getCauseFormat();
                    String correction = response.getCorrectionFormat();
                    String details = response.getDetailsFormat();
                    Map<String, String> objRefsMap = response.getObjRefsMap();
                    Map<String, String> variablesMap = response.getVariablesMap();
                    sb.append("Response ")
                        .append(responseIdx++)
                        .append(": \n   RetCode   : ");
                    decodeRetValue(sb, retCode);
                    if (message != null && !"".equals(message))
                    {
                        sb.append("\n   Message   : ").append(format(message));
                    }
                    if (details != null && !"".equals(details))
                    {
                        sb.append("\n   Details   : ").append(format(details));
                    }
                    if (cause != null && !"".equals(cause))
                    {
                        sb.append("\n   Cause     : ").append(format(cause));
                    }
                    if (correction != null && !"".equals(correction))
                    {
                        sb.append("\n   Correction: ").append(format(correction));
                    }
                    if (objRefsMap != null && !objRefsMap.isEmpty())
                    {
                        sb.append("\n   ObjRefs: ");
                        for (Entry<String, String> entry : objRefsMap.entrySet())
                        {
                            sb.append("\n      ").append(entry.getKey()).append("=").append(entry.getValue());
                        }
                    }
                    else
                    {
                        sb.append("\n   No ObjRefs defined! Report this to the dev - this should not happen!");
                    }
                    if (variablesMap != null && !variablesMap.isEmpty())
                    {
                        sb.append("\n   Variables: ");
                        for (Entry<String, String> entry : variablesMap.entrySet())
                        {
                            sb.append("\n      ").append(entry.getKey()).append("=").append(entry.getValue());
                        }
                    }
                    else
                    {
                        sb.append("\n   No Variables");
                    }
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
        println("shutting down");
    }

    private String format(String msg)
    {
        return msg.replaceAll("\\n", "\n               ");
    }

    private void decodeRetValue(StringBuilder sb, long retCode)
    {
        decode(sb, retCode, 0xC000000000000000L, RET_CODES_TYPE);
        decode(sb, retCode, 0x3C00000000000000L, RET_CODES_OBJ);
        sb.append(retCode & 0x00FFFFFFFFFFFFFFL);
        if ((retCode & 0x00FFFFFFFFFFFFFFL) == 0)
        {
            sb.append(" This looks wrong - please report to developer");
        }
    }

    private void decode(StringBuilder sb, long retCode, long mask, Map<Long, String> retCodes)
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
        System.out.println(str);
    }

    public int sendCreateNode(String nodeName, String nodeType, Map<String, String> props)
        throws IOException
    {
        int msgId = this.msgId++;
        send(
            msgId,
            API_CRT_NODE,
            MsgCrtNode.newBuilder().
                setNodeName(nodeName).
                setNodeType(nodeType).
                putAllNodeProps(props).
                build()
        );
        return msgId;
    }

    public int sendDeleteNode(String nodeName) throws IOException
    {
        int msgId = this.msgId++;
        send(
            msgId,
            API_DEL_NODE,
            MsgDelNode.newBuilder().
                setNodeName(nodeName).
                build()
        );
        return msgId;
    }

    public int sendCreateRscDfn(
        String resName,
        Map<String, String> resDfnProps,
        Iterable<? extends VlmDfn> vlmDfn
    )
        throws IOException
    {
        int msgId = this.msgId++;
        send(
            msgId,
            API_CRT_RSC_DFN,
            MsgCrtRscDfn.newBuilder().
                setRscName(resName).
                putAllRscProps(resDfnProps).
                addAllVlmDfns(vlmDfn).
                build()
        );
        return msgId;
    }

    public int sendDeleteRscDfn(String resName) throws IOException
    {
        int msgId = this.msgId++;
        send(
            msgId,
            API_DEL_RSC_DFN,
            MsgDelRscDfn.newBuilder().
                setRscName(resName).
                build()
        );
        return msgId;
    }

    public int sendCreateStorPoolDfn(String storPoolName) throws IOException
    {
        int msgId = this.msgId++;
        send(
            msgId,
            API_CRT_STOR_POOL_DFN,
            MsgCrtStorPoolDfn.newBuilder().
                setStorPoolName(storPoolName).
                build()
        );
        return msgId;
    }

    public int sendDeleteStorPoolDfn(String storPoolName) throws IOException
    {
        int msgId = this.msgId++;
        send(
            msgId,
            API_DEL_STOR_POOL_DFN,
            MsgDelStorPoolDfn.newBuilder().
                setStorPoolName(storPoolName).
                build()
        );
        return msgId;
    }

    public int sendCreateStorPool(String nodeName, String storPoolName, String driver) throws IOException
    {
        int msgId = this.msgId++;
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

    public int sendDeleteStorPool(String nodeName, String storPoolName) throws IOException
    {
        int msgId = this.msgId++;
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

    public int sendCreateRsc(
        String nodeName,
        String resName,
        Map<String, String> resProps,
        Iterable<? extends Vlm> vlms
    )
        throws IOException
    {
        int msgId = this.msgId++;
        send(
            msgId,
            API_CRT_RSC,
            MsgCrtRsc.newBuilder().
                setNodeName(nodeName).
                setRscName(resName).
                putAllRscProps(resProps).
                addAllVlms(vlms).
                build()
        );
        return msgId;
    }

    public int sendDeleteRsc(String nodeName, String resName) throws IOException
    {
        int msgId = this.msgId++;
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

    private void send(int msgId, String apiCall, Message msg) throws IOException
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
        msg.writeDelimitedTo(baos);
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
    }

    public VlmDfn createVlmDfn(int vlmNr, int vlmSize)
    {
        return
            VlmDfn.newBuilder().
            setVlmNr(vlmNr).
            setVlmSize(vlmSize).
            build();
    }

    public Vlm createVlmDfn(int vlmNr, String storPoolName, String blockDevice, String metaDisk)
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

        msgId = client.sendCreateNode(nodeName, "satellite", nodeProps);
        client.println(msgId + " create node");
        Thread.sleep(500);


        Map<String, String> resDfnProps = new HashMap<>();
        resDfnProps.put(resPropsTestKey, resPropsTestValue);
        List<VlmDfn> vlmDfn = new ArrayList<>();
        vlmDfn.add(client.createVlmDfn(1, 1_000_000));
        msgId = client.sendCreateRscDfn(resName, resDfnProps, vlmDfn);
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
        vlms.add(client.createVlmDfn(1, storPoolName, "blockDevice", "internal"));
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
}
