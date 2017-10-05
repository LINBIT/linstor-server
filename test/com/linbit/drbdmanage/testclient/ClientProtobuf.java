package com.linbit.drbdmanage.testclient;

import java.awt.event.KeyEvent;
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

import com.google.protobuf.Message;
import com.linbit.drbdmanage.api.ApiConsts;
import com.linbit.drbdmanage.proto.MsgCrtNodeOuterClass.MsgCrtNode;
import com.linbit.drbdmanage.proto.MsgCrtRscDfnOuterClass.MsgCrtRscDfn;
import com.linbit.drbdmanage.proto.MsgCrtVlmDfnOuterClass.VlmDfn;
import com.linbit.drbdmanage.proto.MsgDelNodeOuterClass.MsgDelNode;
import com.linbit.drbdmanage.proto.MsgDelRscDfnOuterClass.MsgDelRscDfn;
import com.linbit.drbdmanage.proto.MsgHeaderOuterClass.MsgHeader;

public class ClientProtobuf implements Runnable
{
    private static final int PAIRS_PER_LINE = 16;
    private static final int SPACE_AFTER_BYTES = 8;

    private Socket sock;
    private InputStream inputStream;
    private OutputStream outputStream;
    private int msgId = 0;

    private Thread thread;
    private boolean shutdown;
    private StringBuilder readable = new StringBuilder();

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
        sock.close();
        shutdown = true;
        thread.interrupt();
    }

    @Override
    public void run()
    {
        final int lineBrakeAfterBytes = PAIRS_PER_LINE + (PAIRS_PER_LINE > SPACE_AFTER_BYTES ? 1 : 0);
        while (!shutdown)
        {
            int read;
            try
            {
                read = inputStream.read();

                System.out.printf("%02X ", read & 0xFF);
                if (isPrintableChar((char) read))
                {
                    readable.append((char) read);
                }
                else
                {
                    readable.append('.');
                }

                if (readable.length() == lineBrakeAfterBytes)
                {
                    System.out.println(readable.toString());
                    readable.setLength(0);
                }
                else
                if (readable.length() == SPACE_AFTER_BYTES)
                {
                    readable.append(" ");
                    System.out.print(" ");
                }
            }
            catch (IOException e)
            {
                if (!shutdown)
                {
                    e.printStackTrace();
                }
            }
        }
        println("shutting down");
    }

    private boolean isPrintableChar(char c)
    {
        Character.UnicodeBlock block = Character.UnicodeBlock.of( c );
        return (!Character.isISOControl(c)) &&
            c != KeyEvent.CHAR_UNDEFINED &&
            block != null &&
            block != Character.UnicodeBlock.SPECIALS;
    }

    private void println(String str)
    {
        // first flush the current readable
        if (readable.length() > 0)
        {
            int spaces = (PAIRS_PER_LINE-readable.length())*3;
            if (PAIRS_PER_LINE > SPACE_AFTER_BYTES)
            {
                spaces++;
            }
            String format = "%" + spaces + "s%s%n%s%n";
            System.out.printf(
                format,
                "",
                readable.toString(),
                str
            );
            readable.setLength(0);
        }
        else
        {
            System.out.println(str);
        }
    }

    public int sendCreateNode(String nodeName, Map<String, String> props) throws IOException
    {
        int msgId = this.msgId++;
        send(
            msgId,
            ApiConsts.API_CRT_NODE,
            MsgCrtNode.newBuilder().
                setNodeName(nodeName).
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
            ApiConsts.API_DEL_NODE,
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
            ApiConsts.API_CRT_RSC_DFN,
            MsgCrtRscDfn.newBuilder().
                setRscName(resName).
                putAllRscProps(resDfnProps).
                addAllVlmDfnMap(vlmDfn).
                build()
        );
        return msgId;
    }

    public int sendDeleteRscDfn(String resName)
        throws IOException
    {
        int msgId = this.msgId++;
        send(
            msgId,
            ApiConsts.API_DEL_RSC_DFN,
            MsgDelRscDfn.newBuilder().
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

//        System.out.println("sending header: " + header.length);
        outputStream.write(header);
//        System.out.println("sending protoHeader:" + protoHeader.length);
        outputStream.write(protoHeader);
//        System.out.println("sending protoData:" + protoData.length);
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

    public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException
    {
        ClientProtobuf client = new ClientProtobuf(9500);

        String nodePropsTestKey = "TestNodeKey";
        String nodePropsTestValue = "TestNodeValue";
        String nodeName = "TestNode";

        String resName = "TestRes";
        String resPropsTestKey = "TestResKey";
        String resPropsTestValue = "TestResValue";

        Map<String, String> nodeProps = new HashMap<>();
        nodeProps.put(nodePropsTestKey, nodePropsTestValue);

        int msgId = 0;

        msgId = client.sendCreateNode(nodeName, nodeProps);
        client.println(msgId + " create node");
        Thread.sleep(500);

        Map<String, String> resDfnProps = new HashMap<>();
        resDfnProps.put(resPropsTestKey, resPropsTestValue);
        List<VlmDfn> vlmDfn = new ArrayList<>();
        vlmDfn.add(client.createVlmDfn(1, 1_000_000));
        msgId = client.sendCreateRscDfn(resName, resDfnProps, vlmDfn);
        client.println(msgId + " create rscDfn");
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
