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
import java.util.HashMap;
import java.util.Map;

import com.google.protobuf.Message;
import com.linbit.drbdmanage.api.ApiConsts;
import com.linbit.drbdmanage.proto.MsgCrtNodeOuterClass.MsgCrtNode;
import com.linbit.drbdmanage.proto.MsgDelNodeOuterClass.MsgDelNode;
import com.linbit.drbdmanage.proto.MsgHeaderOuterClass.MsgHeader;

public class ClientProtobuf implements Runnable
{
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
        sock.close();
        shutdown = true;
        thread.interrupt();
    }

    @Override
    public void run()
    {

        StringBuilder readable = new StringBuilder();
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

                if (readable.length() == 8)
                {
                    System.out.println(readable.toString());
                    readable.setLength(0);
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
        System.out.println(readable.toString() + "\nshutting down");
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
        System.out.println("\n" + msgId + " create node");
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
        System.out.println("\n" + msgId + " delete node");
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

    public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException
    {
        ClientProtobuf client = new ClientProtobuf(9500);

        Map<String, String> props = new HashMap<>();
        props.put("TestKey", "TestValue");

        client.sendCreateNode("TestNode", props);
        Thread.sleep(500);
        client.sendDeleteNode("TestNode");

        client.outputStream.flush();

        Thread.sleep(1000);

        client.shutdown();
    }

    public static boolean isPrintableChar( char c ) {
        Character.UnicodeBlock block = Character.UnicodeBlock.of( c );
        return (!Character.isISOControl(c)) &&
                c != KeyEvent.CHAR_UNDEFINED &&
                block != null &&
                block != Character.UnicodeBlock.SPECIALS;
    }
}
