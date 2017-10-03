package com.linbit.drbdmanage.testclient;

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
import com.linbit.drbdmanage.proto.MsgHeaderOuterClass.MsgHeader;

public class ClientProtobuf
{
    private Socket sock;
    private InputStream inputStream;
    private OutputStream outputStream;

    public ClientProtobuf(int port) throws UnknownHostException, IOException
    {
        this("127.0.0.1", port);
    }

    public ClientProtobuf(String host, int port) throws UnknownHostException, IOException
    {
        sock = new Socket(host, port);
        inputStream = sock.getInputStream();
        outputStream = sock.getOutputStream();
    }

    public void shutdown() throws IOException
    {
        sock.close();
    }

    public void sendCreateNode(String nodeName, Map<String, String> props) throws IOException
    {
        send(
            MsgHeader.newBuilder().
                setApiCall(ApiConsts.API_CRT_NODE).
                setMsgId(0).
                build(),
            MsgCrtNode.newBuilder().
                setNodeName(nodeName).
                putAllNodeProps(props).
                build()
        );
    }

    private void send(MsgHeader headerMsg, Message msg) throws IOException
    {
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

        client.outputStream.flush();

        client.shutdown();
    }
}
