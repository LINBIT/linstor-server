package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.netcom.Peer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.google.protobuf.Message;

public class ProtoBufUtils
{
    public static void sendAnswer(Peer controllerPeer, Message msg) throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        msg.writeDelimitedTo(baos);
        controllerPeer.sendMessage(baos.toByteArray());
    }

    private ProtoBufUtils()
    {
    }
}
