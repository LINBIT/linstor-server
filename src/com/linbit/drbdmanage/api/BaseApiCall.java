package com.linbit.drbdmanage.api;

import com.google.protobuf.GeneratedMessageV3;
import com.linbit.drbdmanage.ApiCall;
import com.linbit.drbdmanage.ApiCallRc;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.proto.MsgHeaderOuterClass.MsgHeader;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Base class for network APIs
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public abstract class BaseApiCall implements ApiCall
{
    protected void writeMsgHeader(OutputStream out, int msgId, String apiCallName)
        throws IOException
    {
        MsgHeader.Builder headerBld = MsgHeader.newBuilder();
        headerBld.setMsgId(msgId);
        headerBld.setApiCall(apiCallName);
        MsgHeader header = headerBld.build();
        header.writeDelimitedTo(out);
    }

    protected void writeMsgContent(OutputStream out, GeneratedMessageV3 protobufsMsg)
        throws IOException
    {
        protobufsMsg.writeDelimitedTo(out);
    }

    protected void answerApiCallRc(Peer peer, ApiCallRc apiCallRc)
    {
//        peer.sendMessage(msg);
    }
}
