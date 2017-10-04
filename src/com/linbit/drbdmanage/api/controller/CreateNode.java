package com.linbit.drbdmanage.api.controller;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Map.Entry;

import com.google.protobuf.InvalidProtocolBufferException;
import com.linbit.drbdmanage.ApiCallRc;
import com.linbit.drbdmanage.api.ApiConsts;
import com.linbit.drbdmanage.api.BaseApiCall;
import com.linbit.drbdmanage.core.Controller;
import com.linbit.drbdmanage.netcom.Message;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.proto.MsgCrtNodeOuterClass.MsgCrtNode;
import com.linbit.drbdmanage.security.AccessContext;

public class CreateNode extends BaseApiCall
{
    private final Controller controller;

    public CreateNode(Controller controllerRef)
    {
        controller = controllerRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_CRT_NODE;
    }

    @Override
    public void execute(
        AccessContext accCtx,
        Message msg,
        int msgId,
        InputStream msgDataIn,
        Peer client
    )
    {
        try
        {
            MsgCrtNode msgCreateNode = MsgCrtNode.parseDelimitedFrom(msgDataIn);
            System.out.println("received msgCrtNode: ");
            System.out.println("   " + msgCreateNode.getNodeName());
            System.out.println("   " + msgCreateNode.getNodePropsCount());
            Map<String, String> nodeProps = msgCreateNode.getNodePropsMap();
            for (Entry<String, String> entry : nodeProps.entrySet())
            {
                System.out.println("   " + entry.getKey() + ": " + entry.getValue());
            }

            System.out.println("creating...");
            ApiCallRc apiCallRc = controller.getApiCallHandler().createNode(
                accCtx,
                client,
                msgCreateNode.getNodeName(),
                msgCreateNode.getNodePropsMap()
            );
        }
        catch (InvalidProtocolBufferException e)
        {
            e.printStackTrace();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

}
