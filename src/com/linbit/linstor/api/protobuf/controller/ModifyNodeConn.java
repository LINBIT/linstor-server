package com.linbit.linstor.api.protobuf.controller;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.MsgModNodeConnOuterClass.MsgModNodeConn;
import com.linbit.linstor.security.AccessContext;

@ProtobufApiCall
public class ModifyNodeConn extends BaseProtoApiCall
{
    private final Controller controller;

    public ModifyNodeConn(Controller controllerRef)
    {
        super(controllerRef.getErrorReporter());
        controller = controllerRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_MOD_NODE_CONN;
    }

    @Override
    public String getDescription()
    {
        return "Modifies a node connection";
    }

    @Override
    protected void executeImpl(
        AccessContext accCtx,
        Message msg,
        int msgId,
        InputStream msgDataIn,
        Peer client
    )
        throws IOException
    {
        MsgModNodeConn msgModNodeConn = MsgModNodeConn.parseDelimitedFrom(msgDataIn);
        UUID nodeConnUuid = null;
        if (msgModNodeConn.hasNodeConnUuid())
        {
            nodeConnUuid = UUID.fromString(msgModNodeConn.getNodeConnUuid());
        }
        String nodeName1 = msgModNodeConn.getNode1Name();
        String nodeName2 = msgModNodeConn.getNode2Name();
        Map<String, String> overrideProps = ProtoMapUtils.asMap(msgModNodeConn.getOverridePropsList());
        Set<String> deletePropKeys = new HashSet<>(msgModNodeConn.getDeletePropKeysList());

        ApiCallRc apiCallRc = controller.getApiCallHandler().modifyNodeConn(
            accCtx,
            client,
            nodeConnUuid,
            nodeName1,
            nodeName2,
            overrideProps,
            deletePropKeys
        );
        answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }

}
