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
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.MsgModNodeOuterClass.MsgModNode;
import com.linbit.linstor.security.AccessContext;

@ProtobufApiCall
public class ModifyNode extends BaseProtoApiCall
{
    private final Controller controller;

    public ModifyNode(Controller controller)
    {
        super(controller.getErrorReporter());
        this.controller = controller;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_MOD_NODE;
    }

    @Override
    public String getDescription()
    {
        return "Modifies a node";
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
        MsgModNode msgModNode = MsgModNode.parseDelimitedFrom(msgDataIn);
        UUID nodeUuid = null;
        if (msgModNode.hasNodeUuid())
        {
            nodeUuid = UUID.fromString(msgModNode.getNodeUuid());
        }
        String nodeName = msgModNode.getNodeName();
        String nodeType = null;
        if (msgModNode.hasNodeType())
        {
            nodeType = msgModNode.getNodeType();
        }
        Map<String, String> overrideProps = asMap(msgModNode.getOverridePropsList());
        Set<String> deletePropKeys = new HashSet<>(msgModNode.getDeletePropKeysList());

        ApiCallRc apiCallRc = controller.getApiCallHandler().modifyNode(
            accCtx,
            client,
            nodeUuid,
            nodeName,
            nodeType,
            overrideProps,
            deletePropKeys
        );
        answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }

}
