package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.proto.MsgModNodeOuterClass.MsgModNode;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@ProtobufApiCall(
    name = ApiConsts.API_MOD_NODE,
    description = "Modifies a node"
)
@Singleton
public class ModifyNode implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;

    @Inject
    public ModifyNode(CtrlApiCallHandler apiCallHandlerRef, ApiCallAnswerer apiCallAnswererRef)
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
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
        Map<String, String> overrideProps = ProtoMapUtils.asMap(msgModNode.getOverridePropsList());
        Set<String> deletePropKeys = new HashSet<>(msgModNode.getDeletePropKeysList());
        Set<String> deleteNamespaces = new HashSet<>(msgModNode.getDelNamespacesList());

        ApiCallRc apiCallRc = apiCallHandler.modifyNode(
            nodeUuid,
            nodeName,
            nodeType,
            overrideProps,
            deletePropKeys,
            deleteNamespaces
        );
        apiCallAnswerer.answerApiCallRc(apiCallRc);
    }

}
