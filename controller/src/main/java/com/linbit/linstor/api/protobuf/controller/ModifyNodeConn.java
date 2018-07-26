package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.proto.MsgModNodeConnOuterClass.MsgModNodeConn;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@ProtobufApiCall(
    name = ApiConsts.API_MOD_NODE_CONN,
    description = "Modifies a node connection"
)
public class ModifyNodeConn implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;

    @Inject
    public ModifyNodeConn(CtrlApiCallHandler apiCallHandlerRef, ApiCallAnswerer apiCallAnswererRef)
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
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

        ApiCallRc apiCallRc = apiCallHandler.modifyNodeConn(
            nodeConnUuid,
            nodeName1,
            nodeName2,
            overrideProps,
            deletePropKeys
        );
        apiCallAnswerer.answerApiCallRc(apiCallRc);
    }

}
