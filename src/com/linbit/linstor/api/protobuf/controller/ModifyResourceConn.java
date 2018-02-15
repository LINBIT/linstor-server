package com.linbit.linstor.api.protobuf.controller;

import com.google.inject.Inject;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.CtrlApiCallHandler;
import com.linbit.linstor.proto.MsgModRscConnOuterClass.MsgModRscConn;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@ProtobufApiCall(
    name = ApiConsts.API_MOD_RSC_CONN,
    description = "Modifies a resource connection"
)
public class ModifyResourceConn implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;

    @Inject
    public ModifyResourceConn(CtrlApiCallHandler apiCallHandlerRef, ApiCallAnswerer apiCallAnswererRef)
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgModRscConn msgModRscConn = MsgModRscConn.parseDelimitedFrom(msgDataIn);
        UUID rscConnUuid = null;
        if (msgModRscConn.hasRscConnUuid())
        {
            rscConnUuid = UUID.fromString(msgModRscConn.getRscConnUuid());
        }
        String nodeName1 = msgModRscConn.getNode1Name();
        String nodeName2 = msgModRscConn.getNode2Name();
        String rscName = msgModRscConn.getRscName();
        Map<String, String> overrideProps = ProtoMapUtils.asMap(msgModRscConn.getOverridePropsList());
        Set<String> deletePropKeys = new HashSet<>(msgModRscConn.getDeletePropKeysList());

        ApiCallRc apiCallRc = apiCallHandler.modifyRscConn(
            rscConnUuid,
            nodeName1,
            nodeName2,
            rscName,
            overrideProps,
            deletePropKeys
        );
        apiCallAnswerer.answerApiCallRc(apiCallRc);
    }

}
