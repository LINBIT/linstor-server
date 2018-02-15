package com.linbit.linstor.api.protobuf.controller;

import com.google.inject.Inject;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.CtrlApiCallHandler;
import com.linbit.linstor.proto.MsgModVlmConnOuterClass.MsgModVlmConn;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@ProtobufApiCall(
    name = ApiConsts.API_MOD_VLM_CONN,
    description = "Modifies a volume connection"
)
public class ModifyVolumeConn implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;

    @Inject
    public ModifyVolumeConn(CtrlApiCallHandler apiCallHandlerRef, ApiCallAnswerer apiCallAnswererRef)
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgModVlmConn msgModVlmConn = MsgModVlmConn.parseDelimitedFrom(msgDataIn);
        UUID vlmConnUuid = null;
        if (msgModVlmConn.hasVlmConnUuid())
        {
            vlmConnUuid = UUID.fromString(msgModVlmConn.getVlmConnUuid());
        }
        String nodeName1 = msgModVlmConn.getNode1Name();
        String nodeName2 = msgModVlmConn.getNode2Name();
        String rscName = msgModVlmConn.getRscName();
        int vlmNr = msgModVlmConn.getVlmNr();
        Map<String, String> overrideProps = ProtoMapUtils.asMap(msgModVlmConn.getOverridePropsList());
        Set<String> deletePropKeys = new HashSet<>(msgModVlmConn.getDeletePropKeysList());

        ApiCallRc apiCallRc = apiCallHandler.modifyVlmConn(
            vlmConnUuid,
            nodeName1,
            nodeName2,
            rscName,
            vlmNr,
            overrideProps,
            deletePropKeys
        );
        apiCallAnswerer.answerApiCallRc(apiCallRc);
    }

}
