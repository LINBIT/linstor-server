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
import com.linbit.linstor.proto.requests.MsgModRscConnOuterClass.MsgModRscConn;

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
@Singleton
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
        Set<String> deletePropNamespace = new HashSet<>(msgModRscConn.getDelNamespacesList());

        ApiCallRc apiCallRc = apiCallHandler.modifyRscConn(
            rscConnUuid,
            nodeName1,
            nodeName2,
            rscName,
            overrideProps,
            deletePropKeys,
            deletePropNamespace
        );
        apiCallAnswerer.answerApiCallRc(apiCallRc);
    }

}
