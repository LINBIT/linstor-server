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
import com.linbit.linstor.proto.requests.MsgModRscOuterClass.MsgModRsc;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@ProtobufApiCall(
    name = ApiConsts.API_MOD_RSC,
    description = "Modifies a resource"
)
@Singleton
public class ModifyResource implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;

    @Inject
    public ModifyResource(CtrlApiCallHandler apiCallHandlerRef, ApiCallAnswerer apiCallAnswererRef)
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgModRsc msgModRsc = MsgModRsc.parseDelimitedFrom(msgDataIn);
        UUID rscUuid = null;
        if (msgModRsc.hasRscUuid())
        {
            rscUuid = UUID.fromString(msgModRsc.getRscUuid());
        }
        String nodeName = msgModRsc.getNodeName();
        String rscName = msgModRsc.getRscName();
        Map<String, String> overrideProps = ProtoMapUtils.asMap(msgModRsc.getOverridePropsList());
        Set<String> deletePropKeys = new HashSet<>(msgModRsc.getDeletePropKeysList());
        Set<String> deletePropNamespace = new HashSet<>(msgModRsc.getDelNamespacesList());

        ApiCallRc apiCallRc = apiCallHandler.modifyRsc(
            rscUuid,
            nodeName,
            rscName,
            overrideProps,
            deletePropKeys,
            deletePropNamespace
        );
        apiCallAnswerer.answerApiCallRc(apiCallRc);
    }

}
