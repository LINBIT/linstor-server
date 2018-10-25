package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.proto.MsgModDrbdProxyOuterClass.MsgModDrbdProxy;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@ProtobufApiCall(
    name = ApiConsts.API_MOD_DRBD_PROXY,
    description = "Modifies DRBD Proxy configuration for a resource definition"
)
public class ModifyDrbdProxy implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;

    @Inject
    public ModifyDrbdProxy(CtrlApiCallHandler apiCallHandlerRef, ApiCallAnswerer apiCallAnswererRef)
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgModDrbdProxy modDrbdProxy = MsgModDrbdProxy.parseDelimitedFrom(msgDataIn);

        UUID rscDfnUUID = modDrbdProxy.hasRscDfnUuid() ? UUID.fromString(modDrbdProxy.getRscDfnUuid()) : null;
        String rscNameStr = modDrbdProxy.getRscName();
        Map<String, String> overrideProps = ProtoMapUtils.asMap(modDrbdProxy.getOverridePropsList());
        Set<String> delProps = new HashSet<>(modDrbdProxy.getDeletePropKeysList());
        String compressionType = modDrbdProxy.hasCompressionType() ? modDrbdProxy.getCompressionType() : null;
        Map<String, String> compressionProps = ProtoMapUtils.asMap(modDrbdProxy.getCompressionPropsList());

        ApiCallRc apiCallRc = apiCallHandler.modifyDrbdProxy(
            rscDfnUUID,
            rscNameStr,
            overrideProps,
            delProps,
            compressionType,
            compressionProps
        );

        apiCallAnswerer.answerApiCallRc(apiCallRc);
    }
}
