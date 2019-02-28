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
import com.linbit.linstor.proto.requests.MsgModCtrlOuterClass.MsgModCtrl;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;

@ProtobufApiCall(
    name = ApiConsts.API_SET_CTRL_PROP,
    description = "Sets a controller config property (possibly overriding old value)."
)
@Singleton
public class ModifyCtrlCfgProp implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;

    @Inject
    public ModifyCtrlCfgProp(CtrlApiCallHandler apiCallHandlerRef, ApiCallAnswerer apiCallAnswererRef)
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgModCtrl protoMsg = MsgModCtrl.parseDelimitedFrom(msgDataIn);
        ApiCallRc apiCallRc = apiCallHandler.modifyCtrl(
            ProtoMapUtils.asMap(protoMsg.getOverridePropsList()),
            new HashSet<>(protoMsg.getDeletePropKeysList()),
            new HashSet<>(protoMsg.getDelNamespacesList())
        );
        apiCallAnswerer.answerApiCallRc(apiCallRc);
    }

}
