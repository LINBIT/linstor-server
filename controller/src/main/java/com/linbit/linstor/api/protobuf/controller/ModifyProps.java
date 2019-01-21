package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.proto.MsgModKvsPropsOuterClass.MsgModKvsProps;
import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = ApiConsts.API_MOD_KVS_PROPS,
    description = "Creates, modifies or deletes given properties"
)
@Singleton
public class ModifyProps implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;

    @Inject
    public ModifyProps(CtrlApiCallHandler apiCallHandlerRef, ApiCallAnswerer apiCallAnswererRef)
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
    }

    @Override
    public void execute(InputStream msgDataIn) throws IOException
    {
        MsgModKvsProps msgModProps = MsgModKvsProps.parseDelimitedFrom(msgDataIn);
        ApiCallRc apiCallRc = apiCallHandler.modifyKvsProps(
            msgModProps.getInstanceName(),
            ProtoMapUtils.asMap(msgModProps.getModPropsList()),
            msgModProps.getDelPropsList()
        );
        apiCallAnswerer.answerApiCallRc(apiCallRc);
    }
}
