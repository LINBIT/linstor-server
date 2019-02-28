package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.proto.requests.MsgModKvsOuterClass;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.UUID;

@ProtobufApiCall(
    name = ApiConsts.API_MOD_KVS,
    description = "Modifies a KeyValueStore",
    transactional = true
)
@Singleton
public class ModifyKeyValueStore implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;

    @Inject
    public ModifyKeyValueStore(
        CtrlApiCallHandler apiCallHandlerRef,
        ApiCallAnswerer apiCallAnswererRef
    )
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
    }

    @Override
    public void execute(InputStream msgDataIn) throws IOException
    {
        MsgModKvsOuterClass.MsgModKvs msgModKvs = MsgModKvsOuterClass.MsgModKvs.parseDelimitedFrom(msgDataIn);
        UUID kvsUuid = null;
        if (msgModKvs.hasNodeUuid())
        {
            kvsUuid = UUID.fromString(msgModKvs.getNodeUuid());
        }
        ApiCallRc apiCallRc = apiCallHandler.modifyKvs(
            kvsUuid,
            msgModKvs.getKvsName(),
            ProtoMapUtils.asMap(msgModKvs.getOverridePropsList()),
            new HashSet<>(msgModKvs.getDeletePropKeysList()),
            new HashSet<>(msgModKvs.getDelNamespacesList())
        );
        apiCallAnswerer.answerApiCallRc(apiCallRc);
    }
}
