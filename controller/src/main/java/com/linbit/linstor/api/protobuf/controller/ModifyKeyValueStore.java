package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.MsgModKvsOuterClass;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

@ProtobufApiCall(
        name = ApiConsts.API_MOD_KVS,
        description = "Modifies a KeyValueStore",
        transactional = false
)
@Singleton
public class ModifyKeyValueStore implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;
    private final Provider<Peer> clientProvider;
    private final Provider<Long> apiCallId;
    private final CtrlClientSerializer ctrlClientSerializer;

    @Inject
    public ModifyKeyValueStore(
            CtrlApiCallHandler apiCallHandlerRef,
            ApiCallAnswerer apiCallAnswererRef,
            Provider<Peer> clientProviderRef,
            @Named(ApiModule.API_CALL_ID) Provider<Long> apiCallIdRef,
            CtrlClientSerializer ctrlClientSerializerRef)
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
        clientProvider = clientProviderRef;
        apiCallId = apiCallIdRef;
        ctrlClientSerializer = ctrlClientSerializerRef;
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
                msgModKvs.getInstanceName(),
                ProtoMapUtils.asMap(msgModKvs.getOverridePropsList()),
                msgModKvs.getDeletePropKeysList()
        );
        apiCallAnswerer.answerApiCallRc(apiCallRc);
    }
}
