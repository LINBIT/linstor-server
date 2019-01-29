package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.netcom.Peer;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
        name = ApiConsts.API_LST_KVS,
        description = "Queries the list of kvs",
        transactional = false
)
@Singleton
public class ListKvs implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final Provider<Peer> clientProvider;
    private final Provider<Long> apiCallId;
    private final CtrlClientSerializer ctrlClientSerializer;

    @Inject
    public ListKvs(
            CtrlApiCallHandler apiCallHandlerRef,
            Provider<Peer> clientProviderRef,
            @Named(ApiModule.API_CALL_ID) Provider<Long> apiCallIdRef,
            CtrlClientSerializer ctrlClientSerializerRef)
    {
        apiCallHandler = apiCallHandlerRef;
        clientProvider = clientProviderRef;
        apiCallId = apiCallIdRef;
        ctrlClientSerializer = ctrlClientSerializerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
            throws IOException
    {
        clientProvider.get().sendMessage(
                ctrlClientSerializer
                        .answerBuilder(ApiConsts.API_LST_KVS, apiCallId.get())
                        .keyValueStoreList(apiCallHandler.listKvs()).build()
        );
    }
}
