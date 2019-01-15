package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.netcom.Peer;

import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author rpeinthor
 */
@ProtobufApiCall(
    name = ApiConsts.API_LST_RSC_DFN,
    description = "Queries the list of resource definitions",
    transactional = false
)
@Singleton
public class ListResourceDefinition implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final Provider<Peer> clientProvider;
    private final Provider<Long> apiCallId;
    private final CtrlClientSerializer ctrlClientSerializer;

    @Inject
    public ListResourceDefinition(
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

        byte[] response = ctrlClientSerializer
            .answerBuilder(ApiConsts.API_LST_RSC_DFN, apiCallId.get())
            .resourceDfnList(apiCallHandler.listResourceDefinition())
            .build();

        clientProvider.get().sendMessage(response);
    }
}
