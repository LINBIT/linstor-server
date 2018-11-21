package com.linbit.linstor.api.protobuf.satellite;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import java.io.IOException;
import java.io.InputStream;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.satellite.StltApiCallHandler;
import com.linbit.linstor.netcom.Peer;

/**
 *
 * @author rpeinthor
 */
@ProtobufApiCall(
    name = ApiConsts.API_HOSTNAME,
    description = "Returns the uname -n output.",
    requiresAuth = false,
    transactional = false
)
public class Hostname implements ApiCall
{
    private final StltApiCallHandler apiCallHandler;
    private final Peer client;
    private final CommonSerializer commonSerializer;

    private Provider<Long> apiCallId;

    @Inject
    public Hostname(
        StltApiCallHandler apiCallHandlerRef,
        Peer clientRef,
        CommonSerializer commonSerializerRef,
        @Named(ApiModule.API_CALL_ID) Provider<Long> apiCallIdRef)
    {
        apiCallHandler = apiCallHandlerRef;
        client = clientRef;
        commonSerializer = commonSerializerRef;
        apiCallId = apiCallIdRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        client.sendMessage(commonSerializer.answerBuilder(ApiConsts.API_HOSTNAME, apiCallId.get())
            .hostName(apiCallHandler.getHostname())
            .build()
        );
    }
}
