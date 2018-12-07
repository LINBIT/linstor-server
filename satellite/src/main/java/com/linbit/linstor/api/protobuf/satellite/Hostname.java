package com.linbit.linstor.api.protobuf.satellite;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
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
@Singleton
public class Hostname implements ApiCall
{
    private final StltApiCallHandler apiCallHandler;
    private final Provider<Peer> peerProvider;
    private final CommonSerializer commonSerializer;

    private Provider<Long> apiCallId;

    @Inject
    public Hostname(
        StltApiCallHandler apiCallHandlerRef,
        @Named(ApiModule.API_CALL_ID) Provider<Long> apiCallIdRef,
        Provider<Peer> peerProviderRef, CommonSerializer commonSerializerRef
    )
    {
        apiCallHandler = apiCallHandlerRef;
        peerProvider = peerProviderRef;
        commonSerializer = commonSerializerRef;
        apiCallId = apiCallIdRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        peerProvider.get().sendMessage(commonSerializer.answerBuilder(ApiConsts.API_HOSTNAME, apiCallId.get())
            .hostName(apiCallHandler.getHostname())
            .build()
        );
    }
}
