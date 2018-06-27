package com.linbit.linstor.api.protobuf.satellite;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.satellite.StltApiCallHandler;
import com.linbit.linstor.netcom.IllegalMessageStateException;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.MsgHeaderOuterClass.MsgHeader;
import com.linbit.linstor.proto.MsgHostnameOuterClass.MsgHostname;

/**
 *
 * @author rpeinthor
 */
@ProtobufApiCall(
    name = ApiConsts.API_HOSTNAME,
    description = "Returns the uname -n output.",
    requiresAuth = false
)
public class Hostname implements ApiCall
{
    private final StltApiCallHandler apiCallHandler;
    private final Peer client;
    private final CommonSerializer commonSerializer;

    private Provider<Integer> msgId;

    @Inject
    public Hostname(
        StltApiCallHandler apiCallHandlerRef,
        Peer clientRef,
        CommonSerializer commonSerializerRef,
        @Named(ApiModule.MSG_ID) Provider<Integer> msgIdRef)
    {
        apiCallHandler = apiCallHandlerRef;
        client = clientRef;
        commonSerializer = commonSerializerRef;
        msgId = msgIdRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        client.sendMessage(commonSerializer.builder(ApiConsts.API_HOSTNAME, msgId.get())
            .hostName(apiCallHandler.getHostname())
            .build()
        );
    }
}
