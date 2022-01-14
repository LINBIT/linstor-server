package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.MsgHeaderOuterClass.MsgHeader;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.OutputStream;

@Singleton
public class ApiCallAnswerer
{
    private final CommonSerializer commonSerializer;

    private final Provider<Peer> peerProvider;
    private final Provider<Long> apiCallIdProvider;

    @Inject
    public ApiCallAnswerer(
        CommonSerializer commonSerializerRef,
        Provider<Peer> peerProviderRef,
        @Named(ApiModule.API_CALL_ID) Provider<Long> apiCallIdProviderRef
    )
    {
        commonSerializer = commonSerializerRef;
        peerProvider = peerProviderRef;
        apiCallIdProvider = apiCallIdProviderRef;
    }

    public void writeAnswerHeader(OutputStream out, String apiCallName)
        throws IOException
    {
        MsgHeader header = MsgHeader.newBuilder()
            .setMsgType(MsgHeader.MsgType.ANSWER)
            .setApiCallId(apiCallIdProvider.get())
            .setMsgContent(apiCallName)
            .build();
        header.writeDelimitedTo(out);
    }

    public void answerApiCallRc(ApiCallRc apiCallRc)
    {
        byte[] apiCallMsgData = commonSerializer.answerBuilder(ApiConsts.API_REPLY, apiCallIdProvider.get())
            .apiCallRcSeries(apiCallRc)
            .build();

        peerProvider.get().sendMessage(apiCallMsgData);
    }

    public byte[] answerBytes(byte[] protoMsgsBytes, String apiCall)
    {
        return commonSerializer
            .answerBuilder(apiCall, apiCallIdProvider.get())
            .bytes(protoMsgsBytes)
            .build();
    }

    public byte[] prepareOnewayMessage(byte[] protoMsgsBytes, String apiCall)
    {
        return commonSerializer
            .onewayBuilder(apiCall)
            .bytes(protoMsgsBytes)
            .build();
    }
}
