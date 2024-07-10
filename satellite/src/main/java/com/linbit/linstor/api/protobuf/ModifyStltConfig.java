package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.apicallhandler.StltApiCallHandler;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.common.StltConfigOuterClass.StltConfig;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = InternalApiConsts.API_MOD_STLT_CONFIG,
    description = "modifies the specified satellite config",
    transactional = false
)
@Singleton
public class ModifyStltConfig implements ApiCall
{
    private final StltApiCallHandler apiCallHandler;
    private final CtrlStltSerializer interComSerializer;
    private final Provider<Peer> peerProvider;
    private final Provider<Long> apiCallId;

    @Inject
    public ModifyStltConfig(
        StltApiCallHandler apiCallHandlerRef,
        CtrlStltSerializer interComSerializerRef,
        Provider<Peer> peerProviderRef,
        @Named(ApiModule.API_CALL_ID) Provider<Long> apiCallIdRef
    )
    {
        apiCallHandler = apiCallHandlerRef;
        interComSerializer = interComSerializerRef;
        peerProvider = peerProviderRef;
        apiCallId = apiCallIdRef;
    }

    @Override
    public void execute(InputStream msgDataIn) throws IOException
    {
        StltConfig stltConf = StltConfig.parseDelimitedFrom(msgDataIn);
        final boolean appliedFlag = apiCallHandler.modifyStltConfig(stltConf);
        byte[] msg = interComSerializer.answerBuilder(InternalApiConsts.API_MOD_STLT_CONFIG_RESP, apiCallId.get())
            .stltConfigApplied(appliedFlag).build();
        peerProvider.get().sendMessage(msg, InternalApiConsts.API_MOD_STLT_CONFIG_RESP);
    }
}
