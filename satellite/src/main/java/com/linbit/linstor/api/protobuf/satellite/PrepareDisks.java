package com.linbit.linstor.api.protobuf.satellite;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.satellite.StltApiCallHandler;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.requests.MsgPrepareDisksOuterClass.MsgPrepareDisks;


import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author rpeinthor
 */
@ProtobufApiCall(
    name = ApiConsts.API_PREPARE_DISKS,
    description = "Detects NVME or PMEM and prepares them for Linstor.",
    requiresAuth = false,
    transactional = false
)
@Singleton
public class PrepareDisks implements ApiCall
{
    private final StltApiCallHandler apiCallHandler;
    private final Provider<Peer> peerProvider;
    private final CommonSerializer commonSerializer;

    private Provider<Long> apiCallId;

    @Inject
    public PrepareDisks(
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
        MsgPrepareDisks msgPrepareDisks = MsgPrepareDisks.parseDelimitedFrom(msgDataIn);

        ApiCallRc apiCallRc = apiCallHandler.prepareDisks(
            msgPrepareDisks.getNvmeFilter(),
            msgPrepareDisks.getDetectPmem()
        );
        peerProvider.get().sendMessage(
            commonSerializer
                .answerBuilder(ApiConsts.API_REPLY, apiCallId.get())
                .apiCallRcSeries(apiCallRc)
                .build()
        );
    }
}
