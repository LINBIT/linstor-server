package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCallRcWith;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.api.protobuf.MaxVlmSizeCandidatePojo;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlQueryMaxVlmSizeApiCallHandler;
import com.linbit.linstor.proto.requests.MsgQryMaxVlmSizesOuterClass.MsgQryMaxVlmSizes;
import com.linbit.linstor.proto.apidata.AutoSelectFilterApiData;
import com.linbit.locks.LockGuard;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import reactor.core.publisher.Flux;

@ProtobufApiCall(
    name = ApiConsts.API_QRY_MAX_VLM_SIZE,
    description = "Queries the maximum volume size by given replica-count"
)
@Singleton
public class MaxVlmSize implements ApiCallReactive
{
    private final CtrlQueryMaxVlmSizeApiCallHandler ctrlQueryMaxVlmSizeApiCallHandler;
    private final ScopeRunner scopeRunner;
    private final CtrlClientSerializer clientComSerializer;
    private final Provider<Long> apiCallId;

    @Inject
    public MaxVlmSize(
        CtrlQueryMaxVlmSizeApiCallHandler ctrlQueryMaxVlmSizeApiCallHandlerRef,
        ScopeRunner scopeRunnerRef,
        CtrlClientSerializer clientComSerializerRef,
        @Named(ApiModule.API_CALL_ID) Provider<Long> apiCallIdRef
    )
    {
        ctrlQueryMaxVlmSizeApiCallHandler = ctrlQueryMaxVlmSizeApiCallHandlerRef;
        scopeRunner = scopeRunnerRef;
        clientComSerializer = clientComSerializerRef;
        apiCallId = apiCallIdRef;
    }


    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn)
        throws IOException
    {
        MsgQryMaxVlmSizes msgQuery = MsgQryMaxVlmSizes.parseDelimitedFrom(msgDataIn);
        Flux<ApiCallRcWith<List<MaxVlmSizeCandidatePojo>>> result = ctrlQueryMaxVlmSizeApiCallHandler.queryMaxVlmSize(
            new AutoSelectFilterApiData(msgQuery.getSelectFilter())
        );

        return result.flatMap(res ->
            scopeRunner.fluxInTransactionlessScope("Serialize max vlm answers", LockGuard.createDeferred(),
                () -> res.hasApiCallRc() ?
                    Flux.just(clientComSerializer
                        .answerBuilder(ApiConsts.API_REPLY, apiCallId.get())
                        .apiCallRcSeries(res.getApiCallRc())
                        .build()) :
                    Flux.just(clientComSerializer
                        .answerBuilder(ApiConsts.API_RSP_MAX_VLM_SIZE, apiCallId.get())
                        .maxVlmSizeCandidateList(res.getValue())
                        .build())
            ));
    }
}
