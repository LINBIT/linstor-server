package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.StorPool;
import com.linbit.linstor.api.ApiCallRcWith;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlStorPoolListApiCallHandler;
import com.linbit.linstor.proto.FilterOuterClass.Filter;
import com.linbit.locks.LockGuard;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;

import reactor.core.publisher.Flux;

/**
 *
 * @author rpeinthor
 */
@ProtobufApiCall(
    name = ApiConsts.API_LST_STOR_POOL,
    description = "Queries the list of storage pools"
)
@Singleton
public class ListStorPool implements ApiCallReactive
{
    private final CtrlStorPoolListApiCallHandler ctrlStorPoolListApiCallHandler;
    private final ScopeRunner scopeRunner;
    private final CtrlClientSerializer clientComSerializer;
    private final Provider<Long> apiCallId;

    @Inject
    public ListStorPool(
        CtrlStorPoolListApiCallHandler ctrlStorPoolListApiCallHandlerRef,
        ScopeRunner scopeRunnerRef,
        CtrlClientSerializer clientComSerializerRef,
        @Named(ApiModule.API_CALL_ID) Provider<Long> apiCallIdRef
    )
    {
        ctrlStorPoolListApiCallHandler = ctrlStorPoolListApiCallHandlerRef;
        scopeRunner = scopeRunnerRef;
        clientComSerializer = clientComSerializerRef;
        apiCallId = apiCallIdRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn)
        throws IOException
    {
        List<String> nodeNames = Collections.emptyList();
        List<String> storPoolNames = Collections.emptyList();
        Filter filter = Filter.parseDelimitedFrom(msgDataIn);
        if (filter != null)
        {
            nodeNames = filter.getNodeNamesList();
            storPoolNames = filter.getStorPoolNamesList();
        }

        Flux<ApiCallRcWith<List<StorPool.StorPoolApi>>> result =
            ctrlStorPoolListApiCallHandler.listStorPools(nodeNames, storPoolNames);

        return result.flatMap(res ->
            scopeRunner.fluxInTransactionlessScope("Serialize storpool answers", LockGuard.createDeferred(),
                () -> Flux.just(
                        clientComSerializer
                            .answerBuilder(ApiConsts.API_LST_STOR_POOL, apiCallId.get())
                            .storPoolList(res.getValue())
                            .build())
                        .concatWith(
                            Flux.just(clientComSerializer
                                .answerBuilder(ApiConsts.API_REPLY, apiCallId.get())
                                .apiCallRcSeries(res.getApiCallRc())
                                .build()
                            )
                        )
            ));
    }
}
