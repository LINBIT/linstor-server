package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCallRcWith;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlVlmListApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.helpers.ResourceList;
import com.linbit.linstor.proto.FilterOuterClass.Filter;
import com.linbit.locks.LockGuard;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import reactor.core.publisher.Flux;

/**
 *
 * @author rpeinthor
 */
@ProtobufApiCall(
    name = ApiConsts.API_LST_VLM,
    description = "Queries the list of volumes"
)
@Singleton
public class ListVolumes implements ApiCallReactive
{
    private final CtrlVlmListApiCallHandler ctrlVlmListApiCallHandler;
    private final ScopeRunner scopeRunner;
    private final CtrlClientSerializer clientComSerializer;
    private final Provider<Long> apiCallId;

    @Inject
    public ListVolumes(
        CtrlVlmListApiCallHandler ctrlVlmListApiCallHandlerRef,
        ScopeRunner scopeRunnerRef,
        CtrlClientSerializer clientComSerializerRef,
        @Named(ApiModule.API_CALL_ID) Provider<Long> apiCallIdRef
    )
    {
        ctrlVlmListApiCallHandler = ctrlVlmListApiCallHandlerRef;
        scopeRunner = scopeRunnerRef;
        clientComSerializer = clientComSerializerRef;
        apiCallId = apiCallIdRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn)
        throws IOException
    {
        List<String> nodeNames = new ArrayList<>();
        List<String> storPoolNames = new ArrayList<>();
        List<String> resourceNames = new ArrayList<>();

        Filter filter = Filter.parseDelimitedFrom(msgDataIn);
        if (filter != null)
        {
            nodeNames = filter.getNodeNamesList();
            storPoolNames = filter.getStorPoolNamesList();
            resourceNames = filter.getResourceNamesList();
        }

        Flux<ApiCallRcWith<ResourceList>> result =
            ctrlVlmListApiCallHandler.listVlms(nodeNames, storPoolNames, resourceNames);

        return result.flatMap(res ->
            scopeRunner.fluxInTransactionlessScope("Serialize listvolume answers", LockGuard.createDeferred(),
                () -> Flux.just(
                    clientComSerializer
                        .answerBuilder(ApiConsts.API_LST_VLM, apiCallId.get())
                        .resourceList(res.getValue().getResources(), res.getValue().getSatelliteStates())
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
