package com.linbit.linstor.api.protobuf.satellite;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.Volume;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.satellite.StltApiCallHandlerUtils;
import com.linbit.linstor.proto.common.FilterOuterClass;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntVlmAllocatedOuterClass.MsgIntVlmAllocated;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntVlmAllocatedOuterClass.VlmAllocated;
import com.linbit.locks.LockGuard;
import com.linbit.utils.Either;
import reactor.core.publisher.Flux;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.linbit.linstor.api.protobuf.serializer.ProtoCommonSerializerBuilder.serializeApiCallRc;

@ProtobufApiCall(
    name = InternalApiConsts.API_REQUEST_VLM_ALLOCATED,
    description = "Returns volume allocated"
)
@Singleton
public class ReqVlmAllocated implements ApiCallReactive
{
    private final ScopeRunner scopeRunner;
    private final StltApiCallHandlerUtils apiCallHandlerUtils;
    private final CommonSerializer commonSerializer;
    private final Provider<Long> apiCallIdProvider;

    @Inject
    public ReqVlmAllocated(
        ScopeRunner scopeRunnerRef,
        StltApiCallHandlerUtils apiCallHandlerUtilsRef,
        CommonSerializer commonSerializerRef,
        @Named(ApiModule.API_CALL_ID) Provider<Long> apiCallIdProviderRef
    )
    {
        scopeRunner = scopeRunnerRef;
        apiCallHandlerUtils = apiCallHandlerUtilsRef;
        commonSerializer = commonSerializerRef;
        apiCallIdProvider = apiCallIdProviderRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn)
    {
        return scopeRunner.fluxInTransactionlessScope(
            "Query volume allocated capacity",
            LockGuard.createDeferred(),
            () -> executeInScope(msgDataIn)
        );
    }

    private Flux<byte[]> executeInScope(InputStream msgDataIn)
        throws IOException
    {
        FilterOuterClass.Filter filter = FilterOuterClass.Filter.parseDelimitedFrom(msgDataIn);

        final Set<StorPoolName> storPoolsFilter =
            filter.getStorPoolNamesList().stream().map(LinstorParsingUtils::asStorPoolName).collect(Collectors.toSet());
        final Set<ResourceName> resourceFilter =
            filter.getResourceNamesList().stream().map(LinstorParsingUtils::asRscName).collect(Collectors.toSet());

        Map<Volume.Key, Either<Long, ApiRcException>> vlmAllocatedCapacities =
            apiCallHandlerUtils.getVlmAllocatedCapacities(storPoolsFilter, resourceFilter);

        MsgIntVlmAllocated.Builder builder = MsgIntVlmAllocated.newBuilder();
        for (Map.Entry<Volume.Key, Either<Long, ApiRcException>> entry : vlmAllocatedCapacities.entrySet())
        {
            Volume.Key vlmKey = entry.getKey();

            VlmAllocated.Builder vlmAllocatedBuilder = VlmAllocated.newBuilder()
                .setRscName(vlmKey.getResourceName().displayValue)
                .setVlmNr(vlmKey.getVolumeNumber().value);

            entry.getValue().consume(
                vlmAllocatedBuilder::setAllocated,
                apiRcException -> vlmAllocatedBuilder
                    .addAllErrors(serializeApiCallRc(apiRcException.getApiCallRc()))
            );

            builder.addAllocatedCapacities(vlmAllocatedBuilder.build());
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        builder.build().writeDelimitedTo(baos);

        return Flux.just(commonSerializer
            .answerBuilder(InternalApiConsts.API_REQUEST_THIN_FREE_SPACE, apiCallIdProvider.get())
            .bytes(baos.toByteArray())
            .build()
        );
    }
}
