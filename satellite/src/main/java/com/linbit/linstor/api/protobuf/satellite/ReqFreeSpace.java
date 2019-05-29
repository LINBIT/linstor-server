package com.linbit.linstor.api.protobuf.satellite;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.api.protobuf.serializer.ProtoCtrlStltSerializerBuilder;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.satellite.StltApiCallHandlerUtils;
import com.linbit.linstor.proto.common.StorPoolFreeSpaceOuterClass.StorPoolFreeSpace;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntFreeSpaceOuterClass.MsgIntFreeSpace;
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

import static com.linbit.linstor.api.protobuf.serializer.ProtoCommonSerializerBuilder.serializeApiCallRc;

@ProtobufApiCall(
    name = InternalApiConsts.API_REQUEST_THIN_FREE_SPACE,
    description = "Returns the free space."
)
@Singleton
public class ReqFreeSpace implements ApiCallReactive
{
    private final ScopeRunner scopeRunner;
    private final StltApiCallHandlerUtils apiCallHandlerUtils;
    private final CommonSerializer commonSerializer;
    private final Provider<Long> apiCallIdProvider;

    @Inject
    public ReqFreeSpace(
        ScopeRunner scopeRunnerRef, StltApiCallHandlerUtils apiCallHandlerUtilsRef,
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
            "Query free space",
            LockGuard.createDeferred(),
            this::executeInScope
        );
    }

    private Flux<byte[]> executeInScope()
        throws IOException
    {
        Map<StorPool, Either<SpaceInfo, ApiRcException>> freeSpaceMap = apiCallHandlerUtils.getAllSpaceInfo(true);

        MsgIntFreeSpace.Builder builder = MsgIntFreeSpace.newBuilder();
        for (Map.Entry<StorPool, Either<SpaceInfo, ApiRcException>> entry : freeSpaceMap.entrySet())
        {
            builder.addFreeSpaces(ProtoCtrlStltSerializerBuilder.buildStorPoolFreeSpace(entry).build());
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
