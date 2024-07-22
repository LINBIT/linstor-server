package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.api.protobuf.serializer.ProtoCtrlStltSerializerBuilder;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.StltApiCallHandlerUtils;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.interfaces.StorPoolInfo;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntFreeSpaceOuterClass.MsgIntFreeSpace;
import com.linbit.locks.LockGuard;
import com.linbit.utils.Either;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import reactor.core.publisher.Flux;


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
        Map<StorPoolInfo, Either<SpaceInfo, ApiRcException>> freeSpaceMap = apiCallHandlerUtils.getSpaceInfo(true);

        MsgIntFreeSpace.Builder builder = MsgIntFreeSpace.newBuilder();
        for (Map.Entry<StorPoolInfo, Either<SpaceInfo, ApiRcException>> entry : freeSpaceMap.entrySet())
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
