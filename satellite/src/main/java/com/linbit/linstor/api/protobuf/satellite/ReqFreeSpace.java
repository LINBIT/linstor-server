package com.linbit.linstor.api.protobuf.satellite;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.satellite.StltApiCallHandlerUtils;
import com.linbit.linstor.proto.StorPoolFreeSpaceOuterClass.StorPoolFreeSpace;
import com.linbit.linstor.proto.javainternal.MsgIntFreeSpaceOuterClass.MsgIntFreeSpace;
import com.linbit.utils.Either;
import reactor.core.publisher.Flux;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static com.linbit.linstor.api.protobuf.serializer.ProtoCommonSerializerBuilder.serializeApiCallRc;

@ProtobufApiCall(
    name = InternalApiConsts.API_REQUEST_THIN_FREE_SPACE,
    description = "Returns the free space."
)
public class ReqFreeSpace implements ApiCallReactive
{
    private final StltApiCallHandlerUtils apiCallHandlerUtils;
    private final CommonSerializer commonSerializer;
    private final long apiCallId;

    @Inject
    public ReqFreeSpace(
        StltApiCallHandlerUtils apiCallHandlerUtilsRef,
        CommonSerializer commonSerializerRef,
        @Named(ApiModule.API_CALL_ID) Long apiCallIdRef
    )
    {
        apiCallHandlerUtils = apiCallHandlerUtilsRef;
        commonSerializer = commonSerializerRef;
        apiCallId = apiCallIdRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn)
        throws IOException
    {
        Map<StorPool, Either<SpaceInfo, ApiRcException>> freeSpaceMap = apiCallHandlerUtils.getAllSpaceInfo(true);

        MsgIntFreeSpace.Builder builder = MsgIntFreeSpace.newBuilder();
        for (Map.Entry<StorPool, Either<SpaceInfo, ApiRcException>> entry : freeSpaceMap.entrySet())
        {
            StorPool storPool = entry.getKey();

            StorPoolFreeSpace.Builder freeSpaceBuilder = StorPoolFreeSpace.newBuilder()
                .setStorPoolUuid(storPool.getUuid().toString())
                .setStorPoolName(storPool.getName().displayValue);

            entry.getValue().consume(
                spaceInfo -> freeSpaceBuilder
                    .setFreeCapacity(spaceInfo.freeCapacity)
                    .setTotalCapacity(spaceInfo.totalCapacity),
                apiRcException -> freeSpaceBuilder
                    // required field
                    .setFreeCapacity(0L)
                    // required field
                    .setTotalCapacity(0L)
                    .addAllErrors(serializeApiCallRc(apiRcException.getApiCallRc()))
            );

            builder.addFreeSpaces(freeSpaceBuilder.build());
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        builder.build().writeDelimitedTo(baos);

        return Flux.just(commonSerializer
            .answerBuilder(InternalApiConsts.API_REQUEST_THIN_FREE_SPACE, apiCallId)
            .bytes(baos.toByteArray())
            .build()
        );
    }
}
