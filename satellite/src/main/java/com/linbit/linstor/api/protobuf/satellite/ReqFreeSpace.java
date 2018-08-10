package com.linbit.linstor.api.protobuf.satellite;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.satellite.StltApiCallHandlerUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.proto.StorPoolFreeSpaceOuterClass;
import com.linbit.linstor.proto.javainternal.MsgIntFreeSpaceOuterClass.MsgIntFreeSpace;
import com.linbit.linstor.storage.StorageException;
import reactor.core.publisher.Flux;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

@ProtobufApiCall(
    name = InternalApiConsts.API_REQUEST_FREE_SPACE,
    description = "Returns the free space."
)
public class ReqFreeSpace implements ApiCallReactive
{
    private final ErrorReporter errorReporter;
    private final StltApiCallHandlerUtils apiCallHandlerUtils;
    private final CommonSerializer commonSerializer;
    private final long apiCallId;

    @Inject
    public ReqFreeSpace(
        ErrorReporter errorReporterRef,
        StltApiCallHandlerUtils apiCallHandlerUtilsRef,
        CommonSerializer commonSerializerRef,
        @Named(ApiModule.API_CALL_ID) Long apiCallIdRef
    )
    {
        errorReporter = errorReporterRef;
        apiCallHandlerUtils = apiCallHandlerUtilsRef;
        commonSerializer = commonSerializerRef;
        apiCallId = apiCallIdRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn)
        throws IOException
    {
        Flux<byte[]> ret;
        try
        {
            Map<StorPool, SpaceInfo> freeSpaceMap = apiCallHandlerUtils.getSpaceInfo();

            MsgIntFreeSpace.Builder builder = MsgIntFreeSpace.newBuilder();
            for (Map.Entry<StorPool, SpaceInfo> entry : freeSpaceMap.entrySet())
            {
                StorPool storPool = entry.getKey();
                builder.addFreeSpace(
                    StorPoolFreeSpaceOuterClass.StorPoolFreeSpace.newBuilder()
                        .setStorPoolUuid(storPool.getUuid().toString())
                        .setStorPoolName(storPool.getName().displayValue)
                        .setFreeSpace(entry.getValue().freeSpace)
                        .setTotalCapacity(entry.getValue().totalCapacity)
                        .build()
                );
            }
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            builder.build().writeDelimitedTo(baos);

            ret = Flux.just(commonSerializer
                .answerBuilder(InternalApiConsts.API_REQUEST_FREE_SPACE, apiCallId)
                .bytes(baos.toByteArray())
                .build()
            );
        }
        catch (StorageException storageExc)
        {
            throw new ApiRcException(ApiCallRcImpl
                .entryBuilder(ApiConsts.FAIL_UNKNOWN_ERROR, "Failed to query free space from storage pool")
                .setCause(storageExc.getMessage())
                .build(),
                storageExc
            );
        }
        return ret;
    }
}
