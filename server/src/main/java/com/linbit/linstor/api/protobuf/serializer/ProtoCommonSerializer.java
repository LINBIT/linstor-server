package com.linbit.linstor.api.protobuf.serializer;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;

@Singleton
public class ProtoCommonSerializer implements CommonSerializer
{
    protected final ErrorReporter errorReporter;
    protected final AccessContext serializerCtx;

    @Inject
    public ProtoCommonSerializer(
        final ErrorReporter errReporterRef,
        final @ApiContext AccessContext serializerCtxRef
    )
    {
        this.errorReporter = errReporterRef;
        this.serializerCtx = serializerCtxRef;
    }

    @Override
    public CommonSerializerBuilder builder()
    {
        return builder(null);
    }

    @Override
    public CommonSerializerBuilder builder(String apiCall)
    {
        return builder(apiCall, null);
    }

    @Override
    public CommonSerializerBuilder builder(String apiCall, Integer msgId)
    {
        return new ProtoCommonSerializerBuilder(errorReporter, serializerCtx, apiCall, msgId);
    }
}
