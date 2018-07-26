package com.linbit.linstor.api.protobuf.serializer;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;

@Singleton
public class ProtoCtrlClientSerializer extends ProtoCommonSerializer
    implements CtrlClientSerializer
{
    @Inject
    public ProtoCtrlClientSerializer(
        final ErrorReporter errReporterRef,
        final @ApiContext AccessContext serializerCtxRef
    )
    {
        super(errReporterRef, serializerCtxRef);
    }

    @Override
    public CtrlClientSerializerBuilder builder()
    {
        return builder(null);
    }

    @Override
    public CtrlClientSerializerBuilder builder(String apiCall)
    {
        return builder(apiCall, null);
    }

    @Override
    public CtrlClientSerializerBuilder builder(String apiCall, Integer msgId)
    {
        return new ProtoCtrlClientSerializerBuilder(errorReporter, serializerCtx, apiCall, msgId);
    }
}
