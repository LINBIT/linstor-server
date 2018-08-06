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
    public CtrlClientSerializerBuilder headerlessBuilder()
    {
        return builder(null, null, false);
    }

    @Override
    public CtrlClientSerializerBuilder onewayBuilder(String msgContent)
    {
        return builder(msgContent, null, false);
    }

    @Override
    public CtrlClientSerializerBuilder apiCallBuilder(String msgContent, Long apiCallId)
    {
        checkApiCallIdNotNull(apiCallId);
        return builder(msgContent, apiCallId, false);
    }

    @Override
    public CtrlClientSerializerBuilder answerBuilder(String msgContent, Long apiCallId)
    {
        checkApiCallIdNotNull(apiCallId);
        return builder(msgContent, apiCallId, true);
    }

    @Override
    public CtrlClientSerializerBuilder completionBuilder(Long apiCallId)
    {
        checkApiCallIdNotNull(apiCallId);
        return builder(null, apiCallId, false);
    }

    private CtrlClientSerializerBuilder builder(String msgContent, Long apiCallId, boolean isAnswer)
    {
        return new ProtoCtrlClientSerializerBuilder(errorReporter, serializerCtx, msgContent, apiCallId, isAnswer);
    }
}
