package com.linbit.linstor.api.protobuf.serializer;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;

import javax.inject.Inject;
import javax.inject.Singleton;

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
    public CommonSerializerBuilder headerlessBuilder()
    {
        return builder(null, null, false);
    }

    @Override
    public CommonSerializerBuilder onewayBuilder(String msgContent)
    {
        return builder(msgContent, null, false);
    }

    @Override
    public CommonSerializerBuilder apiCallBuilder(String msgContent, Long apiCallId)
    {
        checkApiCallIdNotNull(apiCallId);
        return builder(msgContent, apiCallId, false);
    }

    @Override
    public CommonSerializerBuilder answerBuilder(String msgContent, Long apiCallId)
    {
        checkApiCallIdNotNull(apiCallId);
        return builder(msgContent, apiCallId, true);
    }

    @Override
    public CommonSerializerBuilder completionBuilder(Long apiCallId)
    {
        checkApiCallIdNotNull(apiCallId);
        return builder(null, apiCallId, false);
    }

    private CommonSerializerBuilder builder(@Nullable String msgContent, @Nullable Long apiCallId, boolean isAnswer)
    {
        return new ProtoCommonSerializerBuilder(errorReporter, serializerCtx, msgContent, apiCallId, isAnswer);
    }

    protected void checkApiCallIdNotNull(Long apiCallId)
    {
        if (apiCallId == null)
        {
            throw new ImplementationError("API call ID may not be null");
        }
    }
}
