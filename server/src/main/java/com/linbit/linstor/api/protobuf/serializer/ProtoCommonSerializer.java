package com.linbit.linstor.api.protobuf.serializer;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.proto.MsgApiCallResponseOuterClass;
import com.linbit.linstor.security.AccessContext;

import java.io.ByteArrayInputStream;
import java.io.IOException;

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

    @Override
    public ApiCallRc parseApiCallRc(ByteArrayInputStream msgApiCallRc) throws IOException
    {
        MsgApiCallResponseOuterClass.MsgApiCallResponse apiCallResponse =
            MsgApiCallResponseOuterClass.MsgApiCallResponse.parseDelimitedFrom(msgApiCallRc);

        return ApiCallRcImpl.singletonApiCallRc(ApiCallRcImpl
            .entryBuilder(
                apiCallResponse.getRetCode(),
                apiCallResponse.getMessage()
            )
            .setCause(apiCallResponse.getCause())
            .setCorrection(apiCallResponse.getCorrection())
            .setDetails(apiCallResponse.getDetails())
            .build()
        );
    }

    private CommonSerializerBuilder builder(String msgContent, Long apiCallId, boolean isAnswer)
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
