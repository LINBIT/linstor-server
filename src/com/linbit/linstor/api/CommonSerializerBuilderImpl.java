package com.linbit.linstor.api;

import com.linbit.linstor.api.interfaces.serializer.CommonSerializer.CommonSerializerBuilder;
import com.linbit.linstor.logging.ErrorReporter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class CommonSerializerBuilderImpl implements CommonSerializerBuilder
{
    protected final ByteArrayOutputStream baos;
    protected final ErrorReporter errorReporter;
    protected boolean exceptionOccured = false;

    public CommonSerializerBuilderImpl(
        ErrorReporter errorReporterRef,
        CommonSerializerWriter commonSerializationWriterRef,
        String apiCall,
        int msgId
    )
    {
        baos = new ByteArrayOutputStream();

        try
        {
            commonSerializationWriterRef.writeHeader(apiCall, msgId, baos);
        }
        catch (IOException exc)
        {
            errorReporterRef.reportError(exc);
            exceptionOccured = true;
        }
        errorReporter = errorReporterRef;
    }

    @Override
    public byte[] build()
    {
        byte[] ret;
        if (exceptionOccured)
        {
            ret = new byte[0]; // do not send corrupted data
        }
        else
        {
            ret = baos.toByteArray();
        }
        return ret;
    }

    public interface CommonSerializerWriter
    {
        void writeHeader(String apiCall, int msgId, ByteArrayOutputStream baos)
            throws IOException;
    }
}
