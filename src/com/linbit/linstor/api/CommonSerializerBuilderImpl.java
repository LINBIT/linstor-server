package com.linbit.linstor.api;

import com.linbit.linstor.api.interfaces.serializer.CommonSerializer.CommonSerializerBuilder;
import com.linbit.linstor.api.pojo.ResourceState;
import com.linbit.linstor.api.pojo.VolumeState;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.logging.ErrorReporter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class CommonSerializerBuilderImpl implements CommonSerializerBuilder
{
    protected final ErrorReporter errorReporter;
    protected final CommonSerializerWriter commonSerializationWriter;
    protected final ByteArrayOutputStream baos;
    protected boolean exceptionOccured = false;

    public CommonSerializerBuilderImpl(
        ErrorReporter errorReporterRef,
        CommonSerializerWriter commonSerializationWriterRef,
        String apiCall,
        Integer msgId
    )
    {
        errorReporter = errorReporterRef;
        commonSerializationWriter = commonSerializationWriterRef;
        baos = new ByteArrayOutputStream();

        if (apiCall != null)
        {
            try
            {
                commonSerializationWriterRef.writeHeader(apiCall, msgId, baos);
            }
            catch (IOException exc)
            {
                errorReporterRef.reportError(exc);
                exceptionOccured = true;
            }
        }
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

    @Override
    public CommonSerializerBuilder event(Integer watchId, EventIdentifier eventIdentifier)
    {
        try
        {
            commonSerializationWriter.writeEvent(watchId, eventIdentifier, baos);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        return this;
    }

    @Override
    public CommonSerializerBuilder volumeDiskState(String diskState)
    {
        try
        {
            commonSerializationWriter.writeVolumeDiskState(diskState, baos);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        return this;
    }

    @Override
    public CommonSerializerBuilder resourceStateEvent(String resourceStateString)
    {
        try
        {
            commonSerializationWriter.writeResourceStateEvent(resourceStateString, baos);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        return this;
    }

    public interface CommonSerializerWriter
    {
        void writeHeader(String apiCall, Integer msgId, ByteArrayOutputStream baos)
            throws IOException;

        void writeEvent(Integer watchId, EventIdentifier eventIdentifier, ByteArrayOutputStream baos)
        throws IOException;

        void writeVolumeDiskState(String diskState, ByteArrayOutputStream baos)
            throws IOException;

        void writeResourceStateEvent(String resourceStateString, ByteArrayOutputStream baos)
            throws IOException;
    }
}
