package com.linbit.linstor.api;

import com.linbit.linstor.api.interfaces.serializer.CommonSerializer.CommonSerializerBuilder;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.logging.ErrorReport;
import com.linbit.linstor.logging.ErrorReporter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;

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
    public CommonSerializerBuilder apiCallRcSeries(ApiCallRc apiCallRc)
    {
        try
        {
            commonSerializationWriter.writeApiCallRcSeries(apiCallRc, baos);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        return this;
    }

    @Override
    public CommonSerializerBuilder event(
        Integer watchId,
        EventIdentifier eventIdentifier,
        String eventStreamAction
    )
    {
        try
        {
            commonSerializationWriter.writeEvent(watchId, eventIdentifier, eventStreamAction, baos);
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
    public CommonSerializerBuilder resourceStateEvent(Boolean resourceReady)
    {
        try
        {
            commonSerializationWriter.writeResourceStateEvent(resourceReady, baos);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        return this;
    }

    @Override
    public CommonSerializerBuilder resourceDeploymentStateEvent(ApiCallRc apiCallRc)
    {
        try
        {
            commonSerializationWriter.writeResourceDeploymentStateEvent(apiCallRc, baos);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        return this;
    }

    @Override
    public CommonSerializerBuilder errorReports(Set<ErrorReport> errorReports)
    {
        try
        {
            commonSerializationWriter.writeErrorReports(errorReports, baos);
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            exceptionOccured = true;
        }
        return this;
    }

    @Override
    public CommonSerializerBuilder requestErrorReports(
        final Set<String> nodes,
        boolean withContent,
        Optional<Date> since,
        Optional<Date> to,
        final Set<String> ids)
    {
        try
        {
            commonSerializationWriter.writeRequestErrorReports(nodes, withContent, since, to, ids, baos);
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

        void writeApiCallRcSeries(ApiCallRc apiCallRc, ByteArrayOutputStream baos)
            throws IOException;

        void writeEvent(
            Integer watchId,
            EventIdentifier eventIdentifier,
            String eventStreamAction,
            ByteArrayOutputStream baos
        )
        throws IOException;

        void writeVolumeDiskState(String diskState, ByteArrayOutputStream baos)
            throws IOException;

        void writeResourceStateEvent(Boolean resourceReady, ByteArrayOutputStream baos)
            throws IOException;

        void writeResourceDeploymentStateEvent(ApiCallRc apiCallRc, ByteArrayOutputStream baos)
            throws IOException;

        void writeRequestErrorReports(
            Set<String> nodes,
            boolean withContent,
            Optional<Date> since,
            Optional<Date> to,
            Set<String> ids,
            ByteArrayOutputStream baos
        )
            throws IOException;

        void writeErrorReports(
            Set<ErrorReport> errorReports,
            ByteArrayOutputStream baos
        )
            throws IOException;
    }
}
