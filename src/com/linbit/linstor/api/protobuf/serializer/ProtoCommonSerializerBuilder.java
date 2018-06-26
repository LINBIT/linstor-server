package com.linbit.linstor.api.protobuf.serializer;

import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.event.generator.ResourceStateGenerator;
import com.linbit.linstor.logging.ErrorReport;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.proto.MsgApiCallResponseOuterClass;
import com.linbit.linstor.proto.MsgErrorReportOuterClass.MsgErrorReport;
import com.linbit.linstor.proto.MsgEventOuterClass;
import com.linbit.linstor.proto.MsgHeaderOuterClass;
import com.linbit.linstor.proto.MsgHostnameOuterClass.MsgHostname;
import com.linbit.linstor.proto.MsgReqErrorReportOuterClass.MsgReqErrorReport;
import com.linbit.linstor.proto.eventdata.EventRscDeploymentStateOuterClass;
import com.linbit.linstor.proto.eventdata.EventRscDfnReadyOuterClass;
import com.linbit.linstor.proto.eventdata.EventRscStateOuterClass;
import com.linbit.linstor.proto.eventdata.EventVlmDiskStateOuterClass;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

public class ProtoCommonSerializerBuilder implements CommonSerializer.CommonSerializerBuilder
{
    protected final ErrorReporter errorReporter;
    protected final AccessContext serializerCtx;
    protected final ByteArrayOutputStream baos;
    private boolean exceptionOccured;

    @Inject
    public ProtoCommonSerializerBuilder(
        final ErrorReporter errReporterRef,
        final @ApiContext AccessContext serializerCtxRef,
        String apiCall,
        Integer msgId
    )
    {
        this.errorReporter = errReporterRef;
        this.serializerCtx = serializerCtxRef;

        baos = new ByteArrayOutputStream();
        exceptionOccured = false;

        if (apiCall != null)
        {
            try
            {
                header(apiCall, msgId);
            }
            catch (IOException exc)
            {
                errorReporter.reportError(exc);
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

    public void header(String apiCall, Integer msgId) throws IOException
    {
        MsgHeaderOuterClass.MsgHeader.Builder builder = MsgHeaderOuterClass.MsgHeader.newBuilder()
            .setApiCall(apiCall);
        if (msgId != null)
        {
            builder
                .setMsgId(msgId);
        }
        builder
            .build()
            .writeDelimitedTo(baos);
    }

    protected void handleIOException(IOException exc)
    {
        errorReporter.reportError(exc);
        exceptionOccured = true;
    }

    protected void handleAccessDeniedException(AccessDeniedException accDeniedExc)
    {
        errorReporter.reportError(
            new ImplementationError(
                "ProtoInterComSerializer has not enough privileges to serialize node",
                accDeniedExc
            )
        );
        exceptionOccured = true;
    }

    @Override
    public CommonSerializer.CommonSerializerBuilder apiCallRcSeries(ApiCallRc apiCallRc)
    {
        try
        {
            for (MsgApiCallResponseOuterClass.MsgApiCallResponse protoMsg : serializeApiCallRc(apiCallRc))
            {
                protoMsg.writeDelimitedTo(baos);
            }
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CommonSerializer.CommonSerializerBuilder event(
        Integer watchId,
        long eventCounter,
        EventIdentifier eventIdentifier,
        String eventStreamAction
    )
    {
        try
        {
            MsgEventOuterClass.MsgEvent.Builder eventBuilder = MsgEventOuterClass.MsgEvent.newBuilder();

            eventBuilder
                .setWatchId(watchId)
                .setEventCounter(eventCounter)
                .setEventAction(eventStreamAction)
                .setEventName(eventIdentifier.getEventName());

            if (eventIdentifier.getResourceName() != null)
            {
                eventBuilder.setResourceName(eventIdentifier.getResourceName().displayValue);
            }

            if (eventIdentifier.getNodeName() != null)
            {
                eventBuilder.setNodeName(eventIdentifier.getNodeName().displayValue);
            }

            if (eventIdentifier.getVolumeNumber() != null)
            {
                eventBuilder.setVolumeNumber(eventIdentifier.getVolumeNumber().value);
            }

            if (eventIdentifier.getSnapshotName() != null)
            {
                eventBuilder.setSnapshotName(eventIdentifier.getSnapshotName().displayValue);
            }

            eventBuilder.build().writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CommonSerializer.CommonSerializerBuilder volumeDiskState(String diskState)
    {
        try
        {
            EventVlmDiskStateOuterClass.EventVlmDiskState.newBuilder()
                .setDiskState(diskState)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CommonSerializer.CommonSerializerBuilder resourceStateEvent(ResourceStateGenerator.UsageState usageState)
    {
        try
        {
            EventRscStateOuterClass.EventRscState.Builder builder = EventRscStateOuterClass.EventRscState.newBuilder();
            if (usageState.getResourceReady() != null)
            {
                builder.setReady(usageState.getResourceReady());
            }
            if (usageState.getInUse() != null)
            {
                builder.setInUse(usageState.getInUse());
            }

            builder.build().writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CommonSerializer.CommonSerializerBuilder resourceDeploymentStateEvent(ApiCallRc apiCallRc)
    {
        try
        {
            EventRscDeploymentStateOuterClass.EventRscDeploymentState.newBuilder()
                .addAllResponses(serializeApiCallRc(apiCallRc))
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CommonSerializer.CommonSerializerBuilder resourceDefinitionReadyEvent(int readyCount, int errorCount)
    {
        try
        {
            EventRscDfnReadyOuterClass.EventRscDfnReady.newBuilder()
                .setReadyCount(readyCount)
                .setErrorCount(errorCount)
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CommonSerializer.CommonSerializerBuilder requestErrorReports(
        Set<String> nodes,
        boolean withContent,
        Optional<Date> since,
        Optional<Date> to,
        Set<String> ids
    )
    {
        try
        {
            MsgReqErrorReport.Builder bld = MsgReqErrorReport.newBuilder();
            if (since.isPresent())
            {
                bld.setSince(since.get().getTime());
            }
            if (to.isPresent())
            {
                bld.setTo(to.get().getTime());
            }
            bld.addAllNodeNames(nodes).setWithContent(withContent).addAllIds(ids).build().writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CommonSerializer.CommonSerializerBuilder errorReports(Set<ErrorReport> errorReports)
    {
        try
        {
            for (ErrorReport errReport : errorReports)
            {
                MsgErrorReport.Builder msgErrorReport =
                    MsgErrorReport.newBuilder();

                msgErrorReport.setErrorTime(errReport.getDateTime().getTime());
                msgErrorReport.setNodeNames(errReport.getNodeName());
                msgErrorReport.setFilename(errReport.getFileName());
                if (!errReport.getText().isEmpty())
                {
                    msgErrorReport.setText(errReport.getText());
                }
                msgErrorReport.build().writeDelimitedTo(baos);
            }
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CommonSerializer.CommonSerializerBuilder hostName(String hostName)
    {
        try
        {
            MsgHostname msgHostname = MsgHostname.newBuilder().setHostname(hostName).build();
            msgHostname.writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    protected List<MsgApiCallResponseOuterClass.MsgApiCallResponse> serializeApiCallRc(ApiCallRc apiCallRc)
    {
        List<MsgApiCallResponseOuterClass.MsgApiCallResponse> list = new ArrayList<>();

        for (ApiCallRc.RcEntry apiCallEntry : apiCallRc.getEntries())
        {
            MsgApiCallResponseOuterClass.MsgApiCallResponse.Builder msgApiCallResponseBuilder =
                MsgApiCallResponseOuterClass.MsgApiCallResponse.newBuilder();

            msgApiCallResponseBuilder.setRetCode(apiCallEntry.getReturnCode());
            if (apiCallEntry.getCauseFormat() != null)
            {
                msgApiCallResponseBuilder.setCauseFormat(apiCallEntry.getCauseFormat());
            }
            if (apiCallEntry.getCorrectionFormat() != null)
            {
                msgApiCallResponseBuilder.setCorrectionFormat(apiCallEntry.getCorrectionFormat());
            }
            if (apiCallEntry.getDetailsFormat() != null)
            {
                msgApiCallResponseBuilder.setDetailsFormat(apiCallEntry.getDetailsFormat());
            }
            if (apiCallEntry.getMessageFormat() != null)
            {
                msgApiCallResponseBuilder.setMessageFormat(apiCallEntry.getMessageFormat());
            }
            msgApiCallResponseBuilder.addAllObjRefs(ProtoMapUtils.fromMap(apiCallEntry.getObjRefs()));
            msgApiCallResponseBuilder.addAllVariables(ProtoMapUtils.fromMap(apiCallEntry.getVariables()));

            MsgApiCallResponseOuterClass.MsgApiCallResponse protoMsg = msgApiCallResponseBuilder.build();

            list.add(protoMsg);
        }

        return list;
    }
}
