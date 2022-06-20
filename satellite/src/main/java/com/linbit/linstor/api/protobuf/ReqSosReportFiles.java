package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.pojo.FilePojo;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.apicallhandler.StltSosReportApiCallHandler;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.proto.requests.MsgReqSosReportFilesOuterClass.MsgReqSosReportFiles;

import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import com.google.inject.Inject;

@ProtobufApiCall(
    name = InternalApiConsts.API_REQ_SOS_REPORT_FILES,
    description = "Returns the requested files of the specified SOS report.",
    transactional = false
)
@Singleton
public class ReqSosReportFiles implements ApiCall
{
    private final ErrorReporter errorReporter;
    private final StltSosReportApiCallHandler sosApiCallHandler;
    private final ControllerPeerConnector controllerPeerConnector;
    private final CtrlStltSerializer interComSerializer;
    private final Provider<Long> apiCallId;

    @Inject
    public ReqSosReportFiles(
        ErrorReporter errorReporterRef,
        StltSosReportApiCallHandler sosApiCallHandlerRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        CtrlStltSerializer interComSerializerRef,
        @Named(ApiModule.API_CALL_ID) Provider<Long> apiCallIdProviderRef
    )
    {
        errorReporter = errorReporterRef;
        sosApiCallHandler = sosApiCallHandlerRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        interComSerializer = interComSerializerRef;
        apiCallId = apiCallIdProviderRef;
    }

    @Override
    public void execute(InputStream msgDataIn) throws IOException
    {
        MsgReqSosReportFiles reqSosReportFiles = MsgReqSosReportFiles.parseDelimitedFrom(msgDataIn);
        String sosReportName = reqSosReportFiles.getSosReportName();
        List<FilePojo> filesToRespond = sosApiCallHandler.getRequestedSosReportFiles(
            ProtoDeserializationUtils.parseRequestedSosFiles(reqSosReportFiles.getFilesList())
        );

        byte[] build = interComSerializer.answerBuilder(InternalApiConsts.API_RSP_SOS_REPORT_FILE_LIST, apiCallId.get())
            .sosReportFiles(controllerPeerConnector.getLocalNodeName().displayValue, sosReportName, filesToRespond)
            .build();
        errorReporter.logTrace("Responding (partial) sos-report %s, bytes: %d", sosReportName, build.length);
        controllerPeerConnector.getControllerPeer().sendMessage(
            build,
            InternalApiConsts.API_RSP_SOS_REPORT_FILE_LIST
        );
    }
}
