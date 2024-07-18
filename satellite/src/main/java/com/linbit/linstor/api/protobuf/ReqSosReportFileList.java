package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.pojo.FileInfoPojo;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.apicallhandler.StltSosReportApiCallHandler;
import com.linbit.linstor.proto.requests.MsgReqSosReportListOuterClass.MsgReqSosReportList;
import com.linbit.utils.Pair;
import com.linbit.utils.TimeUtils;

import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import com.google.inject.Inject;

@ProtobufApiCall(
    name = InternalApiConsts.API_REQ_SOS_REPORT_FILE_LIST,
    description = "Prepares the sos report and returns a file list (no content).",
    transactional = false
)
@Singleton
public class ReqSosReportFileList implements ApiCall
{
    private final StltSosReportApiCallHandler sosApiCallHandler;
    private final ControllerPeerConnector controllerPeerConnector;
    private final CtrlStltSerializer interComSerializer;
    private final Provider<Long> apiCallId;

    @Inject
    public ReqSosReportFileList(
        StltSosReportApiCallHandler sosApiCallHandlerRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        CtrlStltSerializer interComSerializerRef,
        @Named(ApiModule.API_CALL_ID) Provider<Long> apiCallIdProviderRef
    )
    {
        sosApiCallHandler = sosApiCallHandlerRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        interComSerializer = interComSerializerRef;
        apiCallId = apiCallIdProviderRef;
    }

    @Override
    public void execute(InputStream msgDataIn) throws IOException
    {
        MsgReqSosReportList reqSosReport = MsgReqSosReportList.parseDelimitedFrom(msgDataIn);
        String sosReportName = reqSosReport.getSosReportName();
        Pair<List<FileInfoPojo>, String> fileListAndErrors = sosApiCallHandler.handleSosReportRequestFileList(
            sosReportName,
            TimeUtils.millisToDate(reqSosReport.getSince())
        );

        controllerPeerConnector.getControllerPeer().sendMessage(
            interComSerializer.answerBuilder(InternalApiConsts.API_RSP_SOS_REPORT_FILE_LIST, apiCallId.get())
                .sosReportFileInfoList(
                    controllerPeerConnector.getLocalNodeName().displayValue,
                    sosReportName,
                    fileListAndErrors.objA,
                    fileListAndErrors.objB
                )
                .build(),
            InternalApiConsts.API_RSP_SOS_REPORT_FILE_LIST
        );
    }
}
