package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.CtrlApiCallHandler;
import com.linbit.linstor.logging.ErrorReport;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.MsgErrorReportOuterClass.MsgErrorReport;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Set;
import java.util.TreeSet;

/**
 *
 * @author rpeinthor
 */
@ProtobufApiCall(
    name = ApiConsts.API_LST_ERROR_REPORTS,
    description = "Receives error reports from satellites and dispatches them further to the client"
)
public class ListErrorReports implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final Peer client;

    @Inject
    public ListErrorReports(CtrlApiCallHandler apiCallHandlerRef, Peer clientRef)
    {
        apiCallHandler = apiCallHandlerRef;
        client = clientRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgErrorReport msgErrorReport;
        Set<ErrorReport> errorReports = new TreeSet<>();
        while ((msgErrorReport = MsgErrorReport.parseDelimitedFrom(msgDataIn)) != null)
        {
            errorReports.add(new ErrorReport(
                msgErrorReport.getNodeNames(),
                msgErrorReport.getFilename(),
                new Date(msgErrorReport.getErrorTime()),
                msgErrorReport.getText())
            );
        }
        apiCallHandler.appendErrorReports(client, errorReports);
    }
}
