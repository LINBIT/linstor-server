package com.linbit.linstor.api.protobuf.satellite;

import javax.inject.Inject;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.StltApiCallHandler;
import com.linbit.linstor.core.UpdateMonitor;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.javainternal.MsgIntAuthOuterClass.MsgIntAuth;
import com.linbit.linstor.proto.javainternal.MsgIntAuthSuccessOuterClass;
import com.linbit.linstor.proto.javainternal.MsgIntAuthSuccessOuterClass.MsgIntAuthSuccess;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

@ProtobufApiCall(
    name = InternalApiConsts.API_AUTH,
    description = "Called by the controller to authenticate the controller to the satellite"
)
public class CtrlAuth implements ApiCall
{
    private final StltApiCallHandler apiCallHandler;
    private final ApiCallAnswerer apiCallAnswerer;
    private final UpdateMonitor updateMonitor;
    private final Peer controllerPeer;
    private final ErrorReporter errorReporter;

    @Inject
    public CtrlAuth(
        StltApiCallHandler apiCallHandlerRef,
        ApiCallAnswerer apiCallAnswererRef,
        UpdateMonitor updateMonitorRef,
        Peer controllerPeerRef,
        ErrorReporter errorReporterRef
    )
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallAnswerer = apiCallAnswererRef;
        updateMonitor = updateMonitorRef;
        controllerPeer = controllerPeerRef;
        errorReporter = errorReporterRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        // TODO: implement authentication
        MsgIntAuth auth = MsgIntAuth.parseDelimitedFrom(msgDataIn);
        String nodeName = auth.getNodeName();
        UUID nodeUuid = UUID.fromString(auth.getNodeUuid());
        UUID disklessStorPoolDfnUuid = UUID.fromString(auth.getNodeDisklessStorPoolDfnUuid());
        UUID disklessStorPoolUuid = UUID.fromString(auth.getNodeDisklessStorPoolUuid());


        ApiCallRcImpl apiCallRc = apiCallHandler.authenticate(
            nodeUuid,
            nodeName,
            disklessStorPoolDfnUuid,
            disklessStorPoolUuid,
            controllerPeer
        );
        if (apiCallRc == null)
        {
            // all ok, send the new fullSyncId with the AUTH_ACCEPT msg
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            MsgIntAuthSuccessOuterClass.MsgIntAuthSuccess.Builder builder = MsgIntAuthSuccess.newBuilder();
            builder.setExpectedFullSyncId(updateMonitor.getNextFullSyncId());

            builder.build().writeDelimitedTo(baos);

            controllerPeer.sendMessage(
                apiCallAnswerer.prepareMessage(
                    baos.toByteArray(),
                    InternalApiConsts.API_AUTH_ACCEPT
                )
            );
        }
        else
        {
            // whatever happened should be in the apiCallRc
            controllerPeer.sendMessage(
                apiCallAnswerer.prepareMessage(
                    apiCallAnswerer.createApiCallResponse(apiCallRc),
                    InternalApiConsts.API_AUTH_ERROR
                )
            );
        }
    }
}
