package com.linbit.linstor.api.protobuf.satellite;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.pojo.RscDfnPojo;
import com.linbit.linstor.api.pojo.SnapshotPojo;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.StltApiCallHandler;
import com.linbit.linstor.core.StltApiCallHandlerUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.javainternal.MsgIntSnapshotDataOuterClass.MsgIntSnapshotData;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

@ProtobufApiCall(
    name = InternalApiConsts.API_APPLY_IN_PROGRESS_SNAPSHOT,
    description = "Applies snapshot update data"
)
public class ApplySnapshot implements ApiCall
{
    private final StltApiCallHandler apiCallHandler;
    private final StltApiCallHandlerUtils apiCallHandlerUtils;
    private final ApiCallAnswerer apiCallAnswerer;
    private final ControllerPeerConnector controllerPeerConnector;
    private final Peer controllerPeer;
    private final ErrorReporter errorReporter;

    @Inject
    public ApplySnapshot(
        StltApiCallHandler apiCallHandlerRef,
        StltApiCallHandlerUtils apiCallHandlerUtilsRef,
        ApiCallAnswerer apiCallAnswererRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        Peer controllerPeerRef,
        ErrorReporter errorReporterRef
    )
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallHandlerUtils = apiCallHandlerUtilsRef;
        apiCallAnswerer = apiCallAnswererRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        controllerPeer = controllerPeerRef;
        errorReporter = errorReporterRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgIntSnapshotData snapshotData = MsgIntSnapshotData.parseDelimitedFrom(msgDataIn);

        SnapshotPojo snapshotRaw = asSnapshotPojo(snapshotData);
        apiCallHandler.applySnapshotChanges(snapshotRaw);
    }

    private SnapshotPojo asSnapshotPojo(MsgIntSnapshotData snapshotData)
    {
        return new SnapshotPojo(
            new RscDfnPojo(
                UUID.fromString(snapshotData.getRscDfnUuid()),
                snapshotData.getRscName(),
                snapshotData.getRscDfnPort(),
                snapshotData.getRscDfnSecret(),
                snapshotData.getRscDfnFlags(),
                snapshotData.getRscDfnTransportType(),
                ProtoMapUtils.asMap(snapshotData.getRscDfnPropsList()),
                null
            ),
            UUID.fromString(snapshotData.getSnapshotUuid()),
            snapshotData.getSnapshotName(),
            UUID.fromString(snapshotData.getSnapshotDfnUuid()),
            snapshotData.getSuspendResource(),
            snapshotData.getTakeSnapshot(),
            snapshotData.getFullSyncId(),
            snapshotData.getUpdateId()
        );
    }
}
