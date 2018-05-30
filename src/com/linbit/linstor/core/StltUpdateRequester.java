package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.SnapshotId;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.UUID;

@Singleton
public class StltUpdateRequester
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final CtrlStltSerializer interComSerializer;
    private final ControllerPeerConnector controllerPeerConnector;

    @Inject
    public StltUpdateRequester(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        CtrlStltSerializer interComSerializerRef,
        ControllerPeerConnector controllerPeerConnectorRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        interComSerializer = interComSerializerRef;
        controllerPeerConnector = controllerPeerConnectorRef;
    }

    public void requestControllerUpdate()
    {
        sendRequest(
            interComSerializer
            .builder(InternalApiConsts.API_REQUEST_CONTROLLER, 0)
            .requestControllerUpdate()
            .build()
        );
    }

    public void requestNodeUpdate(UUID nodeUuid, NodeName nodeName)
    {
        sendRequest(
            interComSerializer
                .builder(InternalApiConsts.API_REQUEST_NODE, 0)
                .requestNodeUpdate(nodeUuid, nodeName.getDisplayName())
                .build()
        );
    }

    public void requestRscDfnUpate(UUID rscDfnUuid, ResourceName rscName)
    {
        sendRequest(
            interComSerializer
                .builder(InternalApiConsts.API_REQUEST_RSC_DFN, 0)
                .requestResourceDfnUpdate(rscDfnUuid, rscName.getDisplayName())
                .build()
        );
    }

    public void requestRscUpdate(UUID rscUuid, NodeName nodeName, ResourceName rscName)
    {
        sendRequest(
            interComSerializer
                .builder(InternalApiConsts.API_REQUEST_RSC, 0)
                .requestResourceUpdate(rscUuid, nodeName.getDisplayName(), rscName.getDisplayName())
                .build()
        );
    }

    public void requestStorPoolUpdate(UUID storPoolUuid, StorPoolName storPoolName)
    {
        sendRequest(
            interComSerializer
                .builder(InternalApiConsts.API_REQUEST_STOR_POOL, 0)
                .requestStoragePoolUpdate(storPoolUuid, storPoolName.getDisplayName())
                .build()
        );
    }

    public void requestSnapshotUpdate(UUID snapshotUuid, SnapshotId snapshotId)
    {
        sendRequest(
            interComSerializer
                .builder(InternalApiConsts.API_REQUEST_IN_PROGRESS_SNAPSHOT, 0)
                .requestSnapshotUpdate(
                    snapshotId.getResourceName().getDisplayName(),
                    snapshotUuid,
                    snapshotId.getSnapshotName().getDisplayName()
                )
                .build()
        );
    }

    private void sendRequest(byte[] requestData)
    {
        try
        {
            if (requestData != null && requestData.length > 0)
            {
                Peer controllerPeer = controllerPeerConnector.getLocalNode().getPeer(apiCtx);
                controllerPeer.sendMessage(requestData);
            }
            else
            {
                errorReporter.reportError(
                    new ImplementationError(
                        "Failed to serialize a request ",
                        null
                    )
                );
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            errorReporter.reportError(
                new ImplementationError(
                    "StltApiCtx does not have enough privileges to send a message to controller",
                    accDeniedExc
                )
            );
        }
    }
}
