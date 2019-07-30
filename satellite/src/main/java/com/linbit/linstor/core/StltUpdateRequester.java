package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.UUID;

@Singleton
public class StltUpdateRequester
{
    private final ErrorReporter errorReporter;
    private final CtrlStltSerializer interComSerializer;
    private final ControllerPeerConnector controllerPeerConnector;

    @Inject
    public StltUpdateRequester(
        ErrorReporter errorReporterRef,
        CtrlStltSerializer interComSerializerRef,
        ControllerPeerConnector controllerPeerConnectorRef
    )
    {
        errorReporter = errorReporterRef;
        interComSerializer = interComSerializerRef;
        controllerPeerConnector = controllerPeerConnectorRef;
    }

    public void requestControllerUpdate()
    {
        sendRequest(
            interComSerializer
                .onewayBuilder(InternalApiConsts.API_REQUEST_CONTROLLER)
                .requestControllerUpdate()
                .build()
        );
    }

    public void requestNodeUpdate(UUID nodeUuid, NodeName nodeName)
    {
        sendRequest(
            interComSerializer
                .onewayBuilder(InternalApiConsts.API_REQUEST_NODE)
                .requestNodeUpdate(nodeUuid, nodeName.getDisplayName())
                .build()
        );
    }

    public void requestRscUpdate(UUID rscUuid, NodeName nodeName, ResourceName rscName)
    {
        sendRequest(
            interComSerializer
                .onewayBuilder(InternalApiConsts.API_REQUEST_RSC)
                .requestResourceUpdate(rscUuid, nodeName.getDisplayName(), rscName.getDisplayName())
                .build()
        );
    }

    public void requestStorPoolUpdate(UUID storPoolUuid, StorPoolName storPoolName)
    {
        sendRequest(
            interComSerializer
                .onewayBuilder(InternalApiConsts.API_REQUEST_STOR_POOL)
                .requestStoragePoolUpdate(storPoolUuid, storPoolName.getDisplayName())
                .build()
        );
    }

    public void requestSnapshotUpdate(UUID snapshotUuid, SnapshotDefinition.Key snapshotKey)
    {
        sendRequest(
            interComSerializer
                .onewayBuilder(InternalApiConsts.API_REQUEST_IN_PROGRESS_SNAPSHOT)
                .requestSnapshotUpdate(
                    snapshotKey.getResourceName().getDisplayName(),
                    snapshotUuid,
                    snapshotKey.getSnapshotName().getDisplayName()
                )
                .build()
        );
    }

    private void sendRequest(byte[] requestData)
    {
        if (requestData != null && requestData.length > 0)
        {
            Peer controllerPeer = controllerPeerConnector.getControllerPeer();
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
}
