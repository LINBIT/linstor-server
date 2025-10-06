package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.identifier.ExternalFileName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Set;
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
                .build(),
            InternalApiConsts.API_REQUEST_CONTROLLER
        );
    }

    public void requestNodeUpdate(UUID nodeUuid, NodeName nodeName)
    {
        sendRequest(
            interComSerializer
                .onewayBuilder(InternalApiConsts.API_REQUEST_NODE)
                .requestNodeUpdate(nodeUuid, nodeName.getDisplayName())
                .build(),
            InternalApiConsts.API_REQUEST_NODE
        );
    }

    public void requestRscUpdate(UUID rscUuid, NodeName nodeName, ResourceName rscName)
    {
        sendRequest(
            interComSerializer
                .onewayBuilder(InternalApiConsts.API_REQUEST_RSC)
                .requestResourceUpdate(rscUuid, nodeName.getDisplayName(), rscName.getDisplayName())
                .build(),
            InternalApiConsts.API_REQUEST_RSC
        );
    }

    public void requestStorPoolUpdate(UUID storPoolUuid, StorPool.Key spKeyRef)
    {
        sendRequest(
            interComSerializer
                .onewayBuilder(InternalApiConsts.API_REQUEST_STOR_POOL)
                .requestStoragePoolUpdate(
                    storPoolUuid,
                    spKeyRef.getNodeName().displayValue,
                    spKeyRef.getStorPoolName().displayValue
                )
                .build(),
            InternalApiConsts.API_REQUEST_STOR_POOL
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
                .build(),
            InternalApiConsts.API_REQUEST_IN_PROGRESS_SNAPSHOT
        );
    }

    public void requestSharedLocks(Set<SharedStorPoolName> sharedSPLocksRef)
    {
        sendRequest(
            interComSerializer
                .onewayBuilder(InternalApiConsts.API_REQUEST_SHARED_SP_LOCKS)
                .requestSharedStorPoolLocks(sharedSPLocksRef)
                .build(),
            InternalApiConsts.API_REQUEST_SHARED_SP_LOCKS
        );
    }

    public void requestExternalFileUpdate(UUID externalFileUuidRef, ExternalFileName externalFileNameRef)
    {
        sendRequest(
            interComSerializer
                .onewayBuilder(InternalApiConsts.API_REQUEST_EXTERNAL_FILE)
                .requestExternalFileUpdate(
                    externalFileUuidRef,
                    externalFileNameRef.extFileName
                )
                .build(),
            InternalApiConsts.API_REQUEST_EXTERNAL_FILE
        );
    }

    public void requestRemoteUpdate(UUID remoteUuidRef, RemoteName remoteNameRef)
    {
        sendRequest(
            interComSerializer
                .onewayBuilder(InternalApiConsts.API_REQUEST_REMOTE)
                .requestRemoteUpdate(
                    remoteUuidRef,
                    remoteNameRef.displayValue
                )
                .build(),
            InternalApiConsts.API_REQUEST_REMOTE
        );
    }

    private void sendRequest(byte[] requestData, String apiCall)
    {
        if (requestData != null && requestData.length > 0)
        {
            Peer controllerPeer = controllerPeerConnector.getControllerPeer();
            controllerPeer.sendMessage(requestData, apiCall);
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
