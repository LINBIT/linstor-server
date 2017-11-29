package com.linbit.linstor.core;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeConnection;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceConnection;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeConnection;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeDefinition.VlmDfnApi;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiType;
import com.linbit.linstor.api.interfaces.serializer.CtrlAuthSerializer;
import com.linbit.linstor.api.interfaces.serializer.CtrlSerializer;
import com.linbit.linstor.api.protobuf.controller.serializer.AuthSerializerProto;
import com.linbit.linstor.api.protobuf.controller.serializer.ResourceDataSerializerProto;
import com.linbit.linstor.api.protobuf.controller.serializer.StorPoolDataSerializerProto;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;

public class CtrlApiCallHandler
{
    private final CtrlAuthenticationApiCallHandler authApiCallHandler;
    private final CtrlNodeApiCallHandler nodeApiCallHandler;
    private final CtrlRscDfnApiCallHandler rscDfnApiCallHandler;
    private final CtrlVlmDfnApiCallHandler vlmDfnApiCallHandler;
    private final CtrlRscApiCallHandler rscApiCallHandler;
    private final CtrlStorPoolDfnApiCallHandler storPoolDfnApiCallHandler;
    private final CtrlStorPoolApiCallHandler storPoolApiCallHandler;
    private final CtrlNodeConnectionApiCallHandler nodeConnApiCallHandler;
    private final CtrlRscConnectionApiCallHandler rscConnApiCallHandler;
    private final CtrlVlmConnectionApiCallHandler vlmConnApiCallHandler;

    private final Controller controller;

    CtrlApiCallHandler(Controller controllerRef, ApiType type, AccessContext apiCtx)
    {
        controller = controllerRef;
        ErrorReporter errorReporter = controller.getErrorReporter();
        CtrlAuthSerializer authSerializer;
        CtrlSerializer<Resource> rscSerializer;
        CtrlSerializer<StorPool> storPoolSerializer;
        switch (type)
        {
            case PROTOBUF:
                authSerializer = new AuthSerializerProto();
                rscSerializer = new ResourceDataSerializerProto(apiCtx, errorReporter);
                storPoolSerializer = new StorPoolDataSerializerProto(apiCtx, errorReporter);
                break;
            default:
                throw new ImplementationError("Unknown ApiType: " + type, null);
        }
        authApiCallHandler = new CtrlAuthenticationApiCallHandler(controllerRef, authSerializer);
        nodeApiCallHandler = new CtrlNodeApiCallHandler(controllerRef, apiCtx);
        rscDfnApiCallHandler = new CtrlRscDfnApiCallHandler(controllerRef, apiCtx);
        vlmDfnApiCallHandler = new CtrlVlmDfnApiCallHandler(controllerRef, rscSerializer, apiCtx);
        rscApiCallHandler = new CtrlRscApiCallHandler(controllerRef, rscSerializer, apiCtx);
        storPoolDfnApiCallHandler = new CtrlStorPoolDfnApiCallHandler(controllerRef);
        storPoolApiCallHandler = new CtrlStorPoolApiCallHandler(controllerRef, storPoolSerializer, apiCtx);
        nodeConnApiCallHandler = new CtrlNodeConnectionApiCallHandler(controllerRef);
        rscConnApiCallHandler = new CtrlRscConnectionApiCallHandler(controllerRef);
        vlmConnApiCallHandler = new CtrlVlmConnectionApiCallHandler(controllerRef);
    }

    public void completeSatelliteAuthentication(Peer peer)
    {
        // no locks needed
        authApiCallHandler.completeAuthentication(peer);
    }

    /**
     * Creates a new {@link Node}
     *
     * @param accCtx
     * @param client
     * @param nodeNameStr required
     * @param nodeTypeStr required
     * @param props not null, might be empty
     * @return
     */
    public ApiCallRc createNode(
        AccessContext accCtx,
        Peer client,
        String nodeNameStr,
        String nodeTypeStr,
        Map<String, String> props
    )
    {
        ApiCallRc apiCallRc;
        if (props == null)
        {
            props = Collections.emptyMap();
        }
        try
        {
            controller.nodesMapLock.writeLock().lock();
            apiCallRc = nodeApiCallHandler.createNode(
                accCtx,
                client,
                nodeNameStr,
                nodeTypeStr,
                props
            );
        }
        finally
        {
            controller.nodesMapLock.writeLock().unlock();
        }
        return apiCallRc;
    }

    /**
     * Marks the given {@link Node} for deletion.
     *
     * The node is only deleted once the satellite confirms that it has no more
     * {@link Resource}s and {@link StorPool}s deployed.
     *
     * @param accCtx
     * @param client
     * @param nodeName required
     * @return
     */
    public ApiCallRc deleteNode(AccessContext accCtx, Peer client, String nodeName)
    {
        ApiCallRc apiCallRc;
        try
        {
            controller.nodesMapLock.writeLock().lock();
            apiCallRc = nodeApiCallHandler.deleteNode(accCtx, client, nodeName);
        }
        finally
        {
            controller.nodesMapLock.writeLock().unlock();

        }
        return apiCallRc;
    }

    /**
     * Creates new {@link ResourceDefinition}
     *
     * @param accCtx
     * @param client
     * @param resourceName required
     * @param port optional
     * @param secret optional
     * @param props optional
     * @param volDescrMap optional
     * @return
     */
    public ApiCallRc createResourceDefinition(
        AccessContext accCtx,
        Peer client,
        String resourceName,
        Integer port,
        String secret,
        Map<String, String> props,
        List<VolumeDefinition.VlmDfnApi> volDescrMap
    )
    {
        ApiCallRc apiCallRc;
        if (port == null)
        {
            port = 8042; // FIXME find free port with poolAllocator
        }
        if (secret == null || secret.trim().equals(""))
        {
            secret = controller.generateSharedSecret();
        }
        if (props == null)
        {
            props = Collections.emptyMap();
        }
        if (volDescrMap == null)
        {
            volDescrMap = Collections.emptyList();
        }
        try
        {
            controller.rscDfnMapLock.writeLock().lock();
            apiCallRc = rscDfnApiCallHandler.createResourceDefinition(
                accCtx,
                client,
                resourceName,
                port,
                secret,
                props,
                volDescrMap
            );
        }
        finally
        {
            controller.rscDfnMapLock.writeLock().unlock();
        }
        return apiCallRc;
    }

    /**
     * Marks a {@link ResourceDefinition} for deletion.
     *
     * It will only be removed when all satellites confirm the deletion of the corresponding
     * {@link Resource}s.
     *
     * @param accCtx
     * @param client
     * @param resourceName required
     * @return
     */
    public ApiCallRc deleteResourceDefinition(
        AccessContext accCtx,
        Peer client,
        String resourceName
    )
    {
        ApiCallRc apiCallRc;
        try
        {
            controller.rscDfnMapLock.writeLock().lock();
            apiCallRc = rscDfnApiCallHandler.deleteResourceDefinition(
                accCtx,
                client,
                resourceName
            );
        }
        finally
        {
            controller.rscDfnMapLock.writeLock().unlock();
        }
        return apiCallRc;
    }

    /**
     * Creates new {@link VolumeDefinition}s for a given {@link ResourceDefinition}.
     *
     * @param accCtx
     * @param client
     * @param rscName required
     * @param vlmDfnApiList optional
     * @return
     */
    public ApiCallRc createVlmDfns(
        AccessContext accCtx,
        Peer client,
        String rscName,
        List<VlmDfnApi> vlmDfnApiList
    )
    {
        ApiCallRc apiCallRc;
        if (vlmDfnApiList == null)
        {
            vlmDfnApiList = Collections.emptyList();
        }
        try
        {
            controller.rscDfnMapLock.writeLock().lock();
            apiCallRc = vlmDfnApiCallHandler.createVolumeDefinitions(
                accCtx,
                client,
                rscName,
                vlmDfnApiList
            );
        }
        finally
        {
            controller.rscDfnMapLock.writeLock().unlock();
        }
        return apiCallRc;
    }

    // TODO: deleteVlmDfns

    /**
     * Creates a new {@link Resource}
     *
     * @param accCtx
     * @param client
     * @param nodeName required
     * @param rscName required
     * @param rscPropsMap optional
     * @param vlmApiDataList optional
     * @return
     */
    public ApiCallRc createResource(
        AccessContext accCtx,
        Peer client,
        String nodeName,
        String rscName,
        Map<String, String> rscPropsMap,
        List<Volume.VlmApi> vlmApiDataList
    )
    {
        ApiCallRc apiCallRc;
        if (rscPropsMap == null)
        {
            rscPropsMap = Collections.emptyMap();
        }
        if (vlmApiDataList == null)
        {
            vlmApiDataList = Collections.emptyList();
        }
        try
        {
            controller.nodesMapLock.writeLock().lock();
            controller.rscDfnMapLock.writeLock().lock();

            apiCallRc = rscApiCallHandler.createResource(
                accCtx,
                client,
                nodeName,
                rscName,
                rscPropsMap,
                vlmApiDataList
            );
        }
        finally
        {
            controller.rscDfnMapLock.writeLock().unlock();
            controller.nodesMapLock.writeLock().unlock();
        }

        return apiCallRc;
    }

    /**
     * Marks a {@link Resource} for deletion.
     *
     * The {@link Resource} is only deleted once the corresponding satellite confirmed
     * that it has undeployed (deleted) the {@link Resource}
     *
     * @param accCtx
     * @param client
     * @param nodeName required
     * @param rscName required
     * @return
     */
    public ApiCallRc deleteResource(
        AccessContext accCtx,
        Peer client,
        String nodeName,
        String rscName
    )
    {
        ApiCallRc apiCallRc;
        try
        {
            controller.nodesMapLock.writeLock().lock();
            controller.rscDfnMapLock.writeLock().lock();

            apiCallRc = rscApiCallHandler.deleteResource(
                accCtx,
                client,
                nodeName,
                rscName
            );
        }
        finally
        {
            controller.rscDfnMapLock.writeLock().unlock();
            controller.nodesMapLock.writeLock().unlock();
        }

        return apiCallRc;
    }

    /**
     * Creates a new {@link StorPoolDefinition}.
     *
     * @param accCtx
     * @param client
     * @param storPoolName required
     * @param storPoolDfnPropsMap optional
     * @return
     */
    public ApiCallRc createStoragePoolDefinition(
        AccessContext accCtx,
        Peer client,
        String storPoolName,
        Map<String, String> storPoolDfnPropsMap
    )
    {
        ApiCallRc apiCallRc;
        if (storPoolDfnPropsMap == null)
        {
            storPoolDfnPropsMap = Collections.emptyMap();
        }
        try
        {
            controller.storPoolDfnMapLock.writeLock().lock();
            apiCallRc = storPoolDfnApiCallHandler.createStorPoolDfn(
                accCtx,
                client,
                storPoolName,
                storPoolDfnPropsMap
            );
        }
        finally
        {
            controller.storPoolDfnMapLock.writeLock().unlock();
        }
        return apiCallRc;
    }

    /**
     * Marks a {@link StorPoolDefinition} for deletion.
     *
     * The {@link StorPoolDefinition} is only deleted once all corresponding satellites
     * confirmed that they have undeployed (deleted) the {@link StorPool}.
     *
     * @param accCtx
     * @param client
     * @param storPoolName required
     * @return
     */
    public ApiCallRc deleteStoragePoolDefinition(
        AccessContext accCtx,
        Peer client,
        String storPoolName
    )
    {
        ApiCallRc apiCallRc;
        try
        {
            controller.storPoolDfnMapLock.writeLock().lock();
            apiCallRc = storPoolDfnApiCallHandler.deleteStorPoolDfn(
                accCtx,
                client,
                storPoolName
            );
        }
        finally
        {
            controller.storPoolDfnMapLock.writeLock().unlock();
        }
        return apiCallRc;
    }

    /**
     * Creates a {@link StorPool}.
     *
     * @param accCtx
     * @param client
     * @param nodeName required
     * @param storPoolName required
     * @param driver required
     * @param storPoolPropsMap optional
     * @return
     */
    public ApiCallRc createStoragePool(
        AccessContext accCtx,
        Peer client,
        String nodeName,
        String storPoolName,
        String driver,
        Map<String, String> storPoolPropsMap
    )
    {
        ApiCallRc apiCallRc;
        if (storPoolPropsMap == null)
        {
            storPoolPropsMap = Collections.emptyMap();
        }
        try
        {
            controller.nodesMapLock.writeLock().lock();
            controller.storPoolDfnMapLock.writeLock().lock();

            apiCallRc = storPoolApiCallHandler.createStorPool(
                accCtx,
                client,
                nodeName,
                storPoolName,
                driver,
                storPoolPropsMap
            );
        }
        finally
        {
            controller.storPoolDfnMapLock.writeLock().unlock();
            controller.nodesMapLock.writeLock().unlock();
        }

        return apiCallRc;
    }

    /**
     * Marks the {@link StorPool} for deletion.
     *
     * The {@link StorPool} is only deleted once the corresponding satellite
     * confirms that it has undeployed (deleted) the {@link StorPool}.
     *
     * @param accCtx
     * @param client
     * @param nodeName
     * @param storPoolName
     * @return
     */
    public ApiCallRc deleteStoragePool(
        AccessContext accCtx,
        Peer client,
        String nodeName,
        String storPoolName
    )
    {
        ApiCallRc apiCallRc;
        try
        {
            controller.nodesMapLock.writeLock().lock();
            controller.storPoolDfnMapLock.writeLock().lock();

            apiCallRc = storPoolApiCallHandler.deleteStorPool(
                accCtx,
                client,
                nodeName,
                storPoolName
            );
        }
        finally
        {
            controller.storPoolDfnMapLock.writeLock().unlock();
            controller.nodesMapLock.writeLock().unlock();
        }

        return apiCallRc;
    }

    /**
     * Creates a new {@link NodeConnection}.
     *
     * @param accCtx
     * @param client
     * @param nodeName1 required
     * @param nodeName2 required
     * @param nodeConnPropsMap optional, recommended
     * @return
     */
    public ApiCallRc createNodeConnection(
        AccessContext accCtx,
        Peer client,
        String nodeName1,
        String nodeName2,
        Map<String, String> nodeConnPropsMap
    )
    {
        ApiCallRc apiCallRc;
        if (nodeConnPropsMap == null)
        {
            nodeConnPropsMap = Collections.emptyMap();
        }
        try
        {
            controller.nodesMapLock.writeLock().lock();
            apiCallRc = nodeConnApiCallHandler.createNodeConnection(
                accCtx,
                client,
                nodeName1,
                nodeName2,
                nodeConnPropsMap
            );
        }
        finally
        {
            controller.nodesMapLock.writeLock().unlock();
        }
        return apiCallRc;
    }

    /**
     * Deletes the {@link NodeConnection}.
     *
     * @param accCtx
     * @param client
     * @param nodeName required
     * @param storPoolName required
     * @return
     */
    public ApiCallRc deleteNodeConnection(
        AccessContext accCtx,
        Peer client,
        String nodeName1,
        String nodeName2
    )
    {
        ApiCallRc apiCallRc;
        try
        {
            controller.nodesMapLock.writeLock().lock();
            apiCallRc = nodeConnApiCallHandler.deleteNodeConnection(
                accCtx,
                client,
                nodeName1,
                nodeName2
            );
        }
        finally
        {
            controller.nodesMapLock.writeLock().unlock();
        }
        return apiCallRc;
    }

    /**
     * Creates a new {@link ResourceConnection}.
     *
     * @param accCtx
     * @param client
     * @param nodeName1 required
     * @param nodeName2 required
     * @param rscName required
     * @param rscConnPropsMap optional, recommended
     * @return
     */
    public ApiCallRc createResourceConnection(
        AccessContext accCtx,
        Peer client,
        String nodeName1,
        String nodeName2,
        String rscName,
        Map<String, String> rscConnPropsMap
    )
    {
        ApiCallRc apiCallRc;
        if (rscConnPropsMap == null)
        {
            rscConnPropsMap = Collections.emptyMap();
        }
        try
        {
            controller.nodesMapLock.writeLock().lock();
            controller.rscDfnMapLock.writeLock().lock();
            apiCallRc = rscConnApiCallHandler.createResourceConnection(
                accCtx,
                client,
                nodeName1,
                nodeName2,
                rscName,
                rscConnPropsMap
            );
		}
        finally
        {
            controller.rscDfnMapLock.writeLock().unlock();
            controller.nodesMapLock.writeLock().unlock();
        }
        return apiCallRc;
    }

    /**
     * Deletes a {@link ResourceConnection}.
     *
     * @param accCtx
     * @param client
     * @param nodeName1 required
     * @param nodeName2 required
     * @param rscName required
     * @return
     */
    public ApiCallRc deleteResourceConnection(
        AccessContext accCtx,
        Peer client,
        String nodeName1,
        String nodeName2,
        String rscName
    )
    {
        ApiCallRc apiCallRc;
        try
        {
            controller.nodesMapLock.writeLock().lock();
            controller.rscDfnMapLock.writeLock().lock();
            apiCallRc = rscConnApiCallHandler.deleteResourceConnection(
                accCtx,
                client,
                nodeName1,
                nodeName2,
                rscName
            );
        }
        finally
        {
            controller.rscDfnMapLock.writeLock().unlock();
            controller.nodesMapLock.writeLock().unlock();
        }
        return apiCallRc;
    }

    /**
     * Creates a new {@link VolumeConnection}.
     *
     * @param accCtx
     * @param client
     * @param nodeName1 required
     * @param nodeName2 required
     * @param rscName required
     * @param vlmNr required
     * @param vlmConnPropsMap optional, recommended
     * @return
     */
    public ApiCallRc createVolumeConnection(
        AccessContext accCtx,
        Peer client,
        String nodeName1,
        String nodeName2,
        String rscName,
        int vlmNr,
        Map<String, String> vlmConnPropsMap
    )
    {
        ApiCallRc apiCallRc;
        if (vlmConnPropsMap == null)
        {
            vlmConnPropsMap = Collections.emptyMap();
        }
        try
        {
            controller.nodesMapLock.writeLock().lock();
            controller.rscDfnMapLock.writeLock().lock();
            apiCallRc = vlmConnApiCallHandler.createVolumeConnection(
                accCtx,
                client,
                nodeName1,
                nodeName2,
                rscName,
                vlmNr,
                vlmConnPropsMap
            );
        }
        finally
        {
            controller.rscDfnMapLock.writeLock().unlock();
            controller.nodesMapLock.writeLock().unlock();
        }
        return apiCallRc;
    }

    /**
     * Deletes a {@link VolumeConnection}.
     *
     * @param accCtx
     * @param client
     * @param nodeName1 required
     * @param nodeName2 required
     * @param rscName required
     * @param vlmNr required
     * @return
     */
    public ApiCallRc deleteVolumeConnection(
        AccessContext accCtx,
        Peer client,
        String nodeName1,
        String nodeName2,
        String rscName,
        int vlmNr
    )
    {
        ApiCallRc apiCallRc;
        try
        {
            controller.nodesMapLock.writeLock().lock();
            controller.rscDfnMapLock.writeLock().lock();
            apiCallRc = vlmConnApiCallHandler.deleteVolumeConnection(
                accCtx,
                client,
                nodeName1,
                nodeName2,
                rscName,
                vlmNr
            );
        }
        finally
        {
            controller.rscDfnMapLock.writeLock().unlock();
            controller.nodesMapLock.writeLock().unlock();
        }
        return apiCallRc;
    }

    /**
     * This method should be called when the controller sent a message to a satellite
     * that its resources {@code rscName} has changed, and the satellite now queries those
     * changes.
     * Calling this method will collect the needed data and send it to the given
     * satellite.
     *
     * @param satellite required
     * @param msgId required
     * @param rscName required
     * @param rscUuid required (for double checking)
     */
    public void requestResource(Peer satellite, int msgId, String rscName, UUID rscUuid)
    {
        try
        {
            controller.nodesMapLock.readLock().lock();
            controller.rscDfnMapLock.readLock().lock();
            controller.storPoolDfnMapLock.readLock().lock();

            rscApiCallHandler.respondResource(rscName, rscUuid, msgId, satellite);
        }
        finally
        {
            controller.nodesMapLock.readLock().unlock();
            controller.rscDfnMapLock.readLock().unlock();
            controller.storPoolDfnMapLock.readLock().unlock();
        }
    }

    /**
     * This method should be called when the controller sent a message to a satellite
     * that its storPools {@code storPoolName} has changed, and the satellite now
     * queries those changes.
     * Calling this method will collect the needed data and send it to the given
     * satellite.
     * @param satellite required
     * @param msgId required
     * @param storPoolName required
     * @param storPoolUuid required (for double checking)
     */
    public void requestStorPool(
        Peer satellite,
        int msgId,
        String storPoolName,
        UUID storPoolUuid
    )
    {
        try
        {
            controller.nodesMapLock.readLock().lock();
            controller.storPoolDfnMapLock.readLock().lock();

            storPoolApiCallHandler.respondStorPool(storPoolName, storPoolUuid, msgId, satellite);
        }
        finally
        {
            controller.nodesMapLock.readLock().unlock();
            controller.storPoolDfnMapLock.readLock().unlock();
        }
    }
}
