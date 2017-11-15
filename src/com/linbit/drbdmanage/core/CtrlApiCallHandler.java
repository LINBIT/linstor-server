package com.linbit.drbdmanage.core;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.drbdmanage.Resource;
import com.linbit.drbdmanage.StorPool;
import com.linbit.drbdmanage.Volume;
import com.linbit.drbdmanage.VolumeDefinition;
import com.linbit.drbdmanage.api.ApiCallRc;
import com.linbit.drbdmanage.api.ApiType;
import com.linbit.drbdmanage.api.interfaces.Serializer;
import com.linbit.drbdmanage.api.protobuf.controller.serializer.ResourceDataSerializerProto;
import com.linbit.drbdmanage.api.protobuf.controller.serializer.StorPoolDataSerializerProto;
import com.linbit.drbdmanage.logging.ErrorReporter;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.security.AccessContext;

public class CtrlApiCallHandler
{
    private final CtrlNodeApiCallHandler nodeApiCallHandler;
    private final CtrlRscDfnApiCallHandler rscDfnApiCallHandler;
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
        Serializer<Resource> rscSerializer;
        Serializer<StorPool> storPoolSerializer;
        switch (type)
        {
            case PROTOBUF:
                rscSerializer = new ResourceDataSerializerProto(apiCtx, errorReporter);
                storPoolSerializer = new StorPoolDataSerializerProto(apiCtx, errorReporter);
                break;
            default:
                throw new ImplementationError("Unknown ApiType: " + type, null);
        }
        nodeApiCallHandler = new CtrlNodeApiCallHandler(controllerRef, apiCtx);
        rscApiCallHandler = new CtrlRscApiCallHandler(controllerRef, rscSerializer, apiCtx);
        rscDfnApiCallHandler = new CtrlRscDfnApiCallHandler(controllerRef, apiCtx);
        storPoolDfnApiCallHandler = new CtrlStorPoolDfnApiCallHandler(controllerRef);
        storPoolApiCallHandler = new CtrlStorPoolApiCallHandler(controllerRef, storPoolSerializer, apiCtx);
        nodeConnApiCallHandler = new CtrlNodeConnectionApiCallHandler(controllerRef);
        rscConnApiCallHandler = new CtrlRscConnectionApiCallHandler(controllerRef);
        vlmConnApiCallHandler = new CtrlVlmConnectionApiCallHandler(controllerRef);
    }

    public ApiCallRc createNode(
        AccessContext accCtx,
        Peer client,
        String nodeNameStr,
        String nodeTypeStr,
        Map<String, String> props
    )
    {
        ApiCallRc apiCallRc;
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

    public ApiCallRc createResourceDefinition(
        AccessContext accCtx,
        Peer client,
        String resourceName,
        int port,
        String secret,
        Map<String, String> props,
        List<VolumeDefinition.VlmDfnApi> volDescrMap
    )
    {
        ApiCallRc apiCallRc;
        try
        {
            controller.rscDfnMapLock.writeLock().lock();
            if (secret == null || secret.trim().equals(""))
            {
                secret = controller.generateSharedSecret();
            }
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

    public ApiCallRc createStoragePoolDefinition(
        AccessContext accCtx,
        Peer client,
        String storPoolName,
        Map<String, String> storPoolDfnPropsMap
    )
    {
        ApiCallRc apiCallRc;
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

    public ApiCallRc createNodeConnection(
        AccessContext accCtx,
        Peer client,
        String nodeName1,
        String nodeName2,
        Map<String, String> nodeConnPropsMap
    )
    {
        ApiCallRc apiCallRc;
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

    public void requestResource(String rscName, UUID rscUuid, int msgId, Peer satellite)
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

    public void requestStorPool(String storPoolName, UUID storPoolUuid, int msgId, Peer satellite)
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
