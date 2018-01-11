package com.linbit.linstor.core;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.linstor.NetInterface.NetInterfaceApi;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeConnection;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceConnection;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.SatelliteConnection.SatelliteConnectionApi;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeConnection;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeDefinition.VlmDfnApi;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiType;
import com.linbit.linstor.api.interfaces.serializer.CtrlAuthSerializer;
import com.linbit.linstor.api.interfaces.serializer.CtrlFullSyncSerializer;
import com.linbit.linstor.api.interfaces.serializer.CtrlNodeSerializer;
import com.linbit.linstor.api.interfaces.serializer.CtrlSerializer;
import com.linbit.linstor.api.protobuf.controller.serializer.AuthSerializerProto;
import com.linbit.linstor.api.protobuf.controller.serializer.FullSyncSerializerProto;
import com.linbit.linstor.api.protobuf.controller.serializer.NodeDataSerializerProto;
import com.linbit.linstor.api.protobuf.controller.serializer.ResourceDataSerializerProto;
import com.linbit.linstor.api.protobuf.controller.serializer.StorPoolDataSerializerProto;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.api.interfaces.serializer.InterComSerializer;
import com.linbit.linstor.api.protobuf.ProtoInterComSerializer;

public class CtrlApiCallHandler
{
    private final CtrlAuthenticationApiCallHandler authApiCallHandler;
    private final CtrlFullSyncApiCallHandler fullSyncApiCallHandler;
    private final CtrlNodeApiCallHandler nodeApiCallHandler;
    private final CtrlRscDfnApiCallHandler rscDfnApiCallHandler;
    private final CtrlVlmDfnApiCallHandler vlmDfnApiCallHandler;
    private final CtrlRscApiCallHandler rscApiCallHandler;
    private final CtrlVlmApiCallHandler vlmApiCallHandler;
    private final CtrlStorPoolDfnApiCallHandler storPoolDfnApiCallHandler;
    private final CtrlStorPoolApiCallHandler storPoolApiCallHandler;
    private final CtrlNodeConnectionApiCallHandler nodeConnApiCallHandler;
    private final CtrlRscConnectionApiCallHandler rscConnApiCallHandler;
    private final CtrlVlmConnectionApiCallHandler vlmConnApiCallHandler;

    private ApiCtrlAccessorImpl apiCtrlAccessors;

    CtrlApiCallHandler(ApiCtrlAccessorImpl apiCtrlAccessorsRef, ApiType type, AccessContext apiCtx)
    {

        apiCtrlAccessors = apiCtrlAccessorsRef;
        final ErrorReporter errorReporter = apiCtrlAccessors.getErrorReporter();
        final CtrlAuthSerializer authSerializer;
        final CtrlFullSyncSerializer fullSyncSerializer;
        final CtrlNodeSerializer nodeSerializer;
        final CtrlSerializer<Resource> rscSerializer;
        final CtrlSerializer<StorPool> storPoolSerializer;
        final InterComSerializer interComSrzl;

        switch (type)
        {
            case PROTOBUF:
                authSerializer = new AuthSerializerProto();
                nodeSerializer = new NodeDataSerializerProto(apiCtx, errorReporter);
                rscSerializer = new ResourceDataSerializerProto(apiCtx, errorReporter);
                storPoolSerializer = new StorPoolDataSerializerProto(apiCtx, errorReporter);
                fullSyncSerializer = new FullSyncSerializerProto(
                    errorReporter,
                    (NodeDataSerializerProto) nodeSerializer,
                    (ResourceDataSerializerProto) rscSerializer,
                    (StorPoolDataSerializerProto) storPoolSerializer
                );
                interComSrzl = new ProtoInterComSerializer(errorReporter);
                break;
            default:
                throw new ImplementationError("Unknown ApiType: " + type, null);
        }
        authApiCallHandler = new CtrlAuthenticationApiCallHandler(apiCtrlAccessors, authSerializer);
        fullSyncApiCallHandler = new CtrlFullSyncApiCallHandler(apiCtrlAccessors, apiCtx, fullSyncSerializer);
        nodeApiCallHandler = new CtrlNodeApiCallHandler(apiCtrlAccessors, apiCtx, nodeSerializer, interComSrzl);
        rscDfnApiCallHandler = new CtrlRscDfnApiCallHandler(
            apiCtrlAccessors,
            rscSerializer,
            interComSrzl,
            apiCtx
        );
        vlmDfnApiCallHandler = new CtrlVlmDfnApiCallHandler(apiCtrlAccessors, rscSerializer, apiCtx);
        rscApiCallHandler = new CtrlRscApiCallHandler(apiCtrlAccessors, rscSerializer, interComSrzl, apiCtx);
        vlmApiCallHandler = new CtrlVlmApiCallHandler(apiCtrlAccessors, apiCtx);
        storPoolDfnApiCallHandler = new CtrlStorPoolDfnApiCallHandler(apiCtrlAccessors, interComSrzl);
        storPoolApiCallHandler = new CtrlStorPoolApiCallHandler(apiCtrlAccessors, storPoolSerializer, interComSrzl, apiCtx);
        nodeConnApiCallHandler = new CtrlNodeConnectionApiCallHandler(apiCtrlAccessors, nodeSerializer, apiCtx);
        rscConnApiCallHandler = new CtrlRscConnectionApiCallHandler(apiCtrlAccessors, rscSerializer, apiCtx);
        vlmConnApiCallHandler = new CtrlVlmConnectionApiCallHandler(apiCtrlAccessors, rscSerializer, apiCtx);
    }

    public void completeSatelliteAuthentication(Peer peer)
    {
        // no locks needed
        authApiCallHandler.completeAuthentication(peer);
    }

    public void sendFullSync(Peer client)
    {
        try
        {
            apiCtrlAccessors.getNodesMapLock().readLock().lock();
            apiCtrlAccessors.getRscDfnMapLock().readLock().lock();
            apiCtrlAccessors.getStorPoolDfnMapLock().readLock().lock();

            fullSyncApiCallHandler.sendFullSync(client);
        }
        finally
        {
            apiCtrlAccessors.getNodesMapLock().readLock().unlock();
            apiCtrlAccessors.getRscDfnMapLock().readLock().unlock();
            apiCtrlAccessors.getStorPoolDfnMapLock().readLock().unlock();
        }
    }

    /**
     * Creates a new {@link Node}
     *
     * @param accCtx
     * @param client
     * @param nodeNameStr required
     * @param nodeTypeStr required
     * @param netIfs required, at least one needed
     * @param satelliteConnectionApis required, currently all but first ignored. At least one required
     * @param props optional
     * @return
     */
    public ApiCallRc createNode(
        AccessContext accCtx,
        Peer client,
        String nodeNameStr,
        String nodeTypeStr,
        List<NetInterfaceApi> netIfs,
        List<SatelliteConnectionApi> satelliteConnectionApis,
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
            apiCtrlAccessors.getNodesMapLock().writeLock().lock();
            apiCallRc = nodeApiCallHandler.createNode(
                accCtx,
                client,
                nodeNameStr,
                nodeTypeStr,
                netIfs,
                satelliteConnectionApis,
                props
            );
        }
        finally
        {
            apiCtrlAccessors.getNodesMapLock().writeLock().unlock();
        }
        return apiCallRc;
    }

    /**
     * Modifies a given node.
     *
     * @param accCtx
     * @param client
     * @param nodeUuid optional - if given, modification is only performed if it matches the found
     *   node's UUID.
     * @param nodeName required
     * @param nodeType optional - if given, attempts to modify the type of the node
     * @param overrideProps required (can be empty) - overrides the given property key-value pairs
     * @param deletePropKeys required (can be empty) - deletes the given property keys
     * @return
     */
    public ApiCallRc modifyNode(
        AccessContext accCtx,
        Peer client,
        UUID nodeUuid,
        String nodeName,
        String nodeType,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys
    )
    {
        ApiCallRc apiCallRc;
        try
        {
            apiCtrlAccessors.getNodesMapLock().writeLock().lock();
            apiCallRc = nodeApiCallHandler.modifyNode(
                accCtx,
                client,
                nodeUuid,
                nodeName,
                nodeType,
                overrideProps,
                deletePropKeys
            );
        }
        finally
        {
            apiCtrlAccessors.getNodesMapLock().writeLock().unlock();
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
            apiCtrlAccessors.getNodesMapLock().writeLock().lock();
            apiCallRc = nodeApiCallHandler.deleteNode(accCtx, client, nodeName);
        }
        finally
        {
            apiCtrlAccessors.getNodesMapLock().writeLock().unlock();

        }
        return apiCallRc;
    }

    public byte[] listNode(int msgId, AccessContext accCtx, Peer client)
    {
        try
        {
            apiCtrlAccessors.getNodesMapLock().readLock().lock();
            return nodeApiCallHandler.listNodes(msgId, accCtx, client);
        }
        finally
        {
            apiCtrlAccessors.getNodesMapLock().readLock().unlock();
        }
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
        String transportType,
        Map<String, String> props,
        List<VolumeDefinition.VlmDfnApi> volDescrMap
    )
    {
        ApiCallRc apiCallRc;
        if (port == null)
        {
            port = 8042; // FIXME find free port with poolAllocator
        }
        if (secret == null || secret.trim().isEmpty())
        {
            secret = apiCtrlAccessors.generateSharedSecret();
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
            apiCtrlAccessors.getRscDfnMapLock().writeLock().lock();
            apiCallRc = rscDfnApiCallHandler.createResourceDefinition(
                accCtx,
                client,
                resourceName,
                port,
                secret,
                transportType,
                props,
                volDescrMap
            );
        }
        finally
        {
            apiCtrlAccessors.getRscDfnMapLock().writeLock().unlock();
        }
        return apiCallRc;
    }

    /**
     * Modifies a given resource definition.
     *
     * @param accCtx
     * @param client
     * @param rscDfnUuid optional - if given, modification is only performed if it matches the found
     *   rscDfn's UUID.
     * @param rscName required
     * @param port optional - if given, attempts to override the old port
     * @param secret optional - if given, attempts to override the old secret
     * @param overrideProps required (can be empty) - overrides the given property key-value pairs
     * @param deletePropKeys required (can be empty) - deletes the given property keys
     * @return
     */
    public ApiCallRc modifyRscDfn(
        AccessContext accCtx,
        Peer client,
        UUID rscDfnUuid,
        String rscName,
        Integer port,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys
    )
    {
        ApiCallRc apiCallRc;
        try
        {
            apiCtrlAccessors.getRscDfnMapLock().writeLock().lock();
            apiCallRc = rscDfnApiCallHandler.modifyRscDfn(
                accCtx,
                client,
                rscDfnUuid,
                rscName,
                port,
                overrideProps,
                deletePropKeys
            );
        }
        finally
        {
            apiCtrlAccessors.getRscDfnMapLock().writeLock().unlock();
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
            apiCtrlAccessors.getRscDfnMapLock().writeLock().lock();
            apiCallRc = rscDfnApiCallHandler.deleteResourceDefinition(
                accCtx,
                client,
                resourceName
            );
        }
        finally
        {
            apiCtrlAccessors.getRscDfnMapLock().writeLock().unlock();
        }
        return apiCallRc;
    }

    public byte[] listResourceDefinition(int msgId, AccessContext accCtx, Peer client)
    {
        try
        {
            apiCtrlAccessors.getRscDfnMapLock().readLock().lock();
            return rscDfnApiCallHandler.listResourceDefinitions(msgId, accCtx, client);
        }
        finally
        {
            apiCtrlAccessors.getRscDfnMapLock().readLock().unlock();
        }
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
            apiCtrlAccessors.getRscDfnMapLock().writeLock().lock();
            apiCallRc = vlmDfnApiCallHandler.createVolumeDefinitions(
                accCtx,
                client,
                rscName,
                vlmDfnApiList
            );
        }
        finally
        {
            apiCtrlAccessors.getRscDfnMapLock().writeLock().unlock();
        }
        return apiCallRc;
    }

    /**
     * Modifies an existing {@link VolumeDefinition}
     *
     * @param accCtx
     * @param client
     * @param vlmDfnUuid optional, if given checked against persisted UUID
     * @param rscName required
     * @param vlmNr required
     * @param size optional
     * @param minorNr optional
     * @param overrideProps optional
     * @param deletePropKeys optional
     * @return
     */
    public ApiCallRc modifyVlmDfn(
        AccessContext accCtx,
        Peer client,
        UUID vlmDfnUuid,
        String rscName,
        int vlmNr,
        Long size,
        Integer minorNr,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys
    )
    {
        ApiCallRc apiCallRc;

        if (overrideProps == null)
        {
            overrideProps = Collections.emptyMap();
        }
        if (deletePropKeys == null)
        {
            deletePropKeys = Collections.emptySet();
        }
        try
        {
            apiCtrlAccessors.getRscDfnMapLock().writeLock().lock();
            apiCallRc = vlmDfnApiCallHandler.modifyVlmDfn(
                accCtx,
                client,
                vlmDfnUuid,
                rscName,
                vlmNr,
                size,
                minorNr,
                overrideProps,
                deletePropKeys
            );
        }
        finally
        {
            apiCtrlAccessors.getRscDfnMapLock().writeLock().unlock();
        }
        return apiCallRc;
    }

    /**
     * Deletes a {@link VolumeDefinition} for a given {@link ResourceDefinition} and volume nr.
     *
     * @param accCtx
     * @param client
     * @param rscName required
     * @param volumeNr required
     * @return ApiCallResponse with status of the operation
     */
    public ApiCallRc deleteVolumeDefinition(
        AccessContext accCtx,
        Peer client,
        String rscName,
        int volumeNr
    )
    {
        ApiCallRc apiCallRc;
        try
        {
            apiCtrlAccessors.getRscDfnMapLock().writeLock().lock();
            apiCallRc = vlmDfnApiCallHandler.deleteVolumeDefinition(
                accCtx,
                client,
                rscName,
                volumeNr
            );
        }
        finally
        {
            apiCtrlAccessors.getRscDfnMapLock().writeLock().unlock();
        }
        return apiCallRc;
    }

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
            apiCtrlAccessors.getNodesMapLock().writeLock().lock();
            apiCtrlAccessors.getRscDfnMapLock().writeLock().lock();

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
            apiCtrlAccessors.getRscDfnMapLock().writeLock().unlock();
            apiCtrlAccessors.getNodesMapLock().writeLock().unlock();
        }

        return apiCallRc;
    }

    /**
     * Modifies an existing {@link Resource}
     *
     * @param accCtx
     * @param client
     * @param rscUuid optional, if given checked against persisted UUID
     * @param nodeName required
     * @param rscName required
     * @param overrideProps optional
     * @param deletePropKeys optional
     * @return
     */
    public ApiCallRc modifyRsc(
        AccessContext accCtx,
        Peer client,
        UUID rscUuid,
        String nodeName,
        String rscName,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys
    )
    {
        ApiCallRc apiCallRc;

        if (overrideProps == null)
        {
            overrideProps = Collections.emptyMap();
        }
        if (deletePropKeys == null)
        {
            deletePropKeys = Collections.emptySet();
        }
        try
        {
            apiCtrlAccessors.getNodesMapLock().writeLock().lock();
            apiCtrlAccessors.getRscDfnMapLock().writeLock().lock();
            apiCallRc = rscApiCallHandler.modifyResource(
                accCtx,
                client,
                rscUuid,
                nodeName,
                rscName,
                overrideProps,
                deletePropKeys
            );
        }
        finally
        {
            apiCtrlAccessors.getNodesMapLock().writeLock().unlock();
            apiCtrlAccessors.getRscDfnMapLock().writeLock().unlock();
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
            apiCtrlAccessors.getNodesMapLock().writeLock().lock();
            apiCtrlAccessors.getRscDfnMapLock().writeLock().lock();

            apiCallRc = rscApiCallHandler.deleteResource(
                accCtx,
                client,
                nodeName,
                rscName
            );
        }
        finally
        {
            apiCtrlAccessors.getRscDfnMapLock().writeLock().unlock();
            apiCtrlAccessors.getNodesMapLock().writeLock().unlock();
        }

        return apiCallRc;
    }

    public byte[] listResource(int msgId, AccessContext accCtx, Peer client)
    {
        try
        {
            apiCtrlAccessors.getRscDfnMapLock().readLock().lock();
            apiCtrlAccessors.getNodesMapLock().readLock().lock();
            return rscApiCallHandler.listResources(msgId, accCtx, client);
        }
        finally
        {
            apiCtrlAccessors.getRscDfnMapLock().readLock().unlock();
            apiCtrlAccessors.getNodesMapLock().readLock().unlock();
        }
    }

    /**
     * Called if a satellite deleted the resource.
     *
     * Resource will be deleted (NOT marked) and if all resources
     * of the resource definition are deleted, cleanup will be called.
     *
     * @param accCtx
     * @param client
     * @param nodeName required
     * @param rscName required
     * @return
     */
    public ApiCallRc resourceDeleted(
        AccessContext accCtx,
        Peer client,
        String nodeName,
        String rscName
    )
    {
        ApiCallRc apiCallRc;
        try
        {
            apiCtrlAccessors.getNodesMapLock().writeLock().lock();
            apiCtrlAccessors.getRscDfnMapLock().writeLock().lock();

            apiCallRc = rscApiCallHandler.resourceDeleted(
                accCtx,
                client,
                nodeName,
                rscName
            );
        }
        finally
        {
            apiCtrlAccessors.getRscDfnMapLock().writeLock().unlock();
            apiCtrlAccessors.getNodesMapLock().writeLock().unlock();
        }

        return apiCallRc;
    }

    /**
     * Called if a satellite deleted the volume.
     *
     * Volume will be deleted (NOT marked).
     *
     * @param accCtx
     * @param client
     * @param nodeName required
     * @param rscName required
     * @return
     */
    public ApiCallRc volumeDeleted(
        AccessContext accCtx,
        Peer client,
        String nodeName,
        String rscName,
        int volumeNr
    )
    {
        ApiCallRc apiCallRc;
        try
        {
            apiCtrlAccessors.getNodesMapLock().writeLock().lock();
            apiCtrlAccessors.getRscDfnMapLock().writeLock().lock();

            apiCallRc = vlmApiCallHandler.volumeDeleted(
                accCtx,
                client,
                nodeName,
                rscName,
                volumeNr
            );
        }
        finally
        {
            apiCtrlAccessors.getRscDfnMapLock().writeLock().unlock();
            apiCtrlAccessors.getNodesMapLock().writeLock().unlock();
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
            apiCtrlAccessors.getStorPoolDfnMapLock().writeLock().lock();
            apiCallRc = storPoolDfnApiCallHandler.createStorPoolDfn(
                accCtx,
                client,
                storPoolName,
                storPoolDfnPropsMap
            );
        }
        finally
        {
            apiCtrlAccessors.getStorPoolDfnMapLock().writeLock().unlock();
        }
        return apiCallRc;
    }

    /**
     * Modifies an existing {@link StorPoolDefinition}
     *
     * @param accCtx
     * @param client
     * @param storPoolDfnUuid optional, if given checked against persisted UUID
     * @param storPoolName required
     * @param overrideProps optional
     * @param deletePropKeys optional
     * @return
     */
    public ApiCallRc modifyStorPoolDfn(
        AccessContext accCtx,
        Peer client,
        UUID storPoolDfnUuid,
        String storPoolName,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys
    )
    {
        ApiCallRc apiCallRc;

        if (overrideProps == null)
        {
            overrideProps = Collections.emptyMap();
        }
        if (deletePropKeys == null)
        {
            deletePropKeys = Collections.emptySet();
        }
        try
        {
            apiCtrlAccessors.getStorPoolDfnMapLock().writeLock().lock();
            apiCallRc = storPoolDfnApiCallHandler.modifyStorPoolDfn(
                accCtx,
                client,
                storPoolDfnUuid,
                storPoolName,
                overrideProps,
                deletePropKeys
            );
        }
        finally
        {
            apiCtrlAccessors.getStorPoolDfnMapLock().writeLock().unlock();
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
            apiCtrlAccessors.getStorPoolDfnMapLock().writeLock().lock();
            apiCallRc = storPoolDfnApiCallHandler.deleteStorPoolDfn(
                accCtx,
                client,
                storPoolName
            );
        }
        finally
        {
            apiCtrlAccessors.getStorPoolDfnMapLock().writeLock().unlock();
        }
        return apiCallRc;
    }

    public byte[] listStorPoolDefinition(int msgId, AccessContext accCtx, Peer client)
    {
        try
        {
            apiCtrlAccessors.getStorPoolDfnMapLock().readLock().lock();
            return storPoolDfnApiCallHandler.listStorPoolDefinitions(msgId, accCtx, client);
        }
        finally
        {
            apiCtrlAccessors.getStorPoolDfnMapLock().readLock().unlock();
        }
    }

    public byte[] listStorPool(int msgId, AccessContext accCtx, Peer client)
    {
        try
        {
            apiCtrlAccessors.getStorPoolDfnMapLock().readLock().lock();
            return storPoolApiCallHandler.listStorPools(msgId, accCtx, client);
        }
        finally
        {
            apiCtrlAccessors.getStorPoolDfnMapLock().readLock().unlock();
        }
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
            apiCtrlAccessors.getNodesMapLock().writeLock().lock();
            apiCtrlAccessors.getStorPoolDfnMapLock().writeLock().lock();

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
            apiCtrlAccessors.getStorPoolDfnMapLock().writeLock().unlock();
            apiCtrlAccessors.getNodesMapLock().writeLock().unlock();
        }

        return apiCallRc;
    }

    /**
     * Modifies an existing {@link StorPool}
     *
     * @param accCtx
     * @param client
     * @param storPoolUuid optional, if given checked against persisted UUID
     * @param nodeName required
     * @param storPoolName required
     * @param overrideProps optional
     * @param deletePropKeys optional
     * @return
     */
    public ApiCallRc modifyStorPool(
        AccessContext accCtx, Peer client, UUID storPoolUuid, String nodeName, String storPoolName,
        Map<String, String> overrideProps, Set<String> deletePropKeys
    )
    {
        ApiCallRc apiCallRc;

        if (overrideProps == null)
        {
            overrideProps = Collections.emptyMap();
        }
        if (deletePropKeys == null)
        {
            deletePropKeys = Collections.emptySet();
        }
        try
        {
            apiCtrlAccessors.getNodesMapLock().writeLock().lock();
            apiCtrlAccessors.getStorPoolDfnMapLock().writeLock().lock();
            apiCallRc = storPoolApiCallHandler.modifyStorPool(
                accCtx,
                client,
                storPoolUuid,
                nodeName,
                storPoolName,
                overrideProps,
                deletePropKeys
            );
        }
        finally
        {
            apiCtrlAccessors.getNodesMapLock().writeLock().unlock();
            apiCtrlAccessors.getStorPoolDfnMapLock().writeLock().unlock();
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
            apiCtrlAccessors.getNodesMapLock().writeLock().lock();
            apiCtrlAccessors.getStorPoolDfnMapLock().writeLock().lock();

            apiCallRc = storPoolApiCallHandler.deleteStorPool(
                accCtx,
                client,
                nodeName,
                storPoolName
            );
        }
        finally
        {
            apiCtrlAccessors.getStorPoolDfnMapLock().writeLock().unlock();
            apiCtrlAccessors.getNodesMapLock().writeLock().unlock();
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
            apiCtrlAccessors.getNodesMapLock().writeLock().lock();
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
            apiCtrlAccessors.getNodesMapLock().writeLock().unlock();
        }
        return apiCallRc;
    }

    /**
     * Modifies an existing {@link NodeConnection}
     *
     * @param accCtx
     * @param client
     * @param nodeConnUuid optional, if given checks against persisted uuid
     * @param nodeName1 required
     * @param nodeName2 required
     * @param overrideProps optional, can be empty
     * @param deletePropKeys optional, can be empty
     * @return
     */
    public ApiCallRc modifyNodeConn(
        AccessContext accCtx,
        Peer client,
        UUID nodeConnUuid,
        String nodeName1,
        String nodeName2,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys
    )
    {
        if (overrideProps == null)
        {
            overrideProps = Collections.emptyMap();
        }
        if (deletePropKeys == null)
        {
            deletePropKeys = Collections.emptySet();
        }
        ApiCallRc apiCallRc;
        try
        {
            apiCtrlAccessors.getNodesMapLock().writeLock().lock();
            apiCallRc = nodeConnApiCallHandler.modifyNodeConnection(
                accCtx,
                client,
                nodeConnUuid,
                nodeName1,
                nodeName2,
                overrideProps,
                deletePropKeys
            );
        }
        finally
        {
            apiCtrlAccessors.getNodesMapLock().writeLock().unlock();
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
            apiCtrlAccessors.getNodesMapLock().writeLock().lock();
            apiCallRc = nodeConnApiCallHandler.deleteNodeConnection(
                accCtx,
                client,
                nodeName1,
                nodeName2
            );
        }
        finally
        {
            apiCtrlAccessors.getNodesMapLock().writeLock().unlock();
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
            apiCtrlAccessors.getNodesMapLock().writeLock().lock();
            apiCtrlAccessors.getRscDfnMapLock().writeLock().lock();
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
            apiCtrlAccessors.getRscDfnMapLock().writeLock().unlock();
            apiCtrlAccessors.getNodesMapLock().writeLock().unlock();
        }
        return apiCallRc;
    }

    /**
     * Modifies an existing {@link ResourceConnection}
     * @param accCtx
     * @param client
     * @param rscConnUuid optional, if given checked against persisted UUID
     * @param nodeName1 required
     * @param nodeName2 required
     * @param rscName required
     * @param overrideProps optional
     * @param deletePropKeys optional
     * @return
     */
    public ApiCallRc modifyRscConn(
        AccessContext accCtx,
        Peer client,
        UUID rscConnUuid,
        String nodeName1,
        String nodeName2,
        String rscName,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys
    )
    {
        ApiCallRc apiCallRc;

        if (overrideProps == null)
        {
            overrideProps = Collections.emptyMap();
        }
        if (deletePropKeys == null)
        {
            deletePropKeys = Collections.emptySet();
        }
        try
        {
            apiCtrlAccessors.getNodesMapLock().writeLock().lock();
            apiCtrlAccessors.getRscDfnMapLock().writeLock().lock();
            apiCallRc = rscConnApiCallHandler.modifyRscConnection(
                accCtx,
                client,
                rscConnUuid,
                nodeName1,
                nodeName2,
                rscName,
                overrideProps,
                deletePropKeys
            );
        }
        finally
        {
            apiCtrlAccessors.getNodesMapLock().writeLock().unlock();
            apiCtrlAccessors.getRscDfnMapLock().writeLock().unlock();
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
            apiCtrlAccessors.getNodesMapLock().writeLock().lock();
            apiCtrlAccessors.getRscDfnMapLock().writeLock().lock();
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
            apiCtrlAccessors.getRscDfnMapLock().writeLock().unlock();
            apiCtrlAccessors.getNodesMapLock().writeLock().unlock();
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
            apiCtrlAccessors.getNodesMapLock().writeLock().lock();
            apiCtrlAccessors.getRscDfnMapLock().writeLock().lock();
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
            apiCtrlAccessors.getRscDfnMapLock().writeLock().unlock();
            apiCtrlAccessors.getNodesMapLock().writeLock().unlock();
        }
        return apiCallRc;
    }

    /**
     * Modifies an existing {@link VolumeConnection}
     * @param accCtx
     * @param client
     * @param vlmConnUuid optional, if given checked against persisted UUID
     * @param nodeName1 required
     * @param nodeName2 required
     * @param rscName required
     * @param vlmNr required
     * @param overrideProps optional
     * @param deletePropKeys optional
     * @return
     */
    public ApiCallRc modifyVlmConn(
        AccessContext accCtx,
        Peer client,
        UUID vlmConnUuid,
        String nodeName1,
        String nodeName2,
        String rscName,
        int vlmNr,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys
    )
    {
        ApiCallRc apiCallRc;

        if (overrideProps == null)
        {
            overrideProps = Collections.emptyMap();
        }
        if (deletePropKeys == null)
        {
            deletePropKeys = Collections.emptySet();
        }
        try
        {
            apiCtrlAccessors.getNodesMapLock().writeLock().lock();
            apiCtrlAccessors.getRscDfnMapLock().writeLock().lock();
            apiCallRc = vlmConnApiCallHandler.modifyVolumeConnection(
                accCtx,
                client,
                vlmConnUuid,
                nodeName1,
                nodeName2,
                rscName,
                vlmNr,
                overrideProps,
                deletePropKeys
            );
        }
        finally
        {
            apiCtrlAccessors.getNodesMapLock().writeLock().unlock();
            apiCtrlAccessors.getRscDfnMapLock().writeLock().unlock();
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
            apiCtrlAccessors.getNodesMapLock().writeLock().lock();
            apiCtrlAccessors.getRscDfnMapLock().writeLock().lock();
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
            apiCtrlAccessors.getRscDfnMapLock().writeLock().unlock();
            apiCtrlAccessors.getNodesMapLock().writeLock().unlock();
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
     * @param rscUuid required (for double checking)
     * @param rscName required
     */
    public void handleResourceRequest(
        Peer satellite,
        int msgId,
        String nodeName,
        UUID rscUuid,
        String rscName
    )
    {
        try
        {
            apiCtrlAccessors.getNodesMapLock().readLock().lock();
            apiCtrlAccessors.getRscDfnMapLock().readLock().lock();
            apiCtrlAccessors.getStorPoolDfnMapLock().readLock().lock();

            rscApiCallHandler.respondResource(msgId, satellite, nodeName, rscUuid, rscName);
        }
        finally
        {
            apiCtrlAccessors.getNodesMapLock().readLock().unlock();
            apiCtrlAccessors.getRscDfnMapLock().readLock().unlock();
            apiCtrlAccessors.getStorPoolDfnMapLock().readLock().unlock();
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
     * @param storPoolUuid required (for double checking)
     * @param storPoolNameStr required
     */
    public void handleStorPoolRequest(
        Peer satellite,
        int msgId,
        UUID storPoolUuid,
        String storPoolNameStr
    )
    {
        try
        {
            apiCtrlAccessors.getNodesMapLock().readLock().lock();
            apiCtrlAccessors.getStorPoolDfnMapLock().readLock().lock();

            storPoolApiCallHandler.respondStorPool(
                msgId,
                satellite,
                storPoolUuid,
                storPoolNameStr
            );
        }
        finally
        {
            apiCtrlAccessors.getNodesMapLock().readLock().unlock();
            apiCtrlAccessors.getStorPoolDfnMapLock().readLock().unlock();
        }
    }

    public void handleNodeRequest(
        Peer satellite,
        int msgId,
        UUID nodeUuid,
        String nodeNameStr
    )
    {
        try
        {
            apiCtrlAccessors.getNodesMapLock().readLock().lock();

            nodeApiCallHandler.respondNode(
                msgId,
                satellite,
                nodeUuid,
                nodeNameStr
            );
        }
        finally
        {
            apiCtrlAccessors.getNodesMapLock().readLock().unlock();
        }
    }

    public void handlePrimaryResourceRequest(
        AccessContext accCtx,
        Peer satellite,
        int msgId,
        String rscName,
        UUID rscUuid
    )
    {
        try
        {
            apiCtrlAccessors.getRscDfnMapLock().writeLock().lock();
            rscDfnApiCallHandler.handlePrimaryResourceRequest(accCtx, satellite, msgId, rscName, rscUuid);
        }
        finally
        {
            apiCtrlAccessors.getRscDfnMapLock().writeLock().unlock();
        }
    }
}
