package com.linbit.linstor.core;

import com.linbit.linstor.Volume;
import com.linbit.linstor.NetInterface.NetInterfaceApi;
import com.linbit.linstor.SatelliteConnection.SatelliteConnectionApi;
import com.linbit.linstor.VolumeDefinition.VlmDfnApi;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.Lock;

@Singleton
public class CtrlApiCallHandler
{
    private final CtrlConfApiCallHandler ctrlConfApiCallHandler;
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
    private final CtrlNetIfApiCallHandler netIfApiCallHandler;
    private final CtrlClientSerializer ctrlClientcomSrzl;

    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock rscDfnMapLock;
    private final ReadWriteLock storPoolDfnMapLock;
    private final ReadWriteLock ctrlConfigLock;

    @Inject
    CtrlApiCallHandler(
        CtrlConfApiCallHandler ctrlConfApiCallHandlerRef,
        CtrlAuthenticationApiCallHandler authApiCallHandlerRef,
        CtrlFullSyncApiCallHandler fullSyncApiCallHandlerRef,
        CtrlNodeApiCallHandler nodeApiCallHandlerRef,
        CtrlRscDfnApiCallHandler rscDfnApiCallHandlerRef,
        CtrlVlmDfnApiCallHandler vlmDfnApiCallHandlerRef,
        CtrlRscApiCallHandler rscApiCallHandlerRef,
        CtrlVlmApiCallHandler vlmApiCallHandlerRef,
        CtrlStorPoolDfnApiCallHandler storPoolDfnApiCallHandlerRef,
        CtrlStorPoolApiCallHandler storPoolApiCallHandlerRef,
        CtrlNodeConnectionApiCallHandler nodeConnApiCallHandlerRef,
        CtrlRscConnectionApiCallHandler rscConnApiCallHandlerRef,
        CtrlVlmConnectionApiCallHandler vlmConnApiCallHandlerRef,
        CtrlNetIfApiCallHandler netIfApiCallHandlerRef,
        CtrlClientSerializer ctrlClientcomSrzlRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef,
        @Named(CoreModule.CTRL_CONF_LOCK) ReadWriteLock ctrlConfigLockRef
    )
    {
        ctrlConfApiCallHandler = ctrlConfApiCallHandlerRef;
        authApiCallHandler = authApiCallHandlerRef;
        fullSyncApiCallHandler = fullSyncApiCallHandlerRef;
        nodeApiCallHandler = nodeApiCallHandlerRef;
        rscDfnApiCallHandler = rscDfnApiCallHandlerRef;
        vlmDfnApiCallHandler = vlmDfnApiCallHandlerRef;
        rscApiCallHandler = rscApiCallHandlerRef;
        vlmApiCallHandler = vlmApiCallHandlerRef;
        storPoolDfnApiCallHandler = storPoolDfnApiCallHandlerRef;
        storPoolApiCallHandler = storPoolApiCallHandlerRef;
        nodeConnApiCallHandler = nodeConnApiCallHandlerRef;
        rscConnApiCallHandler = rscConnApiCallHandlerRef;
        vlmConnApiCallHandler = vlmConnApiCallHandlerRef;
        ctrlClientcomSrzl = ctrlClientcomSrzlRef;
        netIfApiCallHandler = netIfApiCallHandlerRef;
        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        storPoolDfnMapLock = storPoolDfnMapLockRef;
        ctrlConfigLock = ctrlConfigLockRef;
    }

    public void sendFullSync(Peer client, long expectedFullSyncId)
    {
        try
        {
            nodesMapLock.readLock().lock();
            rscDfnMapLock.readLock().lock();
            storPoolDfnMapLock.readLock().lock();

            client.getSerializerLock().writeLock().lock();

            fullSyncApiCallHandler.sendFullSync(client, expectedFullSyncId);
        }
        finally
        {
            client.getSerializerLock().writeLock().unlock();

            nodesMapLock.readLock().unlock();
            rscDfnMapLock.readLock().unlock();
            storPoolDfnMapLock.readLock().unlock();
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
        Map<String, String> propsRef
    )
    {
        ApiCallRc apiCallRc;
        Map<String, String> props = propsRef;
        if (props == null)
        {
            props = Collections.emptyMap();
        }
        try
        {
            nodesMapLock.writeLock().lock();
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
            nodesMapLock.writeLock().unlock();
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
            nodesMapLock.writeLock().lock();
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
            nodesMapLock.writeLock().unlock();
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
            nodesMapLock.writeLock().lock();
            apiCallRc = nodeApiCallHandler.deleteNode(accCtx, client, nodeName);
        }
        finally
        {
            nodesMapLock.writeLock().unlock();

        }
        return apiCallRc;
    }

    public byte[] listNode(int msgId, AccessContext accCtx)
    {
        byte[] listNodes;
        try
        {
            nodesMapLock.readLock().lock();
            listNodes = nodeApiCallHandler.listNodes(msgId, accCtx);
        }
        finally
        {
            nodesMapLock.readLock().unlock();
        }
        return listNodes;
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
        String secretRef,
        String transportType,
        Map<String, String> propsRef,
        List<VlmDfnApi> vlmDescrMapRef
    )
    {
        ApiCallRc apiCallRc;
        String secret = secretRef;
        if (secret == null || secret.trim().isEmpty())
        {
            secret = SharedSecretGenerator.generateSharedSecret();
        }
        Map<String, String> props = propsRef;
        if (props == null)
        {
            props = Collections.emptyMap();
        }
        List<VlmDfnApi> vlmDescrMap = vlmDescrMapRef;
        if (vlmDescrMap == null)
        {
            vlmDescrMap = Collections.emptyList();
        }
        try
        {
            rscDfnMapLock.writeLock().lock();
            apiCallRc = rscDfnApiCallHandler.createResourceDefinition(
                accCtx,
                client,
                resourceName,
                port,
                secret,
                transportType,
                props,
                vlmDescrMapRef
            );
        }
        finally
        {
            rscDfnMapLock.writeLock().unlock();
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
            rscDfnMapLock.writeLock().lock();
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
            rscDfnMapLock.writeLock().unlock();
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
            rscDfnMapLock.writeLock().lock();
            apiCallRc = rscDfnApiCallHandler.deleteResourceDefinition(
                accCtx,
                client,
                resourceName
            );
        }
        finally
        {
            rscDfnMapLock.writeLock().unlock();
        }
        return apiCallRc;
    }

    public byte[] listResourceDefinition(int msgId, AccessContext accCtx)
    {
        byte[] listResourceDefinitions;
        try
        {
            rscDfnMapLock.readLock().lock();
            listResourceDefinitions = rscDfnApiCallHandler.listResourceDefinitions(msgId, accCtx);
        }
        finally
        {
            rscDfnMapLock.readLock().unlock();
        }
        return listResourceDefinitions;
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
        List<VlmDfnApi> vlmDfnApiListRef
    )
    {
        ApiCallRc apiCallRc;
        List<VlmDfnApi> vlmDfnApiList = vlmDfnApiListRef;
        if (vlmDfnApiList == null)
        {
            vlmDfnApiList = Collections.emptyList();
        }
        try
        {
            rscDfnMapLock.writeLock().lock();
            apiCallRc = vlmDfnApiCallHandler.createVolumeDefinitions(
                accCtx,
                client,
                rscName,
                vlmDfnApiList
            );
        }
        finally
        {
            rscDfnMapLock.writeLock().unlock();
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
        Map<String, String> overridePropsRef,
        Set<String> deletePropKeysRef
    )
    {
        ApiCallRc apiCallRc;


        Map<String, String> overrideProps = overridePropsRef;
        Set<String> deletePropKeys = deletePropKeysRef;
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
            rscDfnMapLock.writeLock().lock();
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
            rscDfnMapLock.writeLock().unlock();
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
            rscDfnMapLock.writeLock().lock();
            apiCallRc = vlmDfnApiCallHandler.deleteVolumeDefinition(
                accCtx,
                client,
                rscName,
                volumeNr
            );
        }
        finally
        {
            rscDfnMapLock.writeLock().unlock();
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
        List<String> flagList,
        Map<String, String> rscPropsMapRef,
        List<Volume.VlmApi> vlmApiDataListRef
    )
    {
        ApiCallRc apiCallRc;
        Map<String, String> rscPropsMap = rscPropsMapRef;
        List<Volume.VlmApi> vlmApiDataList = vlmApiDataListRef;
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
            nodesMapLock.writeLock().lock();
            rscDfnMapLock.writeLock().lock();

            apiCallRc = rscApiCallHandler.createResource(
                accCtx,
                client,
                nodeName,
                rscName,
                flagList,
                rscPropsMap,
                vlmApiDataList
            );
        }
        finally
        {
            rscDfnMapLock.writeLock().unlock();
            nodesMapLock.writeLock().unlock();
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
        Map<String, String> overridePropsRef,
        Set<String> deletePropKeysRef
    )
    {
        ApiCallRc apiCallRc;
        Map<String, String> overrideProps = overridePropsRef;
        Set<String> deletePropKeys = deletePropKeysRef;
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
            nodesMapLock.writeLock().lock();
            rscDfnMapLock.writeLock().lock();
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
            nodesMapLock.writeLock().unlock();
            rscDfnMapLock.writeLock().unlock();
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
            nodesMapLock.writeLock().lock();
            rscDfnMapLock.writeLock().lock();

            apiCallRc = rscApiCallHandler.deleteResource(
                accCtx,
                client,
                nodeName,
                rscName
            );
        }
        finally
        {
            rscDfnMapLock.writeLock().unlock();
            nodesMapLock.writeLock().unlock();
        }

        return apiCallRc;
    }

    public byte[] listResource(int msgId, AccessContext accCtx)
    {
        byte[] listResources;
        try
        {
            nodesMapLock.readLock().lock();
            rscDfnMapLock.readLock().lock();
            listResources = rscApiCallHandler.listResources(msgId, accCtx);
        }
        finally
        {
            rscDfnMapLock.readLock().unlock();
            nodesMapLock.readLock().unlock();
        }
        return listResources;
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
            nodesMapLock.writeLock().lock();
            rscDfnMapLock.writeLock().lock();

            apiCallRc = rscApiCallHandler.resourceDeleted(
                accCtx,
                client,
                nodeName,
                rscName
            );
        }
        finally
        {
            rscDfnMapLock.writeLock().unlock();
            nodesMapLock.writeLock().unlock();
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
            nodesMapLock.writeLock().lock();
            rscDfnMapLock.writeLock().lock();
            storPoolDfnMapLock.writeLock().lock();

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
            storPoolDfnMapLock.writeLock().unlock();
            rscDfnMapLock.writeLock().unlock();
            nodesMapLock.writeLock().unlock();
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
        Map<String, String> storPoolDfnProps = storPoolDfnPropsMap;
        if (storPoolDfnProps == null)
        {
            storPoolDfnProps = Collections.emptyMap();
        }
        try
        {
            storPoolDfnMapLock.writeLock().lock();
            apiCallRc = storPoolDfnApiCallHandler.createStorPoolDfn(
                accCtx,
                client,
                storPoolName,
                storPoolDfnProps
            );
        }
        finally
        {
            storPoolDfnMapLock.writeLock().unlock();
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
     * @param overridePropsRef optional
     * @param deletePropKeysRef optional
     * @return
     */
    public ApiCallRc modifyStorPoolDfn(
        AccessContext accCtx,
        Peer client,
        UUID storPoolDfnUuid,
        String storPoolName,
        Map<String, String> overridePropsRef,
        Set<String> deletePropKeysRef
    )
    {
        ApiCallRc apiCallRc;

        Map<String, String> overrideProps = overridePropsRef;
        Set<String> deletePropKeys = deletePropKeysRef;
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
            storPoolDfnMapLock.writeLock().lock();
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
            storPoolDfnMapLock.writeLock().unlock();
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
            storPoolDfnMapLock.writeLock().lock();
            apiCallRc = storPoolDfnApiCallHandler.deleteStorPoolDfn(
                accCtx,
                client,
                storPoolName
            );
        }
        finally
        {
            storPoolDfnMapLock.writeLock().unlock();
        }
        return apiCallRc;
    }

    public byte[] listStorPoolDefinition(int msgId, AccessContext accCtx)
    {
        byte[] listStorPoolDefinitions;
        try
        {
            storPoolDfnMapLock.readLock().lock();
            listStorPoolDefinitions = storPoolDfnApiCallHandler.listStorPoolDefinitions(msgId, accCtx);
        }
        finally
        {
            storPoolDfnMapLock.readLock().unlock();
        }
        return listStorPoolDefinitions;
    }

    public byte[] listStorPool(int msgId, AccessContext accCtx)
    {
        byte[] listStorPools;
        try
        {
            storPoolDfnMapLock.readLock().lock();
            listStorPools = storPoolApiCallHandler.listStorPools(msgId, accCtx);
        }
        finally
        {
            storPoolDfnMapLock.readLock().unlock();
        }
        return listStorPools;
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
        Map<String, String> storPoolProps = storPoolPropsMap;
        if (storPoolProps == null)
        {
            storPoolProps = Collections.emptyMap();
        }
        try
        {
            nodesMapLock.writeLock().lock();
            storPoolDfnMapLock.writeLock().lock();

            apiCallRc = storPoolApiCallHandler.createStorPool(
                accCtx,
                client,
                nodeName,
                storPoolName,
                driver,
                storPoolProps
            );
        }
        finally
        {
            storPoolDfnMapLock.writeLock().unlock();
            nodesMapLock.writeLock().unlock();
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
     * @param overridePropsRef optional
     * @param deletePropKeysRef optional
     * @return
     */
    public ApiCallRc modifyStorPool(
        AccessContext accCtx,
        Peer client,
        UUID storPoolUuid,
        String nodeName,
        String storPoolName,
        Map<String, String> overridePropsRef,
        Set<String> deletePropKeysRef
    )
    {
        ApiCallRc apiCallRc;

        Map<String, String> overrideProps = overridePropsRef;
        Set<String> deletePropKeys = deletePropKeysRef;
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
            nodesMapLock.writeLock().lock();
            storPoolDfnMapLock.writeLock().lock();
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
            nodesMapLock.writeLock().unlock();
            storPoolDfnMapLock.writeLock().unlock();
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
            nodesMapLock.writeLock().lock();
            storPoolDfnMapLock.writeLock().lock();

            apiCallRc = storPoolApiCallHandler.deleteStorPool(
                accCtx,
                client,
                nodeName,
                storPoolName
            );
        }
        finally
        {
            storPoolDfnMapLock.writeLock().unlock();
            nodesMapLock.writeLock().unlock();
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
        Map<String, String> nodeConnProps = nodeConnPropsMap;
        if (nodeConnProps == null)
        {
            nodeConnProps = Collections.emptyMap();
        }
        try
        {
            nodesMapLock.writeLock().lock();
            apiCallRc = nodeConnApiCallHandler.createNodeConnection(
                accCtx,
                client,
                nodeName1,
                nodeName2,
                nodeConnProps
            );
        }
        finally
        {
            nodesMapLock.writeLock().unlock();
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
     * @param overridePropsRef optional, can be empty
     * @param deletePropKeysRef optional, can be empty
     * @return
     */
    public ApiCallRc modifyNodeConn(
        AccessContext accCtx,
        Peer client,
        UUID nodeConnUuid,
        String nodeName1,
        String nodeName2,
        Map<String, String> overridePropsRef,
        Set<String> deletePropKeysRef
    )
    {
        Map<String, String> overrideProps = overridePropsRef;
        Set<String> deletePropKeys = deletePropKeysRef;
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
            nodesMapLock.writeLock().lock();
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
            nodesMapLock.writeLock().unlock();
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
            nodesMapLock.writeLock().lock();
            apiCallRc = nodeConnApiCallHandler.deleteNodeConnection(
                accCtx,
                client,
                nodeName1,
                nodeName2
            );
        }
        finally
        {
            nodesMapLock.writeLock().unlock();
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
        Map<String, String> rscConnProps = rscConnPropsMap;
        if (rscConnProps == null)
        {
            rscConnProps = Collections.emptyMap();
        }
        try
        {
            nodesMapLock.writeLock().lock();
            rscDfnMapLock.writeLock().lock();
            apiCallRc = rscConnApiCallHandler.createResourceConnection(
                accCtx,
                client,
                nodeName1,
                nodeName2,
                rscName,
                rscConnProps
            );
        }
        finally
        {
            rscDfnMapLock.writeLock().unlock();
            nodesMapLock.writeLock().unlock();
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
     * @param overridePropsRef optional
     * @param deletePropKeysRef optional
     * @return
     */
    public ApiCallRc modifyRscConn(
        AccessContext accCtx,
        Peer client,
        UUID rscConnUuid,
        String nodeName1,
        String nodeName2,
        String rscName,
        Map<String, String> overridePropsRef,
        Set<String> deletePropKeysRef
    )
    {
        ApiCallRc apiCallRc;

        Map<String, String> overrideProps = overridePropsRef;
        Set<String> deletePropKeys = deletePropKeysRef;
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
            nodesMapLock.writeLock().lock();
            rscDfnMapLock.writeLock().lock();
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
            nodesMapLock.writeLock().unlock();
            rscDfnMapLock.writeLock().unlock();
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
            nodesMapLock.writeLock().lock();
            rscDfnMapLock.writeLock().lock();
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
            rscDfnMapLock.writeLock().unlock();
            nodesMapLock.writeLock().unlock();
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
        Map<String, String> vlmConnProps = vlmConnPropsMap;
        if (vlmConnProps == null)
        {
            vlmConnProps = Collections.emptyMap();
        }
        try
        {
            nodesMapLock.writeLock().lock();
            rscDfnMapLock.writeLock().lock();
            apiCallRc = vlmConnApiCallHandler.createVolumeConnection(
                accCtx,
                client,
                nodeName1,
                nodeName2,
                rscName,
                vlmNr,
                vlmConnProps
            );
        }
        finally
        {
            rscDfnMapLock.writeLock().unlock();
            nodesMapLock.writeLock().unlock();
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
     * @param overridePropsRef optional
     * @param deletePropKeysRef optional
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
        Map<String, String> overridePropsRef,
        Set<String> deletePropKeysRef
    )
    {
        ApiCallRc apiCallRc;

        Map<String, String> overrideProps = overridePropsRef;
        Set<String> deletePropKeys = deletePropKeysRef;
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
            nodesMapLock.writeLock().lock();
            rscDfnMapLock.writeLock().lock();
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
            nodesMapLock.writeLock().unlock();
            rscDfnMapLock.writeLock().unlock();
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
            nodesMapLock.writeLock().lock();
            rscDfnMapLock.writeLock().lock();
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
            rscDfnMapLock.writeLock().unlock();
            nodesMapLock.writeLock().unlock();
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
            nodesMapLock.readLock().lock();
            rscDfnMapLock.readLock().lock();
            storPoolDfnMapLock.readLock().lock();

            rscApiCallHandler.respondResource(msgId, satellite, nodeName, rscUuid, rscName);
        }
        finally
        {
            nodesMapLock.readLock().unlock();
            rscDfnMapLock.readLock().unlock();
            storPoolDfnMapLock.readLock().unlock();
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
            nodesMapLock.readLock().lock();
            storPoolDfnMapLock.readLock().lock();

            storPoolApiCallHandler.respondStorPool(
                msgId,
                satellite,
                storPoolUuid,
                storPoolNameStr
            );
        }
        finally
        {
            nodesMapLock.readLock().unlock();
            storPoolDfnMapLock.readLock().unlock();
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
            nodesMapLock.readLock().lock();

            nodeApiCallHandler.respondNode(
                msgId,
                satellite,
                nodeUuid,
                nodeNameStr
            );
        }
        finally
        {
            nodesMapLock.readLock().unlock();
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
            rscDfnMapLock.writeLock().lock();
            rscDfnApiCallHandler.handlePrimaryResourceRequest(accCtx, satellite, msgId, rscName, rscUuid);
        }
        finally
        {
            rscDfnMapLock.writeLock().unlock();
        }
    }

    public CtrlClientSerializer getCtrlClientcomSrzl()
    {
        return ctrlClientcomSrzl;
    }

    public ApiCallRc setCtrlCfgProp(AccessContext accCtx, String key, String namespace, String value)
    {
        ApiCallRc apiCallRc;
        try
        {
            ctrlConfigLock.writeLock().lock();
            apiCallRc  = ctrlConfApiCallHandler.setProp(accCtx, key, namespace, value);
        }
        finally
        {
            ctrlConfigLock.writeLock().unlock();
        }
        return apiCallRc;
    }

    public byte[] listCtrlCfg(AccessContext accCtx, int msgId)
    {
        byte[] data;
        try
        {
            ctrlConfigLock.readLock().lock();
            data  = ctrlConfApiCallHandler.listProps(accCtx, msgId);
        }
        finally
        {
            ctrlConfigLock.readLock().unlock();
        }
        return data;
    }

    public ApiCallRc deleteCtrlCfgProp(AccessContext accCtx, String key, String namespace)
    {
        ApiCallRc apiCallRc;
        try
        {
            ctrlConfigLock.writeLock().lock();
            apiCallRc  = ctrlConfApiCallHandler.deleteProp(accCtx, key, namespace);
        }
        finally
        {
            ctrlConfigLock.writeLock().unlock();
        }
        return apiCallRc;
    }

    public ApiCallRc createNetInterface(
        AccessContext accCtx,
        Peer client,
        String nodeName,
        String netIfName,
        String address
    )
    {
        ApiCallRc apiCallRc;
        Lock ctrlReadLock = ctrlConfigLock.readLock();
        Lock nodeWriteLock = nodesMapLock.writeLock();
        try
        {
            ctrlReadLock.lock();
            nodeWriteLock.lock();

            apiCallRc = netIfApiCallHandler.createNetIf(accCtx, client, nodeName, netIfName, address);
        }
        finally
        {
            nodeWriteLock.unlock();
            ctrlReadLock.unlock();
        }
        return apiCallRc;
    }
}
