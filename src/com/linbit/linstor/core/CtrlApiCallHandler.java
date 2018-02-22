package com.linbit.linstor.core;

import com.linbit.linstor.NetInterface.NetInterfaceApi;
import com.linbit.linstor.Node;
import com.linbit.linstor.SatelliteConnection.SatelliteConnectionApi;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition.VlmDfnApi;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.pojo.FreeSpacePojo;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

public class CtrlApiCallHandler
{
    private final CtrlConfApiCallHandler ctrlConfApiCallHandler;
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

    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock rscDfnMapLock;
    private final ReadWriteLock storPoolDfnMapLock;
    private final ReadWriteLock ctrlConfigLock;

    private final AccessContext accCtx;
    private final Peer peer;
    private final int msgId;

    @Inject
    CtrlApiCallHandler(
        CtrlConfApiCallHandler ctrlConfApiCallHandlerRef,
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
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef,
        @Named(ControllerCoreModule.CTRL_CONF_LOCK) ReadWriteLock ctrlConfigLockRef,
        @PeerContext AccessContext accCtxRef,
        Peer clientRef,
        @Named(ApiModule.MSG_ID) int msgIdRef
    )
    {
        ctrlConfApiCallHandler = ctrlConfApiCallHandlerRef;
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
        netIfApiCallHandler = netIfApiCallHandlerRef;
        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        storPoolDfnMapLock = storPoolDfnMapLockRef;
        ctrlConfigLock = ctrlConfigLockRef;
        accCtx = accCtxRef;
        peer = clientRef;
        msgId = msgIdRef;
    }

    public void sendFullSync(long expectedFullSyncId)
    {
        try
        {
            nodesMapLock.readLock().lock();
            rscDfnMapLock.readLock().lock();
            storPoolDfnMapLock.readLock().lock();

            peer.getSerializerLock().writeLock().lock();

            fullSyncApiCallHandler.sendFullSync(peer, expectedFullSyncId);
        }
        finally
        {
            peer.getSerializerLock().writeLock().unlock();

            storPoolDfnMapLock.readLock().unlock();
            rscDfnMapLock.readLock().unlock();
            nodesMapLock.readLock().unlock();
        }
    }

    /**
     * Creates a new {@link Node}
     *
     * @param nodeNameStr required
     * @param nodeTypeStr required
     * @param netIfs required, at least one needed
     * @param satelliteConnectionApis required, currently all but first ignored. At least one required
     * @param props optional
     * @return
     */
    public ApiCallRc createNode(
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
                peer,
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
     * @param nodeUuid optional - if given, modification is only performed if it matches the found
     *   node's UUID.
     * @param nodeName required
     * @param nodeType optional - if given, attempts to modify the type of the node
     * @param overrideProps required (can be empty) - overrides the given property key-value pairs
     * @param deletePropKeys required (can be empty) - deletes the given property keys
     * @return
     */
    public ApiCallRc modifyNode(
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
                peer,
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
     * @param nodeName required
     * @return
     */
    public ApiCallRc deleteNode(String nodeName)
    {
        ApiCallRc apiCallRc;
        try
        {
            nodesMapLock.writeLock().lock();
            apiCallRc = nodeApiCallHandler.deleteNode(accCtx, peer, nodeName);
        }
        finally
        {
            nodesMapLock.writeLock().unlock();

        }
        return apiCallRc;
    }

    public byte[] listNode()
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
     * @param resourceName required
     * @param port optional
     * @param secret optional
     * @param props optional
     * @param volDescrMap optional
     * @return
     */
    public ApiCallRc createResourceDefinition(
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
                peer,
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
                peer,
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
     * @param resourceName required
     * @return
     */
    public ApiCallRc deleteResourceDefinition(
        String resourceName
    )
    {
        ApiCallRc apiCallRc;
        try
        {
            rscDfnMapLock.writeLock().lock();
            apiCallRc = rscDfnApiCallHandler.deleteResourceDefinition(
                accCtx,
                peer,
                resourceName
            );
        }
        finally
        {
            rscDfnMapLock.writeLock().unlock();
        }
        return apiCallRc;
    }

    public byte[] listResourceDefinition()
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
     * @param rscName required
     * @param vlmDfnApiList optional
     * @return
     */
    public ApiCallRc createVlmDfns(
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
                peer,
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
                peer,
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
     * @param rscName required
     * @param volumeNr required
     * @return ApiCallResponse with status of the operation
     */
    public ApiCallRc deleteVolumeDefinition(
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
                peer,
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
     * @param nodeName required
     * @param rscName required
     * @param rscPropsMap optional
     * @param vlmApiDataList optional
     * @return
     */
    public ApiCallRc createResource(
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
                peer,
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
     * @param rscUuid optional, if given checked against persisted UUID
     * @param nodeName required
     * @param rscName required
     * @param overrideProps optional
     * @param deletePropKeys optional
     * @return
     */
    public ApiCallRc modifyRsc(
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
                peer,
                rscUuid,
                nodeName,
                rscName,
                overrideProps,
                deletePropKeys
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
     * Marks a {@link Resource} for deletion.
     *
     * The {@link Resource} is only deleted once the corresponding satellite confirmed
     * that it has undeployed (deleted) the {@link Resource}
     *
     * @param nodeName required
     * @param rscName required
     * @return
     */
    public ApiCallRc deleteResource(
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
                peer,
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

    public byte[] listResource()
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
     * @param nodeName required
     * @param rscName required
     * @return
     */
    public ApiCallRc resourceDeleted(
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
                peer,
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
     * @param nodeName required
     * @param rscName required
     * @return
     */
    public ApiCallRc volumeDeleted(
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
                peer,
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
     * @param storPoolName required
     * @param storPoolDfnPropsMap optional
     * @return
     */
    public ApiCallRc createStoragePoolDefinition(
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
                peer,
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
     * @param storPoolDfnUuid optional, if given checked against persisted UUID
     * @param storPoolName required
     * @param overridePropsRef optional
     * @param deletePropKeysRef optional
     * @return
     */
    public ApiCallRc modifyStorPoolDfn(
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
                peer,
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
     * @param storPoolName required
     * @return
     */
    public ApiCallRc deleteStoragePoolDefinition(
        String storPoolName
    )
    {
        ApiCallRc apiCallRc;
        try
        {
            storPoolDfnMapLock.writeLock().lock();
            apiCallRc = storPoolDfnApiCallHandler.deleteStorPoolDfn(
                accCtx,
                peer,
                storPoolName
            );
        }
        finally
        {
            storPoolDfnMapLock.writeLock().unlock();
        }
        return apiCallRc;
    }

    public byte[] listStorPoolDefinition()
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

    public byte[] listStorPool()
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
     * @param nodeName required
     * @param storPoolName required
     * @param driver required
     * @param storPoolPropsMap optional
     * @return
     */
    public ApiCallRc createStoragePool(
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
                peer,
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
     * @param storPoolUuid optional, if given checked against persisted UUID
     * @param nodeName required
     * @param storPoolName required
     * @param overridePropsRef optional
     * @param deletePropKeysRef optional
     * @return
     */
    public ApiCallRc modifyStorPool(
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
                peer,
                storPoolUuid,
                nodeName,
                storPoolName,
                overrideProps,
                deletePropKeys
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
     * Marks the {@link StorPool} for deletion.
     *
     * The {@link StorPool} is only deleted once the corresponding satellite
     * confirms that it has undeployed (deleted) the {@link StorPool}.
     *
     * @param nodeName
     * @param storPoolName
     * @return
     */
    public ApiCallRc deleteStoragePool(
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
                peer,
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
     * @param nodeName1 required
     * @param nodeName2 required
     * @param nodeConnPropsMap optional, recommended
     * @return
     */
    public ApiCallRc createNodeConnection(
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
                peer,
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
     * @param nodeConnUuid optional, if given checks against persisted uuid
     * @param nodeName1 required
     * @param nodeName2 required
     * @param overridePropsRef optional, can be empty
     * @param deletePropKeysRef optional, can be empty
     * @return
     */
    public ApiCallRc modifyNodeConn(
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
                peer,
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
     * @param nodeName required
     * @param storPoolName required
     * @return
     */
    public ApiCallRc deleteNodeConnection(
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
                peer,
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
     * @param nodeName1 required
     * @param nodeName2 required
     * @param rscName required
     * @param rscConnPropsMap optional, recommended
     * @return
     */
    public ApiCallRc createResourceConnection(
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
                peer,
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
     * @param rscConnUuid optional, if given checked against persisted UUID
     * @param nodeName1 required
     * @param nodeName2 required
     * @param rscName required
     * @param overridePropsRef optional
     * @param deletePropKeysRef optional
     * @return
     */
    public ApiCallRc modifyRscConn(
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
                peer,
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
            rscDfnMapLock.writeLock().unlock();
            nodesMapLock.writeLock().unlock();
        }
        return apiCallRc;
    }

    /**
     * Deletes a {@link ResourceConnection}.
     *
     * @param nodeName1 required
     * @param nodeName2 required
     * @param rscName required
     * @return
     */
    public ApiCallRc deleteResourceConnection(
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
                peer,
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
     * @param nodeName1 required
     * @param nodeName2 required
     * @param rscName required
     * @param vlmNr required
     * @param vlmConnPropsMap optional, recommended
     * @return
     */
    public ApiCallRc createVolumeConnection(
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
                peer,
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
                peer,
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
            rscDfnMapLock.writeLock().unlock();
            nodesMapLock.writeLock().unlock();
        }
        return apiCallRc;
    }

    /**
     * Deletes a {@link VolumeConnection}.
     *
     * @param nodeName1 required
     * @param nodeName2 required
     * @param rscName required
     * @param vlmNr required
     * @return
     */
    public ApiCallRc deleteVolumeConnection(
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
                peer,
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
     * @param rscUuid required (for double checking)
     * @param rscName required
     */
    public void handleResourceRequest(
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

            rscApiCallHandler.respondResource(msgId, peer, nodeName, rscUuid, rscName);
        }
        finally
        {
            storPoolDfnMapLock.readLock().unlock();
            rscDfnMapLock.readLock().unlock();
            nodesMapLock.readLock().unlock();
        }
    }

    /**
     * This method should be called when the controller sent a message to a satellite
     * that its storPools {@code storPoolName} has changed, and the satellite now
     * queries those changes.
     * Calling this method will collect the needed data and send it to the given
     * satellite.
     * @param storPoolUuid required (for double checking)
     * @param storPoolNameStr required
     */
    public void handleStorPoolRequest(
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
                peer,
                storPoolUuid,
                storPoolNameStr
            );
        }
        finally
        {
            storPoolDfnMapLock.readLock().unlock();
            nodesMapLock.readLock().unlock();
        }
    }

    public void handleNodeRequest(
        UUID nodeUuid,
        String nodeNameStr
    )
    {
        try
        {
            nodesMapLock.readLock().lock();

            nodeApiCallHandler.respondNode(
                msgId,
                peer,
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
        String rscName,
        UUID rscUuid
    )
    {
        try
        {
            rscDfnMapLock.writeLock().lock();
            rscDfnApiCallHandler.handlePrimaryResourceRequest(accCtx, peer, msgId, rscName, rscUuid);
        }
        finally
        {
            rscDfnMapLock.writeLock().unlock();
        }
    }

    public ApiCallRc setCtrlCfgProp(String key, String namespace, String value)
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

    public byte[] listCtrlCfg()
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

    public ApiCallRc deleteCtrlCfgProp(String key, String namespace)
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
        String nodeName,
        String netIfName,
        String address,
        Integer stltPort,
        String stltEncrType
    )
    {
        ApiCallRc apiCallRc;
        Lock ctrlReadLock = ctrlConfigLock.readLock();
        Lock nodeReadLock = nodesMapLock.readLock();
        try
        {
            ctrlReadLock.lock();
            nodeReadLock.lock();

            apiCallRc = netIfApiCallHandler.createNetIf(
                accCtx,
                peer,
                nodeName,
                netIfName,
                address,
                stltPort,
                stltEncrType
            );
        }
        finally
        {
            nodeReadLock.unlock();
            ctrlReadLock.unlock();
        }
        return apiCallRc;
    }

    public ApiCallRc modifyNetInterface(
        String nodeName,
        String netIfName,
        String address,
        Integer stltPort,
        String stltEncrType
    )
    {
        ApiCallRc apiCallRc;
        Lock ctrlReadLock = ctrlConfigLock.readLock();
        Lock nodeReadLock = nodesMapLock.readLock();
        try
        {
            ctrlReadLock.lock();
            nodeReadLock.lock();

            apiCallRc = netIfApiCallHandler.modifyNetIf(
                accCtx,
                peer,
                nodeName,
                netIfName,
                address,
                stltPort,
                stltEncrType
            );
        }
        finally
        {
            nodeReadLock.unlock();
            ctrlReadLock.unlock();
        }
        return apiCallRc;
    }

    public ApiCallRc deleteNetInterface(
        String nodeName,
        String netIfName
    )
    {
        ApiCallRc apiCallRc;
        Lock ctrlReadLock = ctrlConfigLock.readLock();
        Lock nodeReadLock = nodesMapLock.readLock();
        try
        {
            ctrlReadLock.lock();
            nodeReadLock.lock();

            apiCallRc = netIfApiCallHandler.deleteNetIf(accCtx, peer, nodeName, netIfName);
        }
        finally
        {
            nodeReadLock.unlock();
            ctrlReadLock.unlock();
        }
        return apiCallRc;
    }

    public void updateRealFreeSpace(Peer satellitePeer, FreeSpacePojo... freeSpacePojos)
    {
        Lock nodeWriteLock = nodesMapLock.writeLock();
        Lock storPoolWriteLock = storPoolDfnMapLock.writeLock();
        try
        {
            nodeWriteLock.lock();
            storPoolWriteLock.lock();

            storPoolApiCallHandler.updateRealFreeSpace(satellitePeer, freeSpacePojos);
        }
        finally
        {
            storPoolWriteLock.unlock();
            nodeWriteLock.unlock();
        }
    }
}
