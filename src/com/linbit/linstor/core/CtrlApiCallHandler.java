package com.linbit.linstor.core;

import com.linbit.linstor.NetInterface.NetInterfaceApi;
import com.linbit.linstor.Node;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition.VlmDfnApi;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.pojo.FreeSpacePojo;
import com.linbit.linstor.logging.ErrorReport;
import com.linbit.linstor.netcom.Peer;
import com.linbit.utils.LockSupport;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;

public class CtrlApiCallHandler
{
    private final CtrlConfApiCallHandler ctrlConfApiCallHandler;
    private final CtrlFullSyncApiCallHandler fullSyncApiCallHandler;
    private final CtrlNodeApiCallHandler nodeApiCallHandler;
    private final CtrlRscDfnApiCallHandler rscDfnApiCallHandler;
    private final CtrlVlmDfnApiCallHandler vlmDfnApiCallHandler;
    private final CtrlRscApiCallHandler rscApiCallHandler;
    private final CtrlRscAutoPlaceApiCallHandler rscAutoPlaceApiCallHandler;
    private final CtrlVlmApiCallHandler vlmApiCallHandler;
    private final CtrlStorPoolDfnApiCallHandler storPoolDfnApiCallHandler;
    private final CtrlStorPoolApiCallHandler storPoolApiCallHandler;
    private final CtrlNodeConnectionApiCallHandler nodeConnApiCallHandler;
    private final CtrlRscConnectionApiCallHandler rscConnApiCallHandler;
    private final CtrlVlmConnectionApiCallHandler vlmConnApiCallHandler;
    private final CtrlNetIfApiCallHandler netIfApiCallHandler;
    private final CtrlWatchApiCallHandler watchApiCallHandler;
    private final CtrlSnapshotApiCallHandler snapshotApiCallHandler;

    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock rscDfnMapLock;
    private final ReadWriteLock storPoolDfnMapLock;
    private final ReadWriteLock ctrlConfigLock;
    private final ReadWriteLock ctrlErrorListLock;

    private final Provider<Peer> peer;
    private final Provider<Integer> msgId;

    @Inject
    CtrlApiCallHandler(
        CtrlConfApiCallHandler ctrlConfApiCallHandlerRef,
        CtrlFullSyncApiCallHandler fullSyncApiCallHandlerRef,
        CtrlNodeApiCallHandler nodeApiCallHandlerRef,
        CtrlRscDfnApiCallHandler rscDfnApiCallHandlerRef,
        CtrlVlmDfnApiCallHandler vlmDfnApiCallHandlerRef,
        CtrlRscApiCallHandler rscApiCallHandlerRef,
        CtrlRscAutoPlaceApiCallHandler rscAutoPlaceApiCallHandlerRef,
        CtrlVlmApiCallHandler vlmApiCallHandlerRef,
        CtrlStorPoolDfnApiCallHandler storPoolDfnApiCallHandlerRef,
        CtrlStorPoolApiCallHandler storPoolApiCallHandlerRef,
        CtrlNodeConnectionApiCallHandler nodeConnApiCallHandlerRef,
        CtrlRscConnectionApiCallHandler rscConnApiCallHandlerRef,
        CtrlVlmConnectionApiCallHandler vlmConnApiCallHandlerRef,
        CtrlNetIfApiCallHandler netIfApiCallHandlerRef,
        CtrlWatchApiCallHandler watchApiCallHandlerRef,
        CtrlSnapshotApiCallHandler snapshotApiCallHandlerRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef,
        @Named(ControllerCoreModule.CTRL_CONF_LOCK) ReadWriteLock ctrlConfigLockRef,
        @Named(ControllerCoreModule.CTRL_ERROR_LIST_LOCK) ReadWriteLock errorListLockRef,
        Provider<Peer> clientRef,
        @Named(ApiModule.MSG_ID) Provider<Integer> msgIdRef
    )
    {
        ctrlConfApiCallHandler = ctrlConfApiCallHandlerRef;
        fullSyncApiCallHandler = fullSyncApiCallHandlerRef;
        nodeApiCallHandler = nodeApiCallHandlerRef;
        rscDfnApiCallHandler = rscDfnApiCallHandlerRef;
        vlmDfnApiCallHandler = vlmDfnApiCallHandlerRef;
        rscApiCallHandler = rscApiCallHandlerRef;
        rscAutoPlaceApiCallHandler = rscAutoPlaceApiCallHandlerRef;
        vlmApiCallHandler = vlmApiCallHandlerRef;
        storPoolDfnApiCallHandler = storPoolDfnApiCallHandlerRef;
        storPoolApiCallHandler = storPoolApiCallHandlerRef;
        nodeConnApiCallHandler = nodeConnApiCallHandlerRef;
        rscConnApiCallHandler = rscConnApiCallHandlerRef;
        vlmConnApiCallHandler = vlmConnApiCallHandlerRef;
        netIfApiCallHandler = netIfApiCallHandlerRef;
        watchApiCallHandler = watchApiCallHandlerRef;
        snapshotApiCallHandler = snapshotApiCallHandlerRef;
        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        storPoolDfnMapLock = storPoolDfnMapLockRef;
        ctrlConfigLock = ctrlConfigLockRef;
        ctrlErrorListLock = errorListLockRef;
        peer = clientRef;
        msgId = msgIdRef;
    }

    public void sendFullSync(long expectedFullSyncId)
    {
        try (
            LockSupport ls = LockSupport.lock(
                nodesMapLock.readLock(),
                rscDfnMapLock.readLock(),
                storPoolDfnMapLock.readLock(),
                peer.get().getSerializerLock().writeLock()
            )
        )
        {
            fullSyncApiCallHandler.sendFullSync(peer.get(), expectedFullSyncId);
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
        Map<String, String> propsRef
    )
    {
        ApiCallRc apiCallRc;
        Map<String, String> props = propsRef;
        if (props == null)
        {
            props = Collections.emptyMap();
        }
        try (LockSupport ls = LockSupport.lock(nodesMapLock.writeLock()))
        {
            apiCallRc = nodeApiCallHandler.createNode(
                nodeNameStr,
                nodeTypeStr,
                netIfs,
                props
            );
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
        try (LockSupport ls = LockSupport.lock(nodesMapLock.writeLock()))
        {
            apiCallRc = nodeApiCallHandler.modifyNode(
                nodeUuid,
                nodeName,
                nodeType,
                overrideProps,
                deletePropKeys
            );
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
        try (LockSupport lock = LockSupport.lock(nodesMapLock.writeLock()))
        {
            apiCallRc = nodeApiCallHandler.deleteNode(nodeName);
        }
        return apiCallRc;
    }

    public byte[] listNode()
    {
        byte[] listNodes;
        try (LockSupport ls = LockSupport.lock(nodesMapLock.readLock()))
        {
            listNodes = nodeApiCallHandler.listNodes(msgId.get());
        }
        return listNodes;
    }

    public void listErrorReports(
        final Peer client,
        final Set<String> nodes,
        boolean withContent,
        final Optional<Date> since,
        final Optional<Date> to,
        final Set<String> ids
    )
    {
        try (LockSupport ls = LockSupport.lock(ctrlErrorListLock.writeLock()))
        {
            nodeApiCallHandler.listErrorReports(
                client,
                msgId.get(),
                nodes,
                withContent,
                since,
                to,
                ids
            );
        }
    }

    public void appendErrorReports(
        final Peer client,
        Set<ErrorReport> errorReports
    )
    {
        try (LockSupport ls = LockSupport.lock(ctrlErrorListLock.writeLock()))
        {
            nodeApiCallHandler.appendErrorReports(
                client,
                msgId.get(),
                errorReports
            );
        }
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
            secret = SecretGenerator.generateSecretString(SecretGenerator.DRBD_SHARED_SECRET_SIZE);
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
        try (LockSupport ls = LockSupport.lock(rscDfnMapLock.writeLock()))
        {
            apiCallRc = rscDfnApiCallHandler.createResourceDefinition(
                resourceName,
                port,
                secret,
                transportType,
                props,
                vlmDescrMapRef
            );
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
        try (LockSupport ls = LockSupport.lock(rscDfnMapLock.writeLock()))
        {
            apiCallRc = rscDfnApiCallHandler.modifyRscDfn(
                rscDfnUuid,
                rscName,
                port,
                overrideProps,
                deletePropKeys
            );
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
        try (LockSupport ls = LockSupport.lock(rscDfnMapLock.writeLock()))
        {
            apiCallRc = rscDfnApiCallHandler.deleteResourceDefinition(resourceName);
        }
        return apiCallRc;
    }

    public byte[] listResourceDefinition()
    {
        byte[] listResourceDefinitions;
        try (LockSupport ls = LockSupport.lock(rscDfnMapLock.readLock()))
        {
            listResourceDefinitions = rscDfnApiCallHandler.listResourceDefinitions(msgId.get());
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
        try (LockSupport ls = LockSupport.lock(rscDfnMapLock.writeLock()))
        {
            apiCallRc = vlmDfnApiCallHandler.createVolumeDefinitions(
                rscName,
                vlmDfnApiList
            );
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
        try (LockSupport ls = LockSupport.lock(rscDfnMapLock.writeLock()))
        {
            apiCallRc = vlmDfnApiCallHandler.modifyVlmDfn(
                vlmDfnUuid,
                rscName,
                vlmNr,
                size,
                minorNr,
                overrideProps,
                deletePropKeys
            );
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
        try (LockSupport ls = LockSupport.lock(rscDfnMapLock.writeLock()))
        {
            apiCallRc = vlmDfnApiCallHandler.deleteVolumeDefinition(
                rscName,
                volumeNr
            );
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
        try (
            LockSupport ls = LockSupport.lock(
                nodesMapLock.writeLock(),
                rscDfnMapLock.writeLock()
            )
        )
        {
            apiCallRc = rscApiCallHandler.createResource(
                nodeName,
                rscName,
                flagList,
                rscPropsMap,
                vlmApiDataList
            );
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
        try (
            LockSupport ls = LockSupport.lock(
                nodesMapLock.writeLock(),
                rscDfnMapLock.writeLock()
            )
        )
        {
            apiCallRc = rscApiCallHandler.modifyResource(
                rscUuid,
                nodeName,
                rscName,
                overrideProps,
                deletePropKeys
            );
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
        try (
            LockSupport ls = LockSupport.lock(
                nodesMapLock.writeLock(),
                rscDfnMapLock.writeLock()
            )
        )
        {
            apiCallRc = rscApiCallHandler.deleteResource(
                nodeName,
                rscName
            );
        }

        return apiCallRc;
    }

    public byte[] listResource(List<String> filterNodes, List<String> filterResources)
    {
        byte[] listResources;
        try (
            LockSupport ls = LockSupport.lock(
                nodesMapLock.readLock(),
                rscDfnMapLock.readLock()
            )
        )
        {
            listResources = rscApiCallHandler.listResources(
                msgId.get(),
                filterNodes,
                filterResources
            );
        }
        return listResources;
    }

    public byte[] listVolumes(List<String> filterNodes, List<String> filterStorPools, List<String> filterResources)
    {
        byte[] listVolumes;
        try
        {
            nodesMapLock.readLock().lock();
            rscDfnMapLock.readLock().lock();
            listVolumes = vlmApiCallHandler.listVolumes(
                msgId.get(),
                filterNodes,
                filterStorPools,
                filterResources
            );
        }
        finally
        {
            rscDfnMapLock.readLock().unlock();
            nodesMapLock.readLock().unlock();
        }
        return listVolumes;
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
        try (
            LockSupport ls = LockSupport.lock(
                nodesMapLock.writeLock(),
                rscDfnMapLock.writeLock()
            )
        )
        {
            apiCallRc = rscApiCallHandler.resourceDeleted(
                nodeName,
                rscName
            );
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
        try (
            LockSupport ls = LockSupport.lock(
                nodesMapLock.writeLock(),
                rscDfnMapLock.writeLock(),
                storPoolDfnMapLock.writeLock()
            )
        )
        {
            apiCallRc = vlmApiCallHandler.volumeDeleted(
                nodeName,
                rscName,
                volumeNr
            );
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
        try (LockSupport ls = LockSupport.lock(storPoolDfnMapLock.writeLock()))
        {
            apiCallRc = storPoolDfnApiCallHandler.createStorPoolDfn(
                storPoolName,
                storPoolDfnProps
            );
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
        try (LockSupport ls = LockSupport.lock(storPoolDfnMapLock.writeLock()))
        {
            apiCallRc = storPoolDfnApiCallHandler.modifyStorPoolDfn(
                storPoolDfnUuid,
                storPoolName,
                overrideProps,
                deletePropKeys
            );
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
    public ApiCallRc deleteStoragePoolDefinition(String storPoolName)
    {
        ApiCallRc apiCallRc;
        try (LockSupport ls = LockSupport.lock(storPoolDfnMapLock.writeLock()))
        {
            apiCallRc = storPoolDfnApiCallHandler.deleteStorPoolDfn(
                storPoolName
            );
        }
        return apiCallRc;
    }

    public byte[] listStorPoolDefinition()
    {
        byte[] listStorPoolDefinitions;
        try (LockSupport ls = LockSupport.lock(storPoolDfnMapLock.readLock()))
        {
            listStorPoolDefinitions = storPoolDfnApiCallHandler.listStorPoolDefinitions(msgId.get());
        }
        return listStorPoolDefinitions;
    }

    public byte[] listStorPool(List<String> filterNodes, List<String> filterStorPools)
    {
        byte[] listStorPools;
        try (LockSupport ls = LockSupport.lock(storPoolDfnMapLock.readLock()))
        {
            listStorPools = storPoolApiCallHandler.listStorPools(
                msgId.get(),
                filterNodes,
                filterStorPools
            );
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
        try (
            LockSupport ls = LockSupport.lock(
                nodesMapLock.writeLock(),
                storPoolDfnMapLock.writeLock()
            )
        )
        {
            apiCallRc = storPoolApiCallHandler.createStorPool(
                nodeName,
                storPoolName,
                driver,
                storPoolProps
            );
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
        try (
            LockSupport ls = LockSupport.lock(
                nodesMapLock.writeLock(),
                storPoolDfnMapLock.writeLock()
            )
        )
        {
            apiCallRc = storPoolApiCallHandler.modifyStorPool(
                storPoolUuid,
                nodeName,
                storPoolName,
                overrideProps,
                deletePropKeys
            );
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
        try (
            LockSupport ls = LockSupport.lock(
                nodesMapLock.writeLock(),
                storPoolDfnMapLock.writeLock()
            )
        )
        {
            apiCallRc = storPoolApiCallHandler.deleteStorPool(
                nodeName,
                storPoolName
            );
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
        try (LockSupport ls = LockSupport.lock(nodesMapLock.writeLock()))
        {
            apiCallRc = nodeConnApiCallHandler.createNodeConnection(
                nodeName1,
                nodeName2,
                nodeConnProps
            );
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
        try (LockSupport ls = LockSupport.lock(nodesMapLock.writeLock()))
        {
            apiCallRc = nodeConnApiCallHandler.modifyNodeConnection(
                nodeConnUuid,
                nodeName1,
                nodeName2,
                overrideProps,
                deletePropKeys
            );
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
        try (LockSupport ls = LockSupport.lock(nodesMapLock.writeLock()))
        {
            apiCallRc = nodeConnApiCallHandler.deleteNodeConnection(
                nodeName1,
                nodeName2
            );
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
        try (
            LockSupport ls = LockSupport.lock(
                nodesMapLock.writeLock(),
                rscDfnMapLock.writeLock()
            )
        )
        {
            apiCallRc = rscConnApiCallHandler.createResourceConnection(
                nodeName1,
                nodeName2,
                rscName,
                rscConnProps
            );
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
        try (
            LockSupport ls = LockSupport.lock(
                nodesMapLock.writeLock(),
                rscDfnMapLock.writeLock()
            )
        )
        {
            apiCallRc = rscConnApiCallHandler.modifyRscConnection(
                rscConnUuid,
                nodeName1,
                nodeName2,
                rscName,
                overrideProps,
                deletePropKeys
            );
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
        try (
            LockSupport ls = LockSupport.lock(
                nodesMapLock.writeLock(),
                rscDfnMapLock.writeLock()
            )
        )
        {
            apiCallRc = rscConnApiCallHandler.deleteResourceConnection(
                nodeName1,
                nodeName2,
                rscName
            );
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
        try (
            LockSupport ls = LockSupport.lock(
                nodesMapLock.writeLock(),
                rscDfnMapLock.writeLock()
            )
        )
        {
            apiCallRc = vlmConnApiCallHandler.createVolumeConnection(
                nodeName1,
                nodeName2,
                rscName,
                vlmNr,
                vlmConnProps
            );
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
        try (
            LockSupport ls = LockSupport.lock(
                nodesMapLock.writeLock(),
                rscDfnMapLock.writeLock()
            )
        )
        {
            apiCallRc = vlmConnApiCallHandler.modifyVolumeConnection(
                vlmConnUuid,
                nodeName1,
                nodeName2,
                rscName,
                vlmNr,
                overrideProps,
                deletePropKeys
            );
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
        try (
            LockSupport ls = LockSupport.lock(
                nodesMapLock.writeLock(),
                rscDfnMapLock.writeLock()
            )
        )
        {
            apiCallRc = vlmConnApiCallHandler.deleteVolumeConnection(
                nodeName1,
                nodeName2,
                rscName,
                vlmNr
            );
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
        try (
            LockSupport ls = LockSupport.lock(
                nodesMapLock.readLock(),
                rscDfnMapLock.readLock(),
                storPoolDfnMapLock.readLock()
            )
        )
        {
            rscApiCallHandler.respondResource(msgId.get(), nodeName, rscUuid, rscName);
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
        try (
            LockSupport ls = LockSupport.lock(
                nodesMapLock.readLock(),
                storPoolDfnMapLock.readLock()
            )
        )
        {
            storPoolApiCallHandler.respondStorPool(
                msgId.get(),
                storPoolUuid,
                storPoolNameStr
            );
        }
    }

    public void handleControllerRequest(
        UUID nodeUuid,
        String nodeNameStr
    )
    {
        try (LockSupport ls = LockSupport.lock(nodesMapLock.readLock()))
        {
            ctrlConfApiCallHandler.respondController(
                msgId.get(),
                nodeUuid,
                nodeNameStr
            );
        }
    }

    public void handleNodeRequest(
        UUID nodeUuid,
        String nodeNameStr
    )
    {
        try (LockSupport ls = LockSupport.lock(nodesMapLock.readLock()))
        {
            nodeApiCallHandler.respondNode(
                msgId.get(),
                nodeUuid,
                nodeNameStr
            );
        }
    }

    public void handlePrimaryResourceRequest(
        String rscName,
        UUID rscUuid
    )
    {
        try (LockSupport ls = LockSupport.lock(rscDfnMapLock.writeLock()))
        {
            rscDfnApiCallHandler.handlePrimaryResourceRequest(msgId.get(), rscName, rscUuid);
        }
    }

    public ApiCallRc setCtrlCfgProp(String key, String namespace, String value)
    {
        ApiCallRc apiCallRc;
        try (LockSupport ls = LockSupport.lock(ctrlConfigLock.writeLock()))
        {
            apiCallRc  = ctrlConfApiCallHandler.setProp(key, namespace, value);
        }
        return apiCallRc;
    }

    public byte[] listCtrlCfg()
    {
        byte[] data;
        try (LockSupport ls = LockSupport.lock(ctrlConfigLock.readLock()))
        {
            data  = ctrlConfApiCallHandler.listProps(msgId.get());
        }
        return data;
    }

    public ApiCallRc deleteCtrlCfgProp(String key, String namespace)
    {
        ApiCallRc apiCallRc;
        try (LockSupport ls = LockSupport.lock(ctrlConfigLock.writeLock()))
        {
            apiCallRc  = ctrlConfApiCallHandler.deleteProp(key, namespace);
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

        try (
            LockSupport ls = LockSupport.lock(
                ctrlConfigLock.readLock(),
                nodesMapLock.readLock()
            )
        )
        {
            apiCallRc = netIfApiCallHandler.createNetIf(
                nodeName,
                netIfName,
                address,
                stltPort,
                stltEncrType
            );
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

        try (
            LockSupport ls = LockSupport.lock(
                ctrlConfigLock.readLock(),
                nodesMapLock.readLock()
            )
        )
        {
            apiCallRc = netIfApiCallHandler.modifyNetIf(
                nodeName,
                netIfName,
                address,
                stltPort,
                stltEncrType
            );
        }
        return apiCallRc;
    }

    public ApiCallRc deleteNetInterface(
        String nodeName,
        String netIfName
    )
    {
        ApiCallRc apiCallRc;

        try (
            LockSupport ls = LockSupport.lock(
                ctrlConfigLock.readLock(),
                nodesMapLock.readLock()
            )
        )
        {
            apiCallRc = netIfApiCallHandler.deleteNetIf(nodeName, netIfName);
        }
        return apiCallRc;
    }

    public ApiCallRc createWatch(
        int peerWatchId,
        String eventName,
        String nodeNameStr,
        String resourceNameStr,
        Integer volumeNumber,
        String snapshotNameStr
    )
    {
        return watchApiCallHandler.createWatch(
            peerWatchId, eventName, nodeNameStr, resourceNameStr, volumeNumber, snapshotNameStr
        );
    }

    public void updateRealFreeSpace(Peer satellitePeer, FreeSpacePojo... freeSpacePojos)
    {
        try (LockSupport ls = LockSupport.lock(nodesMapLock.writeLock(), storPoolDfnMapLock.writeLock()))
        {
            storPoolApiCallHandler.updateRealFreeSpace(satellitePeer, freeSpacePojos);
        }
    }

    public ApiCallRc createResourcesAutoPlace(
        String rscName,
        int placeCount,
        String storPoolName,
        List<String> notPlaceWithRscList,
        String notPlaceWithRscRegex
    )
    {
        ApiCallRc apiCallRc;

        try (
            LockSupport ls = LockSupport.lock(
                ctrlConfigLock.writeLock(),
                nodesMapLock.writeLock(),
                rscDfnMapLock.writeLock(),
                storPoolDfnMapLock.writeLock()
            )
        )
        {
            apiCallRc = rscAutoPlaceApiCallHandler.autoPlace(
                rscName,
                placeCount,
                storPoolName,
                notPlaceWithRscList,
                notPlaceWithRscRegex
            );
        }
        return apiCallRc;
    }

    public ApiCallRc createSnapshot(
        String rscName,
        String snapshotName
    )
    {

        ApiCallRc apiCallRc;

        try (
            LockSupport ls = LockSupport.lock(
                nodesMapLock.readLock(),
                rscDfnMapLock.writeLock()
            )
        )
        {
            apiCallRc = snapshotApiCallHandler.createSnapshot(
                rscName,
                snapshotName
            );
        }
        return apiCallRc;
    }

    public ApiCallRc setMasterPassphrase(String newPassphrase, String oldPassphrase)
    {
        ApiCallRc apiCallRc;
        try (LockSupport ls = LockSupport.lock(ctrlConfigLock.writeLock()))
        {
            apiCallRc = ctrlConfApiCallHandler.setPassphrase(newPassphrase, oldPassphrase);
        }
        return apiCallRc;
    }

    public ApiCallRc enterPassphrase(String passphrase)
    {
        ApiCallRc apiCallRc;
        try (LockSupport ls = LockSupport.lock(ctrlConfigLock.writeLock()))
        {
            apiCallRc = ctrlConfApiCallHandler.enterPassphrase(passphrase);
        }
        return apiCallRc;
    }
}
