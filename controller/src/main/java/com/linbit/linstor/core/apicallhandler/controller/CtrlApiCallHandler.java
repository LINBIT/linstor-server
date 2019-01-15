package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.NetInterface.NetInterfaceApi;
import com.linbit.linstor.Node;
import com.linbit.linstor.ResourceConnection;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.SnapshotDefinition;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolDefinitionData;
import com.linbit.linstor.VolumeDefinition.VlmDfnApi;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.core.ControllerCoreModule;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.SecretGenerator;
import com.linbit.linstor.core.apicallhandler.controller.helpers.ResourceList;
import com.linbit.locks.LockGuard;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;

@Singleton
public class CtrlApiCallHandler
{
    private final CtrlConfApiCallHandler ctrlConfApiCallHandler;
    private final CtrlNodeApiCallHandler nodeApiCallHandler;
    private final CtrlRscDfnApiCallHandler rscDfnApiCallHandler;
    private final CtrlVlmDfnApiCallHandler vlmDfnApiCallHandler;
    private final CtrlRscApiCallHandler rscApiCallHandler;
    private final CtrlStorPoolDfnApiCallHandler storPoolDfnApiCallHandler;
    private final CtrlStorPoolApiCallHandler storPoolApiCallHandler;
    private final CtrlNodeConnectionApiCallHandler nodeConnApiCallHandler;
    private final CtrlRscConnectionApiCallHandler rscConnApiCallHandler;
    private final CtrlVlmConnectionApiCallHandler vlmConnApiCallHandler;
    private final CtrlNetIfApiCallHandler netIfApiCallHandler;
    private final CtrlWatchApiCallHandler watchApiCallHandler;
    private final CtrlSnapshotApiCallHandler snapshotApiCallHandler;
    private final CtrlSnapshotRestoreApiCallHandler snapshotRestoreApiCallHandler;
    private final CtrlSnapshotRestoreVlmDfnApiCallHandler snapshotRestoreVlmDfnApiCallHandler;
    private final CtrlDrbdProxyModifyApiCallHandler drbdProxyModifyApiCallHandler;

    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock rscDfnMapLock;
    private final ReadWriteLock storPoolDfnMapLock;
    private final ReadWriteLock ctrlConfigLock;

    @Inject
    CtrlApiCallHandler(
        CtrlConfApiCallHandler ctrlConfApiCallHandlerRef,
        CtrlNodeApiCallHandler nodeApiCallHandlerRef,
        CtrlRscDfnApiCallHandler rscDfnApiCallHandlerRef,
        CtrlVlmDfnApiCallHandler vlmDfnApiCallHandlerRef,
        CtrlRscApiCallHandler rscApiCallHandlerRef,
        CtrlStorPoolDfnApiCallHandler storPoolDfnApiCallHandlerRef,
        CtrlStorPoolApiCallHandler storPoolApiCallHandlerRef,
        CtrlNodeConnectionApiCallHandler nodeConnApiCallHandlerRef,
        CtrlRscConnectionApiCallHandler rscConnApiCallHandlerRef,
        CtrlVlmConnectionApiCallHandler vlmConnApiCallHandlerRef,
        CtrlNetIfApiCallHandler netIfApiCallHandlerRef,
        CtrlWatchApiCallHandler watchApiCallHandlerRef,
        CtrlSnapshotApiCallHandler snapshotApiCallHandlerRef,
        CtrlSnapshotRestoreApiCallHandler snapshotRestoreApiCallHandlerRef,
        CtrlSnapshotRestoreVlmDfnApiCallHandler snapshotRestoreVlmDfnApiCallHandlerRef,
        CtrlDrbdProxyModifyApiCallHandler drbdProxyModifyApiCallHandlerRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef,
        @Named(ControllerCoreModule.CTRL_CONF_LOCK) ReadWriteLock ctrlConfigLockRef
    )
    {
        ctrlConfApiCallHandler = ctrlConfApiCallHandlerRef;
        nodeApiCallHandler = nodeApiCallHandlerRef;
        rscDfnApiCallHandler = rscDfnApiCallHandlerRef;
        vlmDfnApiCallHandler = vlmDfnApiCallHandlerRef;
        rscApiCallHandler = rscApiCallHandlerRef;
        storPoolDfnApiCallHandler = storPoolDfnApiCallHandlerRef;
        storPoolApiCallHandler = storPoolApiCallHandlerRef;
        nodeConnApiCallHandler = nodeConnApiCallHandlerRef;
        rscConnApiCallHandler = rscConnApiCallHandlerRef;
        vlmConnApiCallHandler = vlmConnApiCallHandlerRef;
        netIfApiCallHandler = netIfApiCallHandlerRef;
        watchApiCallHandler = watchApiCallHandlerRef;
        snapshotApiCallHandler = snapshotApiCallHandlerRef;
        snapshotRestoreApiCallHandler = snapshotRestoreApiCallHandlerRef;
        snapshotRestoreVlmDfnApiCallHandler = snapshotRestoreVlmDfnApiCallHandlerRef;
        drbdProxyModifyApiCallHandler = drbdProxyModifyApiCallHandlerRef;
        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        storPoolDfnMapLock = storPoolDfnMapLockRef;
        ctrlConfigLock = ctrlConfigLockRef;
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
        try (LockGuard ls = LockGuard.createLocked(nodesMapLock.writeLock()))
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
        try (LockGuard ls = LockGuard.createLocked(nodesMapLock.writeLock()))
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

    public ApiCallRc reconnectNode(
        List<String> nodes
    )
    {
        ApiCallRc apiCallRc;
        try (LockGuard ls = LockGuard.createLocked(nodesMapLock.writeLock()))
        {
            apiCallRc = nodeApiCallHandler.reconnectNode(
                nodes
            );
        }
        return apiCallRc;
    }

    public ArrayList<Node.NodeApi> listNode()
    {
        ArrayList<Node.NodeApi> listNodes;
        try (LockGuard ls = LockGuard.createLocked(nodesMapLock.readLock()))
        {
            listNodes = nodeApiCallHandler.listNodes();
        }
        return listNodes;
    }

    public ApiCallRc createSwordfishTargetNode(String nodeName, Map<String, String> props)
    {
        ApiCallRc apiCallRc;
        try (LockGuard lock = LockGuard.createLocked(nodesMapLock.writeLock()))
        {
            apiCallRc = nodeApiCallHandler.createSwordfishTargetNode(nodeName, props);
        }
        return apiCallRc;
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
        try (LockGuard ls = LockGuard.createLocked(rscDfnMapLock.writeLock()))
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
        try (LockGuard ls = LockGuard.createLocked(rscDfnMapLock.writeLock()))
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

    public ArrayList<ResourceDefinitionData.RscDfnApi> listResourceDefinition()
    {
        ArrayList<ResourceDefinitionData.RscDfnApi> listResourceDefinitions;
        try (LockGuard ls = LockGuard.createLocked(rscDfnMapLock.readLock()))
        {
            listResourceDefinitions = rscDfnApiCallHandler.listResourceDefinitions();
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
        try (LockGuard ls = LockGuard.createLocked(rscDfnMapLock.writeLock()))
        {
            apiCallRc = vlmDfnApiCallHandler.createVolumeDefinitions(
                rscName,
                vlmDfnApiList
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
            LockGuard ls = LockGuard.createLocked(
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

    public ResourceList listResource(List<String> filterNodes, List<String> filterResources)
    {
        ResourceList resourceList;
        try (
            LockGuard ls = LockGuard.createLocked(
                nodesMapLock.readLock(),
                rscDfnMapLock.readLock()
            )
        )
        {
            resourceList = rscApiCallHandler.listResources(
                filterNodes,
                filterResources
            );
        }
        return resourceList;
    }

    public List<ResourceConnection.RscConnApi> listResourceConnections(String rscName)
    {
        List<ResourceConnection.RscConnApi> listRscConns;
        try
        {
            nodesMapLock.readLock().lock();
            rscDfnMapLock.readLock().lock();
            listRscConns = rscApiCallHandler.listResourceConnections(rscName);
        }
        finally
        {
            rscDfnMapLock.readLock().unlock();
            nodesMapLock.readLock().unlock();
        }

        return listRscConns;
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
        try (LockGuard ls = LockGuard.createLocked(storPoolDfnMapLock.writeLock()))
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
        try (LockGuard ls = LockGuard.createLocked(storPoolDfnMapLock.writeLock()))
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
        try (LockGuard ls = LockGuard.createLocked(storPoolDfnMapLock.writeLock()))
        {
            apiCallRc = storPoolDfnApiCallHandler.deleteStorPoolDfn(
                storPoolName
            );
        }
        return apiCallRc;
    }

    public ArrayList<StorPoolDefinitionData.StorPoolDfnApi> listStorPoolDefinition()
    {
        ArrayList<StorPoolDefinitionData.StorPoolDfnApi> listStorPoolDefinitions;
        try (LockGuard ls = LockGuard.createLocked(storPoolDfnMapLock.readLock()))
        {
            listStorPoolDefinitions = storPoolDfnApiCallHandler.listStorPoolDefinitions();
        }
        return listStorPoolDefinitions;
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
            LockGuard ls = LockGuard.createLocked(
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
            LockGuard ls = LockGuard.createLocked(
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
        try (LockGuard ls = LockGuard.createLocked(nodesMapLock.writeLock()))
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
        try (LockGuard ls = LockGuard.createLocked(nodesMapLock.writeLock()))
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
        try (LockGuard ls = LockGuard.createLocked(nodesMapLock.writeLock()))
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
            LockGuard ls = LockGuard.createLocked(
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
            LockGuard ls = LockGuard.createLocked(
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
            LockGuard ls = LockGuard.createLocked(
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
            LockGuard ls = LockGuard.createLocked(
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
            LockGuard ls = LockGuard.createLocked(
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
            LockGuard ls = LockGuard.createLocked(
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

    public ApiCallRc setCtrlCfgProp(String key, String namespace, String value)
    {
        ApiCallRc apiCallRc;
        try (LockGuard ls = LockGuard.createLocked(ctrlConfigLock.writeLock()))
        {
            apiCallRc  = ctrlConfApiCallHandler.setProp(key, namespace, value);
        }
        return apiCallRc;
    }

    public Map<String, String> listCtrlCfg()
    {
        Map<String, String> data;
        try (LockGuard ls = LockGuard.createLocked(ctrlConfigLock.readLock()))
        {
            data  = ctrlConfApiCallHandler.listProps();
        }
        return data;
    }

    public ApiCallRc deleteCtrlCfgProp(String key, String namespace)
    {
        ApiCallRc apiCallRc;
        try (LockGuard ls = LockGuard.createLocked(ctrlConfigLock.writeLock()))
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
            LockGuard ls = LockGuard.createLocked(
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
            LockGuard ls = LockGuard.createLocked(
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
            LockGuard ls = LockGuard.createLocked(
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

    public ApiCallRc deleteWatch(int peerWatchId)
    {
        return watchApiCallHandler.deleteWatch(
            peerWatchId
        );
    }

    public ApiCallRc restoreVlmDfn(String fromRscName, String fromSnapshotName, String toRscName)
    {
        ApiCallRc apiCallRc;

        try (
            LockGuard ls = LockGuard.createLocked(
                rscDfnMapLock.writeLock()
            )
        )
        {
            apiCallRc = snapshotRestoreVlmDfnApiCallHandler.restoreVlmDfn(
                fromRscName,
                fromSnapshotName,
                toRscName
            );
        }
        return apiCallRc;
    }

    public ApiCallRc restoreSnapshot(
        List<String> nodeNames,
        String fromRscName,
        String fromSnapshotName,
        String toRscName
    )
    {
        ApiCallRc apiCallRc;

        try (
            LockGuard ls = LockGuard.createLocked(
                nodesMapLock.readLock(),
                rscDfnMapLock.writeLock()
            )
        )
        {
            apiCallRc = snapshotRestoreApiCallHandler.restoreSnapshot(
                nodeNames,
                fromRscName,
                fromSnapshotName,
                toRscName
            );
        }
        return apiCallRc;
    }

    public ArrayList<SnapshotDefinition.SnapshotDfnListItemApi> listSnapshotDefinition()
    {
        ArrayList<SnapshotDefinition.SnapshotDfnListItemApi> listSnapshotDefinitions;
        try (LockGuard ls = LockGuard.createLocked(rscDfnMapLock.readLock()))
        {
            listSnapshotDefinitions = snapshotApiCallHandler.listSnapshotDefinitions();
        }
        return listSnapshotDefinitions;
    }

    public ApiCallRc setMasterPassphrase(String newPassphrase, String oldPassphrase)
    {
        ApiCallRc apiCallRc;
        try (LockGuard ls = LockGuard.createLocked(ctrlConfigLock.writeLock()))
        {
            apiCallRc = ctrlConfApiCallHandler.setPassphrase(newPassphrase, oldPassphrase);
        }
        return apiCallRc;
    }

    public ApiCallRc enterPassphrase(String passphrase)
    {
        ApiCallRc apiCallRc;
        try (LockGuard ls = LockGuard.createLocked(ctrlConfigLock.writeLock()))
        {
            apiCallRc = ctrlConfApiCallHandler.enterPassphrase(passphrase);
        }
        return apiCallRc;
    }

    /**
     * Modifies the DRBD Proxy configuration for a given resource definition.
     *
     * @param rscDfnUuid optional - if given, modification is only performed if it matches the found
     *   rscDfn's UUID.
     * @param rscName required
     * @param overrideProps required (can be empty) - overrides the given property key-value pairs
     * @param deletePropKeys required (can be empty) - deletes the given property keys
     * @return
     */
    public ApiCallRc modifyDrbdProxy(
        UUID rscDfnUuid,
        String rscName,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        String compressionType,
        Map<String, String> compressionProps
    )
    {
        ApiCallRc apiCallRc;
        try (LockGuard ls = LockGuard.createLocked(rscDfnMapLock.writeLock()))
        {
            apiCallRc = drbdProxyModifyApiCallHandler.modifyDrbdProxy(
                rscDfnUuid,
                rscName,
                overrideProps,
                deletePropKeys,
                compressionType,
                compressionProps
            );
        }
        return apiCallRc;
    }
}
