package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.pojo.RscGrpPojo;
import com.linbit.linstor.core.apicallhandler.controller.helpers.ResourceList;
import com.linbit.linstor.core.apis.KvsApi;
import com.linbit.linstor.core.apis.NodeApi;
import com.linbit.linstor.core.apis.ResourceConnectionApi;
import com.linbit.linstor.core.apis.ResourceDefinitionApi;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinitionData;
import com.linbit.linstor.core.objects.VolumeDefinition.VlmDfnWtihCreationPayload;
import com.linbit.linstor.core.objects.VolumeGroup.VlmGrpApi;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;

import static com.linbit.locks.LockGuardFactory.LockObj.CTRL_CONFIG;
import static com.linbit.locks.LockGuardFactory.LockObj.KVS_MAP;
import static com.linbit.locks.LockGuardFactory.LockObj.NODES_MAP;
import static com.linbit.locks.LockGuardFactory.LockObj.RSC_DFN_MAP;
import static com.linbit.locks.LockGuardFactory.LockObj.RSC_GRP_MAP;
import static com.linbit.locks.LockGuardFactory.LockObj.STOR_POOL_DFN_MAP;
import static com.linbit.locks.LockGuardFactory.LockType.READ;
import static com.linbit.locks.LockGuardFactory.LockType.WRITE;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Singleton
public class CtrlApiCallHandler
{
    private final CtrlConfApiCallHandler ctrlConfApiCallHandler;
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
    private final CtrlWatchApiCallHandler watchApiCallHandler;
    private final CtrlSnapshotApiCallHandler snapshotApiCallHandler;
    private final CtrlSnapshotRestoreVlmDfnApiCallHandler snapshotRestoreVlmDfnApiCallHandler;
    private final CtrlDrbdProxyModifyApiCallHandler drbdProxyModifyApiCallHandler;
    private final CtrlKvsApiCallHandler kvsApiCallHandler;
    private final CtrlRscGrpApiCallHandler rscGrpApiCallHandler;
    private final CtrlVlmGrpApiCallHandler vlmGrpApiCallHandler;

    private final LockGuardFactory lockGuardFactory;

    @Inject
    CtrlApiCallHandler(
        CtrlConfApiCallHandler ctrlConfApiCallHandlerRef,
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
        CtrlWatchApiCallHandler watchApiCallHandlerRef,
        CtrlSnapshotApiCallHandler snapshotApiCallHandlerRef,
        CtrlSnapshotRestoreVlmDfnApiCallHandler snapshotRestoreVlmDfnApiCallHandlerRef,
        CtrlDrbdProxyModifyApiCallHandler drbdProxyModifyApiCallHandlerRef,
        CtrlKvsApiCallHandler kvsApiCallHandlerRef,
        CtrlRscGrpApiCallHandler rscGrpApiCallHandlerRef,
        CtrlVlmGrpApiCallHandler vlmGrpApiCallHandlerRef,
        LockGuardFactory lockGuardFactoryRef
    )
    {
        ctrlConfApiCallHandler = ctrlConfApiCallHandlerRef;
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
        watchApiCallHandler = watchApiCallHandlerRef;
        snapshotApiCallHandler = snapshotApiCallHandlerRef;
        snapshotRestoreVlmDfnApiCallHandler = snapshotRestoreVlmDfnApiCallHandlerRef;
        drbdProxyModifyApiCallHandler = drbdProxyModifyApiCallHandlerRef;
        kvsApiCallHandler = kvsApiCallHandlerRef;
        rscGrpApiCallHandler = rscGrpApiCallHandlerRef;
        vlmGrpApiCallHandler = vlmGrpApiCallHandlerRef;
        lockGuardFactory = lockGuardFactoryRef;
    }

    /**
     * Modifies a given node.
     *
     * @param nodeUuid
     *            optional - if given, modification is only performed if it matches the found
     *            node's UUID.
     * @param nodeName
     *            required
     * @param nodeType
     *            optional - if given, attempts to modify the type of the node
     * @param overrideProps
     *            required (can be empty) - overrides the given property key-value pairs
     * @param deletePropKeys
     *            required (can be empty) - deletes the given property keys
     * @param deleteNamespaces
     * @return
     */
    public ApiCallRc modifyNode(
        UUID nodeUuid,
        String nodeName,
        String nodeType,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        Set<String> deleteNamespaces
    )
    {
        ApiCallRc apiCallRc;
        try (LockGuard lg = lockGuardFactory.build(WRITE, NODES_MAP))
        {
            apiCallRc = nodeApiCallHandler.modifyNode(
                nodeUuid,
                nodeName,
                nodeType,
                overrideProps,
                deletePropKeys,
                deleteNamespaces
            );
        }
        return apiCallRc;
    }

    public ApiCallRc reconnectNode(
        List<String> nodes
    )
    {
        ApiCallRc apiCallRc;
        try (LockGuard lg = lockGuardFactory.build(WRITE, NODES_MAP))
        {
            apiCallRc = nodeApiCallHandler.reconnectNode(
                nodes
            );
        }
        return apiCallRc;
    }

    public ArrayList<NodeApi> listNode()
    {
        ArrayList<NodeApi> listNodes;
        try (LockGuard lg = lockGuardFactory.build(READ, NODES_MAP))
        {
            listNodes = nodeApiCallHandler.listNodes();
        }
        return listNodes;
    }

    public ApiCallRc createSwordfishTargetNode(String nodeName, Map<String, String> props)
    {
        ApiCallRc apiCallRc;
        try (LockGuard lg = lockGuardFactory.build(WRITE, NODES_MAP))
        {
            apiCallRc = nodeApiCallHandler.createSwordfishTargetNode(nodeName, props);
        }
        return apiCallRc;
    }

    /**
     * Creates new resource definition
     * @param rscGrpNameRef
     * @param shortRef
     */
    public ApiCallRc createResourceDefinition(
        String resourceName,
        byte[] extName,
        Integer port,
        String secretRef,
        String transportType,
        Map<String, String> propsRef,
        List<VlmDfnWtihCreationPayload> vlmDescrMapRef,
        List<String> layerStackRef,
        Short peerSlotsRef,
        String rscGrpNameRef
    )
    {
        ApiCallRc apiCallRc;
        String secret = secretRef;
        Map<String, String> props = propsRef;
        if (props == null)
        {
            props = Collections.emptyMap();
        }
        List<VlmDfnWtihCreationPayload> vlmDescrMap = vlmDescrMapRef;
        if (vlmDescrMap == null)
        {
            vlmDescrMap = Collections.emptyList();
        }
        try (LockGuard lg = lockGuardFactory.build(WRITE, RSC_DFN_MAP))
        {
            apiCallRc = rscDfnApiCallHandler.createResourceDefinition(
                resourceName,
                extName,
                port,
                secret,
                transportType,
                props,
                vlmDescrMapRef,
                layerStackRef,
                peerSlotsRef,
                rscGrpNameRef,
                false
            );
        }
        return apiCallRc;
    }

    /**
     * Modifies a given resource definition.
     *
     * @param rscDfnUuid
     *            optional - if given, modification is only performed if it matches the found
     *            rscDfn's UUID.
     * @param rscName
     *            required
     * @param port
     *            optional - if given, attempts to override the old port
     * @param overrideProps
     *            required (can be empty) - overrides the given property key-value pairs
     * @param deletePropKeys
     *            required (can be empty) - deletes the given property keys
     * @param layerStackStrList
     * @param newRscPeerSlotsRef
     */
    public ApiCallRc modifyRscDfn(
        UUID rscDfnUuid,
        String rscName,
        Integer port,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        Set<String> deletePropNamespaces,
        List<String> layerStackStrList,
        Short newRscPeerSlotsRef
    )
    {
        ApiCallRc apiCallRc;
        try (LockGuard lg = lockGuardFactory.build(WRITE, RSC_DFN_MAP))
        {
            apiCallRc = rscDfnApiCallHandler.modifyRscDfn(
                rscDfnUuid,
                rscName,
                port,
                overrideProps,
                deletePropKeys,
                deletePropNamespaces,
                layerStackStrList,
                newRscPeerSlotsRef
            );
        }
        return apiCallRc;
    }

    public ArrayList<ResourceDefinitionApi> listResourceDefinition()
    {
        ArrayList<ResourceDefinitionApi> listResourceDefinitions;
        try (LockGuard lg = lockGuardFactory.build(READ, RSC_DFN_MAP))
        {
            listResourceDefinitions = rscDfnApiCallHandler.listResourceDefinitions();
        }
        return listResourceDefinitions;
    }

    /**
     * Creates new volume definitions for a given resource definition.
     *
     * @param rscName
     *            required
     */
    public ApiCallRc createVlmDfns(
        String rscName,
        List<VlmDfnWtihCreationPayload> vlmDfnWithCrtPayloadApiListRef
    )
    {
        ApiCallRc apiCallRc;
        List<VlmDfnWtihCreationPayload> vlmDfnWithPayloadApiList = vlmDfnWithCrtPayloadApiListRef;
        if (vlmDfnWithPayloadApiList == null)
        {
            vlmDfnWithPayloadApiList = Collections.emptyList();
        }
        try (LockGuard lg = lockGuardFactory.build(WRITE, RSC_DFN_MAP))
        {
            apiCallRc = vlmDfnApiCallHandler.createVolumeDefinitions(
                rscName,
                vlmDfnWithPayloadApiList
            );
        }
        return apiCallRc;
    }

    /**
     * Modifies an existing resource
     *
     * @param rscUuid
     *            optional, if given checked against persisted UUID
     * @param nodeName
     *            required
     * @param rscName
     *            required
     */
    public ApiCallRc modifyRsc(
        UUID rscUuid,
        String nodeName,
        String rscName,
        Map<String, String> overridePropsRef,
        Set<String> deletePropKeysRef,
        Set<String> deletePropNamespaces
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
            LockGuard lg = lockGuardFactory.build(WRITE, NODES_MAP, RSC_DFN_MAP)
        )
        {
            apiCallRc = rscApiCallHandler.modifyResource(
                rscUuid,
                nodeName,
                rscName,
                overrideProps,
                deletePropKeys,
                deletePropNamespaces
            );
        }
        return apiCallRc;
    }

    public ResourceList listResource(String rscNameStr, List<String> filterNodes)
    {
        ResourceList resourceList;
        try (
            LockGuard lg = lockGuardFactory.build(READ, NODES_MAP, RSC_DFN_MAP)
        )
        {
            resourceList = rscApiCallHandler.listResources(
                rscNameStr,
                filterNodes
            );
        }
        return resourceList;
    }

    public ResourceList listResource(List<String> filterNodes, List<String> filterResources)
    {
        ResourceList resourceList;
        try (
            LockGuard lg = lockGuardFactory.build(READ, NODES_MAP, RSC_DFN_MAP)
        )
        {
            resourceList = rscApiCallHandler.listResources(
                filterNodes,
                filterResources
            );
        }
        return resourceList;
    }

    public List<ResourceConnectionApi> listResourceConnections(String rscName)
    {
        List<ResourceConnectionApi> listRscConns;
        try (LockGuard lg = lockGuardFactory.build(READ, NODES_MAP, RSC_DFN_MAP))
        {
            listRscConns = rscApiCallHandler.listResourceConnections(rscName);
        }
        return listRscConns;
    }

    /**
     * Creates a new storPoolDefinition.
     *
     * @param storPoolName
     *            required
     * @param storPoolDfnPropsMap
     *            optional
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
        try (LockGuard lg = lockGuardFactory.build(WRITE, STOR_POOL_DFN_MAP))
        {
            apiCallRc = storPoolDfnApiCallHandler.createStorPoolDfn(
                storPoolName,
                storPoolDfnProps
            );
        }
        return apiCallRc;
    }

    /**
     * Modifies an existing storPoolDefinition
     *
     * @param storPoolDfnUuid
     *            optional, if given checked against persisted UUID
     * @param storPoolName
     *            required
     * @param overridePropsRef
     *            optional
     * @param deletePropKeysRef
     *            optional
     */
    public ApiCallRc modifyStorPoolDfn(
        UUID storPoolDfnUuid,
        String storPoolName,
        Map<String, String> overridePropsRef,
        Set<String> deletePropKeysRef,
        Set<String> deletePropNamespaces
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
        try (LockGuard lg = lockGuardFactory.build(WRITE, STOR_POOL_DFN_MAP))
        {
            apiCallRc = storPoolDfnApiCallHandler.modifyStorPoolDfn(
                storPoolDfnUuid,
                storPoolName,
                overrideProps,
                deletePropKeys,
                deletePropNamespaces
            );
        }
        return apiCallRc;
    }

    /**
     * Marks a storPoolDefinition for deletion.
     *
     * The storPoolDefinition is only deleted once all corresponding satellites
     * confirmed that they have undeployed (deleted) the {@link StorPool}.
     *
     * @param storPoolName
     *            required
     */
    public ApiCallRc deleteStoragePoolDefinition(String storPoolName)
    {
        ApiCallRc apiCallRc;
        try (LockGuard lg = lockGuardFactory.build(WRITE, STOR_POOL_DFN_MAP))
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
        try (LockGuard lg = lockGuardFactory.build(READ, STOR_POOL_DFN_MAP))
        {
            listStorPoolDefinitions = storPoolDfnApiCallHandler.listStorPoolDefinitions();
        }
        return listStorPoolDefinitions;
    }

    /**
     * Modifies an existing {@link StorPool}
     *
     * @param storPoolUuid
     *            optional, if given checked against persisted UUID
     * @param nodeName
     *            required
     * @param storPoolName
     *            required
     * @param overridePropsRef
     *            optional
     * @param deletePropKeysRef
     *            optional
     */
    public ApiCallRc modifyStorPool(
        UUID storPoolUuid,
        String nodeName,
        String storPoolName,
        Map<String, String> overridePropsRef,
        Set<String> deletePropKeysRef,
        Set<String> deletePropNamespaces
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
        try (LockGuard lg = lockGuardFactory.build(WRITE, NODES_MAP, STOR_POOL_DFN_MAP))
        {
            apiCallRc = storPoolApiCallHandler.modifyStorPool(
                storPoolUuid,
                nodeName,
                storPoolName,
                overrideProps,
                deletePropKeys,
                deletePropNamespaces
            );
        }
        return apiCallRc;
    }

    /**
     * Marks the {@link StorPool} for deletion.
     *
     * The {@link StorPool} is only deleted once the corresponding satellite
     * confirms that it has undeployed (deleted) the {@link StorPool}.
     */
    public ApiCallRc deleteStoragePool(
        String nodeName,
        String storPoolName
    )
    {
        ApiCallRc apiCallRc;
        try (LockGuard lg = lockGuardFactory.build(WRITE, NODES_MAP, STOR_POOL_DFN_MAP))
        {
            apiCallRc = storPoolApiCallHandler.deleteStorPool(
                nodeName,
                storPoolName
            );
        }

        return apiCallRc;
    }

    /**
     * Creates a new nodeConnection.
     *
     * @param nodeName1
     *            required
     * @param nodeName2
     *            required
     * @param nodeConnPropsMap
     *            optional, recommended
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
        try (LockGuard lg = lockGuardFactory.build(WRITE, NODES_MAP))
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
     * Modifies an existing nodeConnection
     *
     * @param nodeConnUuid
     *            optional, if given checks against persisted uuid
     * @param nodeName1
     *            required
     * @param nodeName2
     *            required
     * @param overridePropsRef
     *            optional, can be empty
     * @param deletePropKeysRef
     *            optional, can be empty
     */
    public ApiCallRc modifyNodeConn(
        UUID nodeConnUuid,
        String nodeName1,
        String nodeName2,
        Map<String, String> overridePropsRef,
        Set<String> deletePropKeysRef,
        Set<String> deletePropNamespaces
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
        try (LockGuard lg = lockGuardFactory.build(WRITE, NODES_MAP))
        {
            apiCallRc = nodeConnApiCallHandler.modifyNodeConnection(
                nodeConnUuid,
                nodeName1,
                nodeName2,
                overrideProps,
                deletePropKeys,
                deletePropNamespaces
            );
        }
        return apiCallRc;
    }

    /**
     * Deletes the nodeConnection.
     */
    public ApiCallRc deleteNodeConnection(
        String nodeName1,
        String nodeName2
    )
    {
        ApiCallRc apiCallRc;
        try (LockGuard lg = lockGuardFactory.build(WRITE, NODES_MAP))
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
     * @param nodeName1
     *            required
     * @param nodeName2
     *            required
     * @param rscName
     *            required
     * @param rscConnPropsMap
     *            optional, recommended
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
        try (LockGuard lg = lockGuardFactory.build(WRITE, NODES_MAP, RSC_DFN_MAP))
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
     *
     * @param rscConnUuid
     *            optional, if given checked against persisted UUID
     * @param nodeName1
     *            required
     * @param nodeName2
     *            required
     * @param rscName
     *            required
     * @param overridePropsRef
     *            optional
     * @param deletePropKeysRef
     *            optional
     */
    public ApiCallRc modifyRscConn(
        UUID rscConnUuid,
        String nodeName1,
        String nodeName2,
        String rscName,
        Map<String, String> overridePropsRef,
        Set<String> deletePropKeysRef,
        Set<String> deletePropNamespaces
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
        try (LockGuard lg = lockGuardFactory.build(WRITE, NODES_MAP, RSC_DFN_MAP))
        {
            apiCallRc = rscConnApiCallHandler.modifyRscConnection(
                rscConnUuid,
                nodeName1,
                nodeName2,
                rscName,
                overrideProps,
                deletePropKeys,
                deletePropNamespaces
            );
        }
        return apiCallRc;
    }

    /**
     * Deletes a {@link ResourceConnection}.
     *
     * @param nodeName1
     *            required
     * @param nodeName2
     *            required
     * @param rscName
     *            required
     */
    public ApiCallRc deleteResourceConnection(
        String nodeName1,
        String nodeName2,
        String rscName
    )
    {
        ApiCallRc apiCallRc;
        try (LockGuard lg = lockGuardFactory.build(WRITE, NODES_MAP, RSC_DFN_MAP))
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
     * Creates a new volumeConnection.
     *
     * @param nodeName1
     *            required
     * @param nodeName2
     *            required
     * @param rscName
     *            required
     * @param vlmNr
     *            required
     * @param vlmConnPropsMap
     *            optional, recommended
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
        try (LockGuard lg = lockGuardFactory.build(WRITE, NODES_MAP, RSC_DFN_MAP))
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
     * Modifies an existing volumeConnection
     *
     * @param vlmConnUuid
     *            optional, if given checked against persisted UUID
     * @param nodeName1
     *            required
     * @param nodeName2
     *            required
     * @param rscName
     *            required
     * @param vlmNr
     *            required
     * @param overridePropsRef
     *            optional
     * @param deletePropKeysRef
     *            optional
     */
    public ApiCallRc modifyVlmConn(
        UUID vlmConnUuid,
        String nodeName1,
        String nodeName2,
        String rscName,
        int vlmNr,
        Map<String, String> overridePropsRef,
        Set<String> deletePropKeysRef,
        Set<String> deletePropNamespaces
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
        try (LockGuard lg = lockGuardFactory.build(WRITE, NODES_MAP, RSC_DFN_MAP))
        {
            apiCallRc = vlmConnApiCallHandler.modifyVolumeConnection(
                vlmConnUuid,
                nodeName1,
                nodeName2,
                rscName,
                vlmNr,
                overrideProps,
                deletePropKeys,
                deletePropNamespaces
            );
        }
        return apiCallRc;
    }

    /**
     * Deletes a volumeConnection.
     *
     * @param nodeName1
     *            required
     * @param nodeName2
     *            required
     * @param rscName
     *            required
     * @param vlmNr
     *            required
     */
    public ApiCallRc deleteVolumeConnection(
        String nodeName1,
        String nodeName2,
        String rscName,
        int vlmNr
    )
    {
        ApiCallRc apiCallRc;
        try (LockGuard lg = lockGuardFactory.build(WRITE, NODES_MAP, RSC_DFN_MAP))
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

    public ApiCallRc modifyCtrl(
        Map<String, String> overridePropsRef,
        Set<String> deletePropKeysRef,
        Set<String> deletePropNamespaces
    )
    {
        ApiCallRc apiCallRc;
        try (LockGuard lg = lockGuardFactory.build(WRITE, CTRL_CONFIG))
        {
            apiCallRc = ctrlConfApiCallHandler.modifyCtrl(
                overridePropsRef,
                deletePropKeysRef,
                deletePropNamespaces
            );
        }
        return apiCallRc;
    }

    public Map<String, String> listCtrlCfg()
    {
        Map<String, String> data;
        try (LockGuard lg = lockGuardFactory.build(READ, CTRL_CONFIG))
        {
            data = ctrlConfApiCallHandler.listProps();
        }
        return data;
    }

    public ApiCallRc deleteCtrlCfgProp(String key, String namespace)
    {
        ApiCallRc apiCallRc;
        try (LockGuard lg = lockGuardFactory.build(WRITE, CTRL_CONFIG))
        {
            apiCallRc = ctrlConfApiCallHandler.deleteProp(key, namespace);
        }
        return apiCallRc;
    }

    public ApiCallRc createNetInterface(
        String nodeName,
        String netIfName,
        String address,
        Integer stltPort,
        String stltEncrType,
        Boolean setActive
    )
    {
        ApiCallRc apiCallRc;

        try (LockGuard lg = lockGuardFactory
            .create()
            .read(CTRL_CONFIG)
            .write(NODES_MAP)
            .build()
        )
        {
            apiCallRc = netIfApiCallHandler.createNetIf(
                nodeName,
                netIfName,
                address,
                stltPort,
                stltEncrType,
                setActive
            );
        }
        return apiCallRc;
    }

    public ApiCallRc modifyNetInterface(
        String nodeName,
        String netIfName,
        String address,
        Integer stltPort,
        String stltEncrType,
        Boolean setActive
    )
    {
        ApiCallRc apiCallRc;

        try (LockGuard lg = lockGuardFactory
            .create()
            .read(CTRL_CONFIG)
            .write(NODES_MAP)
            .build()
        )
        {
            apiCallRc = netIfApiCallHandler.modifyNetIf(
                nodeName,
                netIfName,
                address,
                stltPort,
                stltEncrType,
                setActive
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

        try (LockGuard lg = lockGuardFactory
            .create()
            .read(CTRL_CONFIG)
            .write(NODES_MAP)
            .build()
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

        try (LockGuard lg = lockGuardFactory.build(WRITE, RSC_DFN_MAP))
        {
            apiCallRc = snapshotRestoreVlmDfnApiCallHandler.restoreVlmDfn(
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
        try (LockGuard lg = lockGuardFactory.build(READ, RSC_DFN_MAP))
        {
            listSnapshotDefinitions = snapshotApiCallHandler.listSnapshotDefinitions();
        }
        return listSnapshotDefinitions;
    }

    public ApiCallRc setMasterPassphrase(String newPassphrase, String oldPassphrase)
    {
        ApiCallRc apiCallRc;
        try (LockGuard lg = lockGuardFactory.build(WRITE, CTRL_CONFIG))
        {
            apiCallRc = ctrlConfApiCallHandler.setPassphrase(newPassphrase, oldPassphrase);
        }
        return apiCallRc;
    }

    public ApiCallRc enterPassphrase(String passphrase)
    {
        ApiCallRc apiCallRc;
        try (LockGuard lg = lockGuardFactory.build(WRITE, CTRL_CONFIG))
        {
            apiCallRc = ctrlConfApiCallHandler.enterPassphrase(passphrase);
        }
        return apiCallRc;
    }

    /**
     * Modifies the DRBD Proxy configuration for a given resource definition.
     *
     * @param rscDfnUuid
     *            optional - if given, modification is only performed if it matches the found
     *            rscDfn's UUID.
     * @param rscName
     *            required
     * @param overrideProps
     *            required (can be empty) - overrides the given property key-value pairs
     * @param deletePropKeys
     *            required (can be empty) - deletes the given property keys
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
        try (LockGuard lg = lockGuardFactory.build(WRITE, RSC_DFN_MAP))
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

    public Set<KvsApi> listKvs()
    {
        Set<KvsApi> kvs;
        try (LockGuard lg = lockGuardFactory.build(READ, KVS_MAP))
        {
            kvs = kvsApiCallHandler.listKvs();
        }
        return kvs;
    }

    public ApiCallRc modifyKvs(
        UUID kvsUuid,
        String kvsName,
        Map<String, String> modProps,
        Set<String> deletePropKeys,
        Set<String> deleteNamespaces
    )
    {
        ApiCallRc apiCallRc;
        try (LockGuard lg = lockGuardFactory.build(WRITE, KVS_MAP))
        {
            apiCallRc = kvsApiCallHandler.modifyKvs(
                kvsUuid,
                kvsName,
                modProps,
                deletePropKeys,
                deleteNamespaces
            );
        }
        return apiCallRc;
    }

    public ApiCallRc deleteKvs(UUID kvsUuid, String kvsName)
    {
        ApiCallRc apiCallRc;
        try (LockGuard lg = lockGuardFactory.build(WRITE, KVS_MAP))
        {
            apiCallRc = kvsApiCallHandler.deleteKvs(kvsUuid, kvsName);
        }
        return apiCallRc;
    }

    public ApiCallRc modifyVlm(
        UUID uuidRef,
        String nodeNameRef,
        String rscNameRef,
        Integer vlmNrRef,
        Map<String, String> overridePropsRef,
        Set<String> deletePropKeys,
        Set<String> deleteNamespacesRef
    )
    {
        ApiCallRc apiCallRc;
        try (LockGuard lg = lockGuardFactory.build(WRITE, NODES_MAP, RSC_DFN_MAP))
        {
            apiCallRc = vlmApiCallHandler.modifyVolume(
                uuidRef,
                nodeNameRef,
                rscNameRef,
                vlmNrRef,
                overridePropsRef,
                deletePropKeys,
                deleteNamespacesRef
            );
        }
        return apiCallRc;
    }

    public ArrayList<ResourceGroup.RscGrpApi> listResourceGroups()
    {
        ArrayList<ResourceGroup.RscGrpApi> listResourceGroups;
        try (LockGuard lg = lockGuardFactory.build(READ, RSC_GRP_MAP))
        {
            listResourceGroups = rscGrpApiCallHandler.listResourceGroups();
        }
        return listResourceGroups;
    }


    public ApiCallRc createResourceGroup(RscGrpPojo rscGrpPojoRef)
    {
        ApiCallRc apiCallRc;
        try (LockGuard lg = lockGuardFactory.build(WRITE, RSC_GRP_MAP))
        {
            apiCallRc = rscGrpApiCallHandler.create(rscGrpPojoRef);
        }
        return apiCallRc;
    }

    public ApiCallRc deleteResourceGroup(String rscGrpNameStrRef)
    {
        ApiCallRc apiCallRc;
        try (LockGuard lg = lockGuardFactory.build(WRITE, RSC_GRP_MAP))
        {
            apiCallRc = rscGrpApiCallHandler.delete(rscGrpNameStrRef);
        }
        return apiCallRc;
    }

    public ApiCallRc createVlmGrps(String rscGrpNameRef, List<VlmGrpApi> vlmGrpApiListRef)
    {
        ApiCallRc apiCallRc;
        try (LockGuard lg = lockGuardFactory.build(WRITE, RSC_GRP_MAP))
        {
            apiCallRc = vlmGrpApiCallHandler.createVlmGrps(
                rscGrpNameRef,
                vlmGrpApiListRef
            );
        }
        return apiCallRc;
    }

    public List<VlmGrpApi> listVolumeGroups(String rscNameRef, Integer vlmNrRef)
    {
        List<VlmGrpApi> listVolumeGroups;
        try (LockGuard lg = lockGuardFactory.build(READ, RSC_GRP_MAP))
        {
            listVolumeGroups = vlmGrpApiCallHandler.listVolumeGroups(rscNameRef, vlmNrRef);
        }
        return listVolumeGroups;
    }

    public ApiCallRc modifyVolumeGroup(
        String rscGrpNameRef,
        int vlmNrRef,
        Map<String, String> overrideProps,
        HashSet<String> deletePropKeys,
        HashSet<String> deleteNamespaces
    )
    {
        ApiCallRc apiCallRc;
        try (LockGuard lg = lockGuardFactory.build(WRITE, RSC_GRP_MAP))
        {
            apiCallRc = vlmGrpApiCallHandler.modify(
                rscGrpNameRef,
                vlmNrRef,
                overrideProps,
                deletePropKeys,
                deleteNamespaces
            );
        }
        return apiCallRc;
    }

    public ApiCallRc deleteVolumeGroup(
        String rscGrpNameRef,
        int vlmNrRef
    )
    {
        ApiCallRc apiCallRc;
        try (LockGuard lg = lockGuardFactory.build(WRITE, RSC_GRP_MAP))
        {
            apiCallRc = vlmGrpApiCallHandler.delete(rscGrpNameRef, vlmNrRef);
        }
        return apiCallRc;
    }
}
