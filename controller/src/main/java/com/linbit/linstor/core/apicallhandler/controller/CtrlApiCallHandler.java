package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.pojo.DrbdRscPojo;
import com.linbit.linstor.api.pojo.RscGrpPojo;
import com.linbit.linstor.core.apicallhandler.controller.helpers.ResourceList;
import com.linbit.linstor.core.apicallhandler.controller.utils.ResourceDefinitionUtils;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apis.ControllerConfigApi;
import com.linbit.linstor.core.apis.KvsApi;
import com.linbit.linstor.core.apis.NodeApi;
import com.linbit.linstor.core.apis.ResourceApi;
import com.linbit.linstor.core.apis.ResourceConnectionApi;
import com.linbit.linstor.core.apis.ResourceDefinitionApi;
import com.linbit.linstor.core.apis.ResourceGroupApi;
import com.linbit.linstor.core.apis.SnapshotDefinitionListItemApi;
import com.linbit.linstor.core.apis.SnapshotShippingListItemApi;
import com.linbit.linstor.core.apis.StorPoolDefinitionApi;
import com.linbit.linstor.core.apis.VolumeDefinitionWithCreationPayload;
import com.linbit.linstor.core.apis.VolumeGroupApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.satellitestate.SatelliteResourceState;
import com.linbit.linstor.satellitestate.SatelliteState;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.tasks.AutoSnapshotTask;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;

import static com.linbit.linstor.core.objects.ResourceDefinition.Flags.CLONING;
import static com.linbit.linstor.core.objects.ResourceDefinition.Flags.FAILED;
import static com.linbit.locks.LockGuardFactory.LockObj.CTRL_CONFIG;
import static com.linbit.locks.LockGuardFactory.LockObj.KVS_MAP;
import static com.linbit.locks.LockGuardFactory.LockObj.NODES_MAP;
import static com.linbit.locks.LockGuardFactory.LockObj.RSC_DFN_MAP;
import static com.linbit.locks.LockGuardFactory.LockObj.RSC_GRP_MAP;
import static com.linbit.locks.LockGuardFactory.LockObj.STOR_POOL_DFN_MAP;
import static com.linbit.locks.LockGuardFactory.LockType.READ;
import static com.linbit.locks.LockGuardFactory.LockType.WRITE;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import reactor.core.publisher.Flux;

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
    private final CtrlSnapshotShippingApiCallHandler snapshotShippingApiCallHandler;
    private final CtrlSnapshotRestoreVlmDfnApiCallHandler snapshotRestoreVlmDfnApiCallHandler;
    private final CtrlDrbdProxyModifyApiCallHandler drbdProxyModifyApiCallHandler;
    private final CtrlKvsApiCallHandler kvsApiCallHandler;
    private final CtrlRscGrpApiCallHandler rscGrpApiCallHandler;
    private final CtrlVlmGrpApiCallHandler vlmGrpApiCallHandler;
    private final DbEngine dbEngine;

    private final LockGuardFactory lockGuardFactory;
    private final AutoSnapshotTask autoSnapshotTask;
    private final CtrlSnapshotDeleteApiCallHandler ctrlSnapDeleteHandler;
    private final Provider<AccessContext> peerAccCtx;
    private final SystemConfRepository systemConfRepository;

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
        CtrlSnapshotShippingApiCallHandler snapshotShippingApiCallHandlerRef,
        CtrlSnapshotRestoreVlmDfnApiCallHandler snapshotRestoreVlmDfnApiCallHandlerRef,
        CtrlDrbdProxyModifyApiCallHandler drbdProxyModifyApiCallHandlerRef,
        CtrlKvsApiCallHandler kvsApiCallHandlerRef,
        CtrlRscGrpApiCallHandler rscGrpApiCallHandlerRef,
        CtrlVlmGrpApiCallHandler vlmGrpApiCallHandlerRef,
        AutoSnapshotTask autoSnapshotTaskRef,
        CtrlSnapshotDeleteApiCallHandler ctrlSnapDeleteHandlerRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        SystemConfRepository systemConfRepositoryRef,
        DbEngine dbEngineRef,
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
        snapshotShippingApiCallHandler = snapshotShippingApiCallHandlerRef;
        snapshotRestoreVlmDfnApiCallHandler = snapshotRestoreVlmDfnApiCallHandlerRef;
        drbdProxyModifyApiCallHandler = drbdProxyModifyApiCallHandlerRef;
        kvsApiCallHandler = kvsApiCallHandlerRef;
        rscGrpApiCallHandler = rscGrpApiCallHandlerRef;
        vlmGrpApiCallHandler = vlmGrpApiCallHandlerRef;
        autoSnapshotTask = autoSnapshotTaskRef;
        ctrlSnapDeleteHandler = ctrlSnapDeleteHandlerRef;
        peerAccCtx = peerAccCtxRef;
        systemConfRepository = systemConfRepositoryRef;
        dbEngine = dbEngineRef;
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
    public Flux<ApiCallRc> modifyNode(
        UUID nodeUuid,
        String nodeName,
        String nodeType,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        Set<String> deleteNamespaces
    )
    {
        Flux<ApiCallRc> flux;
        try (LockGuard lg = lockGuardFactory.build(WRITE, NODES_MAP))
        {
            flux = nodeApiCallHandler.modify(
                nodeUuid,
                nodeName,
                nodeType,
                overrideProps,
                deletePropKeys,
                deleteNamespaces
            );
        }
        return flux;
    }

    public ArrayList<NodeApi> listNodes(List<String> nodeNames, List<String> propFilters)
    {
        ArrayList<NodeApi> nodeList;
        try (LockGuard lg = lockGuardFactory.build(READ, NODES_MAP))
        {
            nodeList = nodeApiCallHandler.listNodes(nodeNames, propFilters);
        }
        return nodeList;
    }

    /**
     * Creates new resource definition
     * @param rscGrpNameRef
     * @param shortRef
     */
    public ApiCallRc createResourceDefinition(
        String resourceName,
        byte[] extName,
        Map<String, String> propsRef,
        List<VolumeDefinitionWithCreationPayload> vlmDescrMapRef,
        List<String> layerStackRef,
        LayerPayload payloadRef,
        String rscGrpNameRef
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        Map<String, String> props = propsRef;
        if (props == null)
        {
            props = Collections.emptyMap();
        }
        try (LockGuard lg = lockGuardFactory.build(WRITE, RSC_DFN_MAP))
        {
            ResourceDefinition rscDfn = rscDfnApiCallHandler.createResourceDefinition(
                resourceName,
                extName,
                props,
                vlmDescrMapRef,
                layerStackRef,
                payloadRef,
                rscGrpNameRef,
                false,
                apiCallRc,
                true
            );

            // if the previous call errored out (i.e. with an InvalidNameException, which gets wrapped into an
            // ApiRcException which will not be thrown due to "throwOnError = false") rscDfn might be null
            if (rscDfn != null)
            {
                // we can ignore the flux for now, since we are not creating resources that would need the flux executed
                ResourceDefinitionUtils.handleAutoSnapProps(
                    autoSnapshotTask,
                    ctrlSnapDeleteHandler,
                    Collections.emptyMap(),
                    Collections.emptySet(),
                    Collections.emptySet(),
                    Collections.singletonList(rscDfn),
                    peerAccCtx.get(),
                    systemConfRepository.getStltConfForView(peerAccCtx.get()),
                    true
                );
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "create resource definition",
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
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
    public Flux<ApiCallRc> modifyRscDfn(
        UUID rscDfnUuid,
        String rscName,
        Integer port,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        Set<String> deletePropNamespaces,
        List<String> layerStackStrList,
        Short newRscPeerSlotsRef,
        @Nullable String rscGroupName
    )
    {
        Flux<ApiCallRc> flux;
        try (LockGuard lg = lockGuardFactory.build(WRITE, RSC_DFN_MAP))
        {
            flux = rscDfnApiCallHandler.modify(
                rscDfnUuid,
                rscName,
                port,
                overrideProps,
                deletePropKeys,
                deletePropNamespaces,
                layerStackStrList,
                newRscPeerSlotsRef,
                rscGroupName
            );
        }
        return flux;
    }

    public ArrayList<ResourceDefinitionApi> listResourceDefinitions()
    {
        return listResourceDefinitions(Collections.emptyList(), Collections.emptyList());
    }

    public ArrayList<ResourceDefinitionApi> listResourceDefinitions(
            List<String> filterRscDfnNames, List<String> propFilters)
    {
        ArrayList<ResourceDefinitionApi> resourceDefinitionList;
        try (LockGuard lg = lockGuardFactory.build(READ, RSC_DFN_MAP))
        {
            resourceDefinitionList = rscDfnApiCallHandler.listResourceDefinitions(filterRscDfnNames, propFilters);
        }
        return resourceDefinitionList;
    }

    /**
     * Creates new volume definitions for a given resource definition.
     *
     * @param rscName
     *            required
     */
    public Flux<ApiCallRc> createVlmDfns(
        String rscName,
        List<VolumeDefinitionWithCreationPayload> vlmDfnWithCrtPayloadApiList
    )
    {
        Flux<ApiCallRc> flux;
        List<VolumeDefinitionWithCreationPayload> vlmDfnWithPayloadApiList = vlmDfnWithCrtPayloadApiList;
        if (vlmDfnWithPayloadApiList == null)
        {
            vlmDfnWithPayloadApiList = Collections.emptyList();
        }
        try (LockGuard lg = lockGuardFactory.build(WRITE, RSC_DFN_MAP))
        {
            flux = vlmDfnApiCallHandler.createVolumeDefinitions(rscName, vlmDfnWithPayloadApiList);
        }
        return flux;
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
    public Flux<ApiCallRc> modifyRsc(
        UUID rscUuid,
        String nodeName,
        String rscName,
        Map<String, String> overridePropsRef,
        Set<String> deletePropKeysRef,
        Set<String> deletePropNamespaces
    )
    {
        Flux<ApiCallRc> apiCallRc;
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
            apiCallRc = rscApiCallHandler.modify(
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
    public Flux<ApiCallRc> modifyStorPoolDfn(
        UUID storPoolDfnUuid,
        String storPoolName,
        Map<String, String> overridePropsRef,
        Set<String> deletePropKeysRef,
        Set<String> deletePropNamespaces
    )
    {
        Flux<ApiCallRc> apiCallRc;

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
            apiCallRc = storPoolDfnApiCallHandler.modify(
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

    public ArrayList<StorPoolDefinitionApi> listStorPoolDefinition()
    {
        ArrayList<StorPoolDefinitionApi> listStorPoolDefinitions;
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
    public Flux<ApiCallRc> modifyStorPool(
        UUID storPoolUuid,
        String nodeName,
        String storPoolName,
        Map<String, String> overridePropsRef,
        Set<String> deletePropKeysRef,
        Set<String> deletePropNamespaces
    )
    {
        Flux<ApiCallRc> flux;

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
            flux = storPoolApiCallHandler.modify(
                storPoolUuid,
                nodeName,
                storPoolName,
                overrideProps,
                deletePropKeys,
                deletePropNamespaces
            );
        }
        return flux;
    }

    /**
     * Marks the {@link StorPool} for deletion.
     *
     * The {@link StorPool} is only deleted once the corresponding satellite
     * confirms that it has undeployed (deleted) the {@link StorPool}.
     */
    public Flux<ApiCallRc> deleteStoragePool(
        String nodeName,
        String storPoolName
    )
    {
        Flux<ApiCallRc> flux;
        try (LockGuard lg = lockGuardFactory.build(WRITE, NODES_MAP, STOR_POOL_DFN_MAP))
        {
            flux = storPoolApiCallHandler.deleteStorPool(
                nodeName,
                storPoolName
            );
        }

        return flux;
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
    public Flux<ApiCallRc> createResourceConnection(
        String nodeName1,
        String nodeName2,
        String rscName,
        Map<String, String> rscConnPropsMap
    )
    {
        Flux<ApiCallRc> flux;
        Map<String, String> rscConnProps = rscConnPropsMap;
        if (rscConnProps == null)
        {
            rscConnProps = Collections.emptyMap();
        }
        try (LockGuard lg = lockGuardFactory.build(WRITE, NODES_MAP, RSC_DFN_MAP))
        {
            flux = rscConnApiCallHandler.createResourceConnection(
                nodeName1,
                nodeName2,
                rscName,
                rscConnProps
            );
        }
        return flux;
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
    public Flux<ApiCallRc> modifyRscConn(
        UUID rscConnUuid,
        String nodeName1,
        String nodeName2,
        String rscName,
        Map<String, String> overridePropsRef,
        Set<String> deletePropKeysRef,
        Set<String> deletePropNamespaces
    )
    {
        Flux<ApiCallRc> flux;

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
            flux = rscConnApiCallHandler.modify(
                rscConnUuid,
                nodeName1,
                nodeName2,
                rscName,
                overrideProps,
                deletePropKeys,
                deletePropNamespaces
            );
        }
        return flux;
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
    public Flux<ApiCallRc> deleteResourceConnection(
        String nodeName1,
        String nodeName2,
        String rscName
    )
    {
        Flux<ApiCallRc> flux;
        try (LockGuard lg = lockGuardFactory.build(WRITE, NODES_MAP, RSC_DFN_MAP))
        {
            flux = rscConnApiCallHandler.deleteResourceConnection(
                nodeName1,
                nodeName2,
                rscName
            );
        }
        return flux;
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

    public Flux<ApiCallRc> modifyCtrl(
        Map<String, String> overridePropsRef,
        Set<String> deletePropKeysRef,
        Set<String> deletePropNamespaces
    )
    {
        Flux<ApiCallRc> flux;
        try (LockGuard lg = lockGuardFactory.build(WRITE, CTRL_CONFIG))
        {
            flux = ctrlConfApiCallHandler.modifyCtrl(
                overridePropsRef,
                deletePropKeysRef,
                deletePropNamespaces
            );
        }
        return flux;
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

    public Flux<ApiCallRc> deleteCtrlCfgProp(String key, String namespace)
    {
        Flux<ApiCallRc> flux;
        try (LockGuard lg = lockGuardFactory.build(WRITE, CTRL_CONFIG))
        {
            flux = ctrlConfApiCallHandler.deletePropWithCommit(key, namespace);
        }
        return flux;
    }

    public Flux<ApiCallRc> setConfig(
        ControllerConfigApi config
    )
        throws AccessDeniedException
    {
        return ctrlConfApiCallHandler.setCtrlConfig(config);
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

    public ApiCallRc restoreVlmDfn(
        String fromRscName,
        String fromSnapshotName,
        String toRscName
    )
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

    public ArrayList<SnapshotDefinitionListItemApi> listSnapshotDefinition(
        List<String> nodeNames, List<String> resourceNames)
    {
        ArrayList<SnapshotDefinitionListItemApi> listSnapshotDefinitions;
        try (LockGuard lg = lockGuardFactory.build(READ, RSC_DFN_MAP))
        {
            listSnapshotDefinitions = snapshotApiCallHandler.listSnapshotDefinitions(
                nodeNames, resourceNames);
        }
        return listSnapshotDefinitions;
    }

    public ArrayList<SnapshotShippingListItemApi> listSnapshotShippings(
        List<String> nodeNames,
        List<String> resourceNames,
        List<String> snapshotNames,
        List<String> status
    )
    {
        ArrayList<SnapshotShippingListItemApi> listSnapshotShippings;
        try (LockGuard lg = lockGuardFactory.build(READ, RSC_DFN_MAP))
        {
            listSnapshotShippings = snapshotShippingApiCallHandler.listSnapshotShippings(
                nodeNames,
                resourceNames,
                snapshotNames,
                status
            );
        }
        return listSnapshotShippings;
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

    public Flux<ApiCallRc> modifyVlm(
        UUID uuidRef,
        String nodeNameRef,
        String rscNameRef,
        Integer vlmNrRef,
        Map<String, String> overridePropsRef,
        Set<String> deletePropKeys,
        Set<String> deleteNamespacesRef
    )
    {
        Flux<ApiCallRc> flux;
        try (LockGuard lg = lockGuardFactory.build(WRITE, NODES_MAP, RSC_DFN_MAP))
        {
            flux = vlmApiCallHandler.modify(
                uuidRef,
                nodeNameRef,
                rscNameRef,
                vlmNrRef,
                overridePropsRef,
                deletePropKeys,
                deleteNamespacesRef
            );
        }
        return flux;
    }

    public List<ResourceGroupApi> listResourceGroups(List<String> rscGrpNames, List<String> propFilters)
    {
        List<ResourceGroupApi> ret;
        try (LockGuard lg = lockGuardFactory.build(READ, RSC_GRP_MAP))
        {
            ret = rscGrpApiCallHandler.listResourceGroups(rscGrpNames, propFilters);
        }
        return ret;
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

    public ApiCallRc createVlmGrps(String rscGrpNameRef, List<VolumeGroupApi> vlmGrpApiListRef)
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

    public List<VolumeGroupApi> listVolumeGroups(String rscNameRef, Integer vlmNrRef)
    {
        List<VolumeGroupApi> listVolumeGroups;
        try (LockGuard lg = lockGuardFactory.build(READ, RSC_GRP_MAP))
        {
            listVolumeGroups = vlmGrpApiCallHandler.listVolumeGroups(rscNameRef, vlmNrRef);
        }
        return listVolumeGroups;
    }

    public Flux<ApiCallRc> modifyVolumeGroup(
        String rscGrpNameRef,
        int vlmNrRef,
        Map<String, String> overrideProps,
        HashSet<String> deletePropKeys,
        HashSet<String> deleteNamespaces,
        List<String> flags
    )
    {
        Flux<ApiCallRc> flux;
        try (LockGuard lg = lockGuardFactory.build(WRITE, RSC_GRP_MAP))
        {
            flux = vlmGrpApiCallHandler.modify(
                rscGrpNameRef,
                vlmNrRef,
                overrideProps,
                deletePropKeys,
                deleteNamespaces,
                flags
            );
        }
        return flux;
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

    private boolean containsLayerKind(ResourceDefinitionApi rscDfnApi, ApiConsts.DeviceLayerKind kind) {
        return rscDfnApi.getLayerData().stream().anyMatch(pair -> pair.objA.equalsIgnoreCase(kind.getValue()));
    }

    private static Map<String, Integer> getExpectedOnlineDiskfulNodeIds(ResourceList rscs)
    {
        Map<String, Integer> diskfulExpectedOnlineNodeIds = new HashMap<>();
        for (ResourceApi resourceApi : rscs.getResources())
        {
            // if DRBD is involved, it has to be the topmost layer if it exists in the
            // layer-tree
            RscLayerDataApi rootLayerData = resourceApi.getLayerData();
            if (rootLayerData.getLayerKind().equals(DeviceLayerKind.DRBD) && !resourceApi.isDRBDDiskless())
            {
                diskfulExpectedOnlineNodeIds.put(
                    resourceApi.getNodeName().toLowerCase(),
                    ((DrbdRscPojo) rootLayerData).getNodeId()
                );
            }
        }
        return diskfulExpectedOnlineNodeIds;
    }

    private ApiConsts.CloneStatus cloneCheckRscDfn(ResourceDefinitionApi cloneRscDfn, ResourceList rscs)
        throws InvalidNameException
    {
        ResourceName rscCloneName = new ResourceName(cloneRscDfn.getResourceName());
        ApiConsts.CloneStatus status = ApiConsts.CloneStatus.CLONING;
        if ((cloneRscDfn.getFlags() & CLONING.flagValue) != CLONING.flagValue)
        {
            boolean isReady = true;
            boolean anyFailed = false;

            // if DRBD is involved wait for all resources to be ready
            if (containsLayerKind(cloneRscDfn, ApiConsts.DeviceLayerKind.DRBD))
            {
                if (rscs.getSatelliteStates().size() >= rscs.getResources().size())
                {
                    Map<String, Integer> diskfulExpectedOnlineNodeIds = getExpectedOnlineDiskfulNodeIds(rscs);

                    final Set<String> nodeNames = rscs.getResources().stream()
                        .map(r -> r.getNodeName().toLowerCase())
                        .collect(Collectors.toSet());

                    for (Map.Entry<NodeName, SatelliteState> entry : rscs.getSatelliteStates().entrySet())
                    {
                        // Ignore non resource nodes
                        String lowerCaseNodeName = entry.getKey().displayValue.toLowerCase();
                        if (nodeNames.contains(lowerCaseNodeName))
                        {
                            Map<String, Integer> expectedOnlineNodeIds = new HashMap<>(
                                diskfulExpectedOnlineNodeIds
                            );
                            expectedOnlineNodeIds.remove(lowerCaseNodeName);

                            SatelliteState stltState = entry.getValue();
                            SatelliteResourceState rscState =
                                stltState.getResourceStates().get(rscCloneName);

                            if (rscState == null)
                            {
                                isReady = false;
                                break;

                            }
                            if (!rscState.isReady(expectedOnlineNodeIds.values()))
                            {
                                isReady = false;

                                anyFailed = rscState.getConnectionStates().values()
                                    .stream()
                                    .flatMap(f -> f.values().stream())
                                    .anyMatch("StandAlone"::equalsIgnoreCase);

                                if (anyFailed)
                                {
                                    break;
                                }

                                boolean allDrbdConnected = rscState.getConnectionStates().values()
                                    .stream()
                                    .flatMap(f -> f.values().stream())
                                    .allMatch("Connected"::equalsIgnoreCase);

                                if (allDrbdConnected)
                                {
                                    anyFailed = rscState.getVolumeStates().values().stream().anyMatch(
                                        vlmState ->
                                            vlmState.getDiskState().equalsIgnoreCase("Inconsistent") ||
                                                vlmState.getDiskState().equalsIgnoreCase("Outdated")
                                    );
                                    if (anyFailed)
                                    {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                else
                {
                    isReady = false;
                }
            }

            if (isReady)
            {
                status = ApiConsts.CloneStatus.COMPLETE;
            }
            else
            {
                if (anyFailed)
                {
                    status = ApiConsts.CloneStatus.FAILED;
                }
            }
        }
        return status;
    }

    public ApiConsts.CloneStatus isCloneReady(
        String cloneName
    )
    {
        ApiConsts.CloneStatus status;
        try (LockGuard ignored = lockGuardFactory.build(READ, NODES_MAP, RSC_DFN_MAP))
        {
            ArrayList<ResourceDefinitionApi> clonedResources = this.listResourceDefinitions(
                Collections.singletonList(cloneName),
                Collections.singletonList(InternalApiConsts.KEY_CLONED_FROM));

            if (clonedResources != null && clonedResources.size() == 1)
            {
                final ResourceDefinitionApi cloneRscDfn = clonedResources.get(0);
                final ResourceList rscs = this.listResource(
                    cloneRscDfn.getResourceName(), Collections.emptyList());

                List<NodeApi> nodes = this.listNodes(rscs.getResources().stream()
                    .map(ResourceApi::getNodeName).collect(Collectors.toList()), Collections.emptyList());
                boolean anyNodesOffline = nodes.stream()
                    .anyMatch(node -> node.connectionStatus() != ApiConsts.ConnectionStatus.ONLINE);

                if ((cloneRscDfn.getFlags() & FAILED.flagValue) == FAILED.flagValue || anyNodesOffline)
                {
                    status = ApiConsts.CloneStatus.FAILED;
                }
                else
                {
                    status = cloneCheckRscDfn(cloneRscDfn, rscs);
                }
            }
            else
            {
                throw new ApiRcException(
                    ApiCallRcImpl.singleApiCallRc(ApiConsts.FAIL_NOT_FOUND_RSC_DFN, "Cloned resource not found.")
                );
            }
        }
        catch (InvalidNameException exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.singleApiCallRc(ApiConsts.FAIL_INVLD_RSC_NAME, "Invalid resource name provided.")
            );
        }
        return status;
    }

    public ApiCallRc backupDb(@Nonnull String backupPath)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        try
        {
            apiCallRc.addEntries(dbEngine.backupDb(backupPath));
        }
        catch (DatabaseException dbExc)
        {
            throw new ApiDatabaseException(dbExc);
        }
        return apiCallRc;
    }

    public boolean isResourceSynced(String resourceName)
    {
        try (LockGuard ignored = lockGuardFactory.build(READ, NODES_MAP, RSC_DFN_MAP))
        {
            return rscDfnApiCallHandler.isResourceSynced(resourceName);
        }
    }
}
