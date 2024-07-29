package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.ExternalFileName;
import com.linbit.linstor.core.identifier.KeyValueStoreName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.ScheduleName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.ExternalFile;
import com.linbit.linstor.core.objects.KeyValueStore;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.Schedule;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.objects.VolumeGroup;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.repository.ExternalFileRepository;
import com.linbit.linstor.core.repository.KeyValueStoreRepository;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.core.repository.RemoteRepository;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.core.repository.ResourceGroupRepository;
import com.linbit.linstor.core.repository.ScheduleRepository;
import com.linbit.linstor.core.repository.StorPoolDefinitionRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.getRscDfnDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscGrpApiCallHandler.getRscGrpDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlVlmGrpApiCallHandler.getVlmGrpDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.helpers.StorPoolHelper.getStorPoolDescriptionInline;

import javax.inject.Inject;
import javax.inject.Provider;

public class CtrlApiDataLoader
{
    private final Provider<AccessContext> peerAccCtx;
    private final AccessContext systemCtx;
    private final NodeRepository nodeRepository;
    private final ResourceDefinitionRepository resourceDefinitionRepository;
    private final StorPoolDefinitionRepository storPoolDefinitionRepository;
    private final KeyValueStoreRepository kvsRepository;
    private final SystemConfRepository systemConfRepository;
    private final ResourceGroupRepository resourceGroupRepository;
    private final ExternalFileRepository extFileRepository;
    private final RemoteRepository remoteRepository;
    private final ScheduleRepository scheduleRepository;

    @Inject
    public CtrlApiDataLoader(
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        @SystemContext AccessContext sysCtxRef,
        NodeRepository nodeRepositoryRef,
        ResourceDefinitionRepository resourceDefinitionRepositoryRef,
        StorPoolDefinitionRepository storPoolDefinitionRepositoryRef,
        KeyValueStoreRepository kvsRepositoryRef,
        SystemConfRepository systemConfRepositoryRef,
        ResourceGroupRepository resourceGroupRepositoryRef,
        ExternalFileRepository extFileRepositoryRef,
        RemoteRepository remoteRepositoryRef,
        ScheduleRepository scheduleRepositoryRef
    )
    {
        peerAccCtx = peerAccCtxRef;
        systemCtx = sysCtxRef;
        nodeRepository = nodeRepositoryRef;
        resourceDefinitionRepository = resourceDefinitionRepositoryRef;
        storPoolDefinitionRepository = storPoolDefinitionRepositoryRef;
        kvsRepository = kvsRepositoryRef;
        systemConfRepository = systemConfRepositoryRef;
        resourceGroupRepository = resourceGroupRepositoryRef;
        extFileRepository = extFileRepositoryRef;
        remoteRepository = remoteRepositoryRef;
        scheduleRepository = scheduleRepositoryRef;
    }

    public final @Nullable Node loadNode(String nodeNameStr, boolean failIfNull)
    {
        return loadNode(LinstorParsingUtils.asNodeName(nodeNameStr), failIfNull);
    }

    public final @Nullable Node loadNode(NodeName nodeName, boolean failIfNull)
    {
        return loadNode(nodeName, failIfNull, false);
    }

    public final @Nullable Node loadNode(NodeName nodeName, boolean failIfNull, boolean ignoreSearchDomain)
    {
        @Nullable Node node;
        NodeName fqdnName = nodeName;
        try
        {
            // if node name is a short name, try to append search domain (if there is any)
            if (!ignoreSearchDomain && !nodeName.getDisplayName().contains("."))
            {
                // TODO: use user properties
                final ReadOnlyProps ctrlProps = systemConfRepository.getCtrlConfForView(systemCtx);
                try
                {
                    final String domain = ctrlProps.getProp(ApiConsts.KEY_SEARCH_DOMAIN);
                    if (domain != null)
                    {
                        fqdnName = LinstorParsingUtils.asNodeName(nodeName.getDisplayName() + "." + domain);
                    }
                }
                catch (InvalidKeyException ignored)
                {
                }
            }

            node = nodeRepository.get(
                peerAccCtx.get(),
                fqdnName
            );

            if (failIfNull && node == null)
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_NOT_FOUND_NODE,
                        "Node '" + fqdnName.displayValue + "' not found."
                    )
                    .setCause("The specified node '" + fqdnName.displayValue + "' could not be found in the database")
                    .setCorrection("Create a node with the name '" + fqdnName.displayValue + "' first.")
                    .build()
                );
            }
        }
        catch (AccessDeniedException accDenied)
        {
            throw new ApiAccessDeniedException(
                accDenied,
                "loading node '" + fqdnName.displayValue + "'.",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        return node;
    }

    public final @Nullable ResourceDefinition loadRscDfn(
        String rscNameStr,
        boolean failIfNull
    )
    {
        return loadRscDfn(LinstorParsingUtils.asRscName(rscNameStr), failIfNull);
    }

    public final @Nullable ResourceDefinition loadRscDfn(
        ResourceName rscName,
        boolean failIfNull
    )
    {
        @Nullable ResourceDefinition rscDfn;
        try
        {
            rscDfn = resourceDefinitionRepository.get(
                peerAccCtx.get(),
                rscName
            );

            if (failIfNull && rscDfn == null)
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_NOT_FOUND_RSC_DFN,
                        "Resource definition '" + rscName.displayValue + "' not found."
                    )
                    .setCause("The specified resource definition '" + rscName.displayValue +
                        "' could not be found in the database")
                    .setCorrection("Create a resource definition with the name '" + rscName.displayValue + "' first.")
                    .build()
                );
            }

        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access " + getRscDfnDescriptionInline(rscName.displayValue),
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        return rscDfn;
    }

    public @Nullable VolumeDefinition loadVlmDfn(String rscNameStr, int vlmNrInt, boolean failIfNull)
    {
        return loadVlmDfn(LinstorParsingUtils.asRscName(rscNameStr), LinstorParsingUtils.asVlmNr(vlmNrInt), failIfNull);
    }

    public @Nullable VolumeDefinition loadVlmDfn(ResourceName rscName, VolumeNumber vlmNr, boolean failIfNull)
    {
        @Nullable ResourceDefinition rscDfn = loadRscDfn(rscName, failIfNull);
        @Nullable VolumeDefinition vlmDfn = null;
        try
        {
            if (rscDfn != null)
            {
                vlmDfn = rscDfn.getVolumeDfn(peerAccCtx.get(), vlmNr);
            }

            if (failIfNull && vlmDfn == null)
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_NOT_FOUND_VLM_DFN,
                        "Volume definition '" + rscName + "' with volume number '" + vlmNr + "' not found."
                    )
                    .setCause("The specified volume definition '" + rscName +
                        "' with volume number '" + vlmNr + "' could not be found in the database")
                    .setCorrection("Create a volume definition with the name '" + rscName + "' first.")
                    .build()
                );
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "load volume definition '" + vlmNr + "' from resource definition '" + rscName + "'",
                ApiConsts.FAIL_ACC_DENIED_VLM_DFN
            );
        }
        return vlmDfn;
    }

    public @Nullable Resource loadRsc(String nodeName, String rscName, boolean failIfNull)
    {
        return loadRsc(LinstorParsingUtils.asNodeName(nodeName), LinstorParsingUtils.asRscName(rscName), failIfNull);
    }

    public @Nullable Resource loadRsc(NodeName nodeName, ResourceName rscName, boolean failIfNull)
    {
        Resource result = null;
        Node node = loadNode(nodeName, failIfNull);
        ResourceDefinition rscDfn = loadRscDfn(rscName, failIfNull);
        if (node != null && rscDfn != null)
        {
            result = loadRsc(rscDfn, node, failIfNull);
        }
        return result;
    }

    public @Nullable Resource loadRsc(ResourceDefinition rscDfn, String nodeNameStr, boolean failIfNull)
    {
        Resource result = null;
        Node node = loadNode(nodeNameStr, failIfNull);
        if (node != null)
        {
            result = loadRsc(rscDfn, node, failIfNull);
        }
        return result;
    }

    public @Nullable Resource loadRsc(ResourceDefinition rscDfn, Node node, boolean failIfNull)
    {
        ResourceName rscName = rscDfn.getName();
        NodeName nodeName = node.getName();
        @Nullable Resource rsc;
        try
        {
            rsc = node.getResource(peerAccCtx.get(), rscDfn.getName());
            if (rsc == null && failIfNull)
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_NOT_FOUND_RSC,
                        "Resource '" + rscName + "' on node '" + nodeName + "' not found."
                    )
                    .setCause("The specified resource '" + rscName + "' on node '" + nodeName + "' could not " +
                        "be found in the database")
                    .setCorrection("Create a resource with the name '" + rscName + "' on node '" + nodeName +
                        "' first.")
                    .build()
                );
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "loading resource '" + rscName + "' on node '" + nodeName + "'",
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return rsc;
    }

    public ResourceConnection loadRscConn(ResourceName rscNameRef, NodeName nodeANameRef, NodeName nodeBNameRef)
    {
        Resource rscA = loadRsc(nodeANameRef, rscNameRef, true);
        Resource rscB = loadRsc(nodeBNameRef, rscNameRef, true);
        ResourceConnection rscCon;
        try
        {
            rscCon = rscA.getAbsResourceConnection(systemCtx, rscB);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "loading resource connection '" + rscNameRef + "' between nodes '" + nodeANameRef + "' and '" +
                    nodeBNameRef + "'",
                ApiConsts.FAIL_ACC_DENIED_RSC_CONN
            );
        }
        return rscCon;
    }

    public final @Nullable SnapshotDefinition loadSnapshotDfn(
        String rscNameStr,
        String snapshotNameStr,
        boolean failIfNull
    )
    {
        return loadSnapshotDfn(
            LinstorParsingUtils.asRscName(rscNameStr),
            LinstorParsingUtils.asSnapshotName(snapshotNameStr),
            failIfNull
        );
    }

    public final @Nullable SnapshotDefinition loadSnapshotDfn(
        ResourceName rscName,
        SnapshotName snapshotName,
        boolean failIfNull
    )
    {
        SnapshotDefinition result = null;
        ResourceDefinition rscDfn = loadRscDfn(rscName, failIfNull);
        if (rscDfn != null)
        {
            result = loadSnapshotDfn(rscDfn, snapshotName, failIfNull);
        }
        return result;
    }

    public final @Nullable SnapshotDefinition loadSnapshotDfn(
        ResourceDefinition rscDfn,
        SnapshotName snapshotName,
        boolean failIfNull
    )
    {
        @Nullable SnapshotDefinition snapshotDfn;
        try
        {
            snapshotDfn = rscDfn.getSnapshotDfn(peerAccCtx.get(), snapshotName);

            if (failIfNull && snapshotDfn == null)
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_NOT_FOUND_SNAPSHOT_DFN,
                        "Snapshot '" + snapshotName.displayValue +
                            "' of resource '" + rscDfn.getName().displayValue + "' not found."
                    )
                    .build()
                );
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "loading snapshot '" + snapshotName + "' of resource '" + rscDfn.getName() + "'",
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT_DFN
            );
        }
        return snapshotDfn;
    }

    public @Nullable Snapshot loadSnapshot(Node node, SnapshotDefinition snapshotDfn)
    {
        Snapshot snapshot;
        try
        {
            snapshot = snapshotDfn.getSnapshot(peerAccCtx.get(), node.getName());

            if (snapshot == null)
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_NOT_FOUND_SNAPSHOT,
                    "Snapshot '" + snapshotDfn.getName() +
                        "' of resource '" + snapshotDfn.getResourceName() +
                        "' on node '" + node.getName() + "' not found."
                ));
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "load snapshot '" + snapshotDfn.getName() +
                    "' of resource '" + snapshotDfn.getResourceName() +
                    "' on node '" + node.getName() + "'",
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT
            );
        }
        return snapshot;
    }

    public SnapshotVolume loadSnapshotVlm(Snapshot snapshot, VolumeNumber vlmNr)
    {
        SnapshotVolume snapshotVolume = snapshot.getVolume(vlmNr);

        if (snapshotVolume == null)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_NOT_FOUND_SNAPSHOT,
                    "Volume " + vlmNr +
                    " of snapshot '" + snapshot.getSnapshotName() +
                    "' of resource '" + snapshot.getResourceName() +
                    "' on node '" + snapshot.getNodeName() + "' not found."
                )
            );
        }
        return snapshotVolume;
    }

    public final @Nullable StorPoolDefinition loadStorPoolDfn(String storPoolNameStr, boolean failIfNull)
    {
        return loadStorPoolDfn(LinstorParsingUtils.asStorPoolName(storPoolNameStr), failIfNull);
    }

    public final @Nullable StorPoolDefinition loadStorPoolDfn(
        StorPoolName storPoolName,
        boolean failIfNull
    )
    {
        @Nullable StorPoolDefinition storPoolDfn;
        try
        {
            storPoolDfn = storPoolDefinitionRepository.get(
                peerAccCtx.get(),
                storPoolName
            );

            if (failIfNull && storPoolDfn == null)
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_NOT_FOUND_STOR_POOL_DFN,
                        "Storage pool definition '" + storPoolName.displayValue + "' not found."
                    )
                    .setCause("The specified storage pool definition '" + storPoolName.displayValue +
                        "' could not be found in the database")
                    .setCorrection("Create a storage pool definition '" + storPoolName.displayValue + "' first.")
                    .build()
                );
            }

        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "loading storage pool definition '" + storPoolName.displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL_DFN
            );
        }
        return storPoolDfn;
    }

    public @Nullable StorPool loadStorPool(String storPoolNameStr, String nodeNameStrRef, boolean failIfNullRef)
    {
        StorPoolDefinition storPoolDfn = loadStorPoolDfn(storPoolNameStr, failIfNullRef);
        Node node = loadNode(nodeNameStrRef, failIfNullRef);

        @Nullable StorPool ret = null;
        if (storPoolDfn != null && node != null)
        {
            ret = loadStorPool(storPoolDfn, node, failIfNullRef);
        }
        return ret;
    }

    public final @Nullable StorPool loadStorPool(String storPoolNameStr, Node node, boolean failIfNullRef)
    {
        @Nullable StorPool ret = null;

        StorPoolDefinition storPoolDfn = loadStorPoolDfn(storPoolNameStr, failIfNullRef);
        if (storPoolDfn != null && node != null)
        {
            ret = loadStorPool(storPoolDfn, node, failIfNullRef);
        }
        return ret;
    }

    public final @Nullable StorPool loadStorPool(
        StorPoolDefinition storPoolDfn,
        Node node,
        boolean failIfNull
    )
    {
        @Nullable StorPool storPool;
        try
        {
            storPool = node.getStorPool(peerAccCtx.get(), storPoolDfn.getName());

            if (failIfNull && storPool == null)
            {
                throw new ApiRcException(
                    ApiCallRcImpl
                        .entryBuilder(
                            ApiConsts.FAIL_NOT_FOUND_STOR_POOL_DFN,
                            "Storage pool '" + storPoolDfn.getName().displayValue + "' on node '" +
                                node.getName().displayValue + "' not found."
                        )
                        .setCause(
                            "The specified storage pool '" + storPoolDfn.getName().displayValue +
                                "' on node '" + node.getName().displayValue + "' could not be found in the database"
                        )
                        .setCorrection(
                            "Create a storage pool '" + storPoolDfn.getName().displayValue + "' on node '" +
                                node.getName().displayValue + "' first."
                        )
                        .build()
                );
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "load " + getStorPoolDescriptionInline(node, storPoolDfn),
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL
            );
        }
        return storPool;
    }

    public final @Nullable KeyValueStore loadKvs(String kvsNameStr, boolean failIfNull)
    {
        return loadKvs(LinstorParsingUtils.asKvsName(kvsNameStr), failIfNull);
    }

    public final @Nullable KeyValueStore loadKvs(KeyValueStoreName kvsName, boolean failIfNull)
    {
        @Nullable KeyValueStore kvs;
        try
        {
            kvs = kvsRepository.get(
                peerAccCtx.get(),
                kvsName
            );

            if (failIfNull && kvs == null)
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_NOT_FOUND_KVS,
                        "KeyValueStore '" + kvsName.displayValue + "' not found."
                    )
                    .setCause(
                        "The specified keyValueStore '" + kvsName.displayValue +
                        "' could not be found in the database"
                    )
                    .setCorrection("Create a keyValueStore with the name '" + kvsName.displayValue + "' first.")
                    .build()
                );
            }
        }
        catch (AccessDeniedException accDenied)
        {
            throw new ApiAccessDeniedException(
                    accDenied,
                    "loading keyValueStore '" + kvsName.displayValue + "'.",
                    ApiConsts.FAIL_ACC_DENIED_KVS
            );
        }
        return kvs;
    }

    public @Nullable Volume loadVlm(
        String nodeNameStrRef,
        String rscNameStrRef,
        Integer vlmNrIntRef,
        boolean failIfNull
    )
    {
        return loadVlm(
            LinstorParsingUtils.asNodeName(nodeNameStrRef),
            LinstorParsingUtils.asRscName(rscNameStrRef),
            LinstorParsingUtils.asVlmNr(vlmNrIntRef),
            failIfNull
        );
    }

    public final @Nullable ResourceGroup loadResourceGroup(String rscGrpNameStringRef, boolean failIfNull)
    {
        return loadResourceGroup(
            LinstorParsingUtils.asRscGrpName(rscGrpNameStringRef),
            failIfNull
        );
    }

    private @Nullable Volume loadVlm(
        NodeName nodeNameREf,
        ResourceName rscNameRef,
        VolumeNumber vlmNrRef,
        boolean failIfNullRef
    )
    {
        Resource rsc = loadRsc(nodeNameREf, rscNameRef, failIfNullRef);
        @Nullable Volume vlm = rsc.getVolume(vlmNrRef);
        if (vlm == null && failIfNullRef)
        {
            throw new ApiRcException(ApiCallRcImpl
                .entryBuilder(
                    ApiConsts.FAIL_NOT_FOUND_VLM,
                    CtrlVlmApiCallHandler.getVlmDescription(nodeNameREf, rscNameRef, vlmNrRef) + " not found."
                )
                .build()
            );
        }
        return vlm;
    }

    public final @Nullable ResourceGroup loadResourceGroup(ResourceGroupName rscGrpNameRef, boolean failIfNull)
    {
        @Nullable ResourceGroup rscGrp;
        try
        {
            rscGrp = resourceGroupRepository.get(
                peerAccCtx.get(),
                rscGrpNameRef
            );
            if (failIfNull && rscGrp == null)
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_NOT_FOUND_RSC_GRP,
                        "Resource group '" + rscGrpNameRef.displayValue + "' not found."
                    )
                    .setCause("The specified resource group '" + rscGrpNameRef.displayValue +
                        "' could not be found in the database")
                    .setCorrection("Create a resource group with the name '" +
                        rscGrpNameRef.displayValue + "' first.")
                    .setSkipErrorReport(true)
                    .build()
                );
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access " + getRscGrpDescriptionInline(rscGrpNameRef.displayValue),
                ApiConsts.FAIL_ACC_DENIED_RSC_GRP
            );
        }
        return rscGrp;
    }

    public final @Nullable VolumeGroup loadVlmGrp(String rscGrpNameStringRef, int vlmNrInt, boolean failIfNull)
    {
        return loadVlmGrp(
            LinstorParsingUtils.asRscGrpName(rscGrpNameStringRef),
            LinstorParsingUtils.asVlmNr(vlmNrInt),
            failIfNull
        );
    }

    public final @Nullable VolumeGroup loadVlmGrp(
        ResourceGroupName rscGrpNameRef,
        VolumeNumber vlmNr,
        boolean failIfNull
    )
    {
        @Nullable VolumeGroup vlmGrp = null;
        try
        {
            ResourceGroup rscGrp = loadResourceGroup(rscGrpNameRef, failIfNull);
            if (rscGrp != null)
            {
                vlmGrp = rscGrp.getVolumeGroup(peerAccCtx.get(), vlmNr);
            }
            if (failIfNull && vlmGrp == null)
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_NOT_FOUND_VLM_GRP,
                        "Volume group '" + rscGrpNameRef.displayValue + "' with volume number '" +
                            vlmNr + "' not found."
                    )
                    .setCause("The specified volume group '" + rscGrpNameRef.displayValue +
                        "' with volume number '" + vlmNr + "' could not be found in the database")
                    .setCorrection("Create a volume group with the name '" +
                        rscGrpNameRef.displayValue + "' and volume number '" + vlmNr + "' first.")
                    .build()
                );
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access " + getVlmGrpDescriptionInline(rscGrpNameRef.displayValue, vlmNr.value),
                ApiConsts.FAIL_ACC_DENIED_VLM_GRP
            );
        }
        return vlmGrp;
    }

    public final @Nullable ExternalFile loadExtFile(String extFileNameStr, boolean failIfNull)
    {
        return loadExtFile(LinstorParsingUtils.asExtFileName(extFileNameStr), failIfNull);
    }

    public final @Nullable ExternalFile loadExtFile(
        ExternalFileName extFileName,
        boolean failIfNull
    )
    {
        @Nullable ExternalFile extFile;
        try
        {
            extFile = extFileRepository.get(
                peerAccCtx.get(),
                extFileName
            );

            if (failIfNull && extFile == null)
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_NOT_FOUND_EXT_FILE,
                        "External file '" + extFileName.extFileName + "' not registered."
                    )
                    .setCause(
                        "The specified external file '" + extFileName.extFileName +
                        "' could not be found in the database")
                    .setCorrection("Create an external file with the name '" + extFileName.extFileName + "' first.")
                    .build()
                );
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access " + getRscDfnDescriptionInline(extFileName.extFileName),
                ApiConsts.FAIL_ACC_DENIED_EXT_FILE
            );
        }
        return extFile;
    }

    public final @Nullable AbsRemote loadRemote(String remoteNameStr, boolean failIfNull)
    {
        return loadRemote(LinstorParsingUtils.asRemoteName(remoteNameStr), failIfNull);
    }

    public final @Nullable AbsRemote loadRemote(
        RemoteName remoteName,
        boolean failIfNull
    )
    {
        @Nullable AbsRemote remote;
        try
        {
            remote = remoteRepository.get(
                peerAccCtx.get(),
                remoteName
            );

            if (failIfNull && remote == null)
            {
                throw new ApiRcException(
                    ApiCallRcImpl
                        .entryBuilder(
                            ApiConsts.FAIL_NOT_FOUND_REMOTE,
                            "Remote '" + remoteName.displayValue + "' not registered."
                        )
                        .setCause(
                            "The specified remote '" + remoteName.displayValue +
                                "' could not be found in the database"
                        )
                        .setCorrection("Create a remote with the name '" + remoteName.displayValue + "' first.")
                        .build()
                );
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access " + getRscDfnDescriptionInline(remoteName.displayValue),
                ApiConsts.FAIL_ACC_DENIED_REMOTE
            );
        }
        return remote;
    }

    public final @Nullable Schedule loadSchedule(String scheduleNameStr, boolean failIfNull)
    {
        return loadSchedule(LinstorParsingUtils.asScheduleName(scheduleNameStr), failIfNull);
    }

    public final @Nullable Schedule loadSchedule(
        ScheduleName scheduleName,
        boolean failIfNull
    )
    {
        @Nullable Schedule schedule;
        try
        {
            schedule = scheduleRepository.get(
                peerAccCtx.get(),
                scheduleName
            );

            if (failIfNull && schedule == null)
            {
                throw new ApiRcException(
                    ApiCallRcImpl
                        .entryBuilder(
                            ApiConsts.FAIL_NOT_FOUND_SCHEDULE,
                            "Schedule '" + scheduleName.displayValue + "' not registered."
                        )
                        .setCause(
                            "The specified schedule '" + scheduleName.displayValue +
                                "' could not be found in the database"
                        )
                        .setCorrection("Create a schedule with the name '" + scheduleName.displayValue + "' first.")
                        .build()
                );
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access " + getRscDfnDescriptionInline(scheduleName.displayValue),
                ApiConsts.FAIL_ACC_DENIED_SCHEDULE
            );
        }
        return schedule;
    }
}
