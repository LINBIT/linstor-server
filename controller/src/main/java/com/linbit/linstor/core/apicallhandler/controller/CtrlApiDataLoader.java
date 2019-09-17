package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.KeyValueStoreName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.KeyValueStore;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.NodeData;
import com.linbit.linstor.core.objects.ResourceData;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceDefinitionData;
import com.linbit.linstor.core.objects.ResourceGroupData;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotDefinitionData;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.StorPoolData;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.objects.StorPoolDefinitionData;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeData;
import com.linbit.linstor.core.objects.VolumeDefinitionData;
import com.linbit.linstor.core.objects.VolumeGroupData;
import com.linbit.linstor.core.repository.KeyValueStoreRepository;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.core.repository.ResourceGroupRepository;
import com.linbit.linstor.core.repository.StorPoolDefinitionRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Provider;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.getRscDfnDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscGrpApiCallHandler.getRscGrpDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlVlmGrpApiCallHandler.getVlmGrpDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.helpers.StorPoolHelper.getStorPoolDescriptionInline;

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

    @Inject
    public CtrlApiDataLoader(
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        @SystemContext AccessContext sysCtxRef,
        NodeRepository nodeRepositoryRef,
        ResourceDefinitionRepository resourceDefinitionRepositoryRef,
        StorPoolDefinitionRepository storPoolDefinitionRepositoryRef,
        KeyValueStoreRepository kvsRepositoryRef,
        SystemConfRepository systemConfRepositoryRef,
        ResourceGroupRepository resourceGroupRepositoryRef
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
    }

    public final NodeData loadNode(String nodeNameStr, boolean failIfNull)
    {
        return loadNode(LinstorParsingUtils.asNodeName(nodeNameStr), failIfNull);
    }

    public final NodeData loadNode(NodeName nodeName, boolean failIfNull)
    {
        return loadNode(nodeName, failIfNull, false);
    }

    public final NodeData loadNode(NodeName nodeName, boolean failIfNull, boolean ignoreSearchDomain)
    {
        NodeData node;
        NodeName fqdnName = nodeName;
        try
        {
            // if node name is a short name, try to append search domain (if there is any)
            if (!ignoreSearchDomain && !nodeName.getDisplayName().contains("."))
            {
                final Props ctrlProps = systemConfRepository.getCtrlConfForView(systemCtx); // TODO: use user properties
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

    public final ResourceDefinitionData loadRscDfn(
        String rscNameStr,
        boolean failIfNull
    )
    {
        return loadRscDfn(LinstorParsingUtils.asRscName(rscNameStr), failIfNull);
    }

    public final ResourceDefinitionData loadRscDfn(
        ResourceName rscName,
        boolean failIfNull
    )
    {
        ResourceDefinitionData rscDfn;
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

    public VolumeDefinitionData loadVlmDfn(String rscNameStr, int vlmNrInt, boolean failIfNull)
    {
        return loadVlmDfn(LinstorParsingUtils.asRscName(rscNameStr), LinstorParsingUtils.asVlmNr(vlmNrInt), failIfNull);
    }

    public VolumeDefinitionData loadVlmDfn(ResourceName rscName, VolumeNumber vlmNr, boolean failIfNull)
    {
        ResourceDefinitionData rscDfn = loadRscDfn(rscName, true);
        VolumeDefinitionData vlmDfn;
        try
        {
            vlmDfn = (VolumeDefinitionData) rscDfn.getVolumeDfn(peerAccCtx.get(), vlmNr);

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

    public ResourceData loadRsc(String nodeName, String rscName, boolean failIfNull)
    {
        return loadRsc(LinstorParsingUtils.asNodeName(nodeName), LinstorParsingUtils.asRscName(rscName), failIfNull);
    }

    public ResourceData loadRsc(NodeName nodeName, ResourceName rscName, boolean failIfNull)
    {
        Node node = loadNode(nodeName, true);
        ResourceDefinitionData rscDfn = loadRscDfn(rscName, true);
        return loadRsc(rscDfn, node, failIfNull);
    }

    public ResourceData loadRsc(ResourceDefinition rscDfn, String nodeNameStr, boolean failIfNull)
    {
        Node node = loadNode(nodeNameStr, true);
        return loadRsc(rscDfn, node, failIfNull);
    }

    public ResourceData loadRsc(ResourceDefinition rscDfn, Node node, boolean failIfNull)
    {
        ResourceName rscName = rscDfn.getName();
        NodeName nodeName = node.getName();
        ResourceData rscData;
        try
        {
            rscData = (ResourceData) node.getResource(peerAccCtx.get(), rscDfn.getName());
            if (rscData == null && failIfNull)
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
        return rscData;
    }

    public final SnapshotDefinitionData loadSnapshotDfn(
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

    public final SnapshotDefinitionData loadSnapshotDfn(
        ResourceName rscName,
        SnapshotName snapshotName,
        boolean failIfNull
    )
    {
        return loadSnapshotDfn(loadRscDfn(rscName, true), snapshotName, failIfNull);
    }

    public final SnapshotDefinitionData loadSnapshotDfn(
        ResourceDefinition rscDfn,
        SnapshotName snapshotName,
        boolean failIfNull
    )
    {
        SnapshotDefinitionData snapshotDfn;
        try
        {
            snapshotDfn = (SnapshotDefinitionData) rscDfn.getSnapshotDfn(peerAccCtx.get(), snapshotName);

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

    public Snapshot loadSnapshot(Node node, SnapshotDefinition snapshotDfn)
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
        SnapshotVolume snapshotVolume;
        try
        {
            snapshotVolume = snapshot.getSnapshotVolume(peerAccCtx.get(), vlmNr);

            if (snapshotVolume == null)
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_NOT_FOUND_SNAPSHOT,
                    "Volume " + vlmNr +
                        " of snapshot '" + snapshot.getSnapshotName() +
                        "' of resource '" + snapshot.getResourceName() +
                        "' on node '" + snapshot.getNodeName() + "' not found."
                ));
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "load volume " + vlmNr +
                    " of snapshot '" + snapshot.getSnapshotName() +
                    "' of resource '" + snapshot.getResourceName() +
                    "' on node '" + snapshot.getNodeName() + "'",
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT
            );
        }
        return snapshotVolume;
    }

    public final StorPoolDefinitionData loadStorPoolDfn(String storPoolNameStr, boolean failIfNull)
    {
        return loadStorPoolDfn(LinstorParsingUtils.asStorPoolName(storPoolNameStr), failIfNull);
    }

    public final StorPoolDefinitionData loadStorPoolDfn(
        StorPoolName storPoolName,
        boolean failIfNull
    )
    {
        StorPoolDefinitionData storPoolDfn;
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

    public final StorPoolData loadStorPool(
        StorPoolDefinition storPoolDfn,
        Node node,
        boolean failIfNull
    )
    {
        StorPoolData storPool;
        try
        {
            storPool = (StorPoolData) node.getStorPool(peerAccCtx.get(), storPoolDfn.getName());

            if (failIfNull && storPool == null)
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_NOT_FOUND_STOR_POOL_DFN,
                        "Storage pool '" + storPoolDfn.getName().displayValue + "' on node '" +
                            node.getName().displayValue + "' not found.")
                    .setCause("The specified storage pool '" + storPoolDfn.getName().displayValue +
                        "' on node '" + node.getName().displayValue + "' could not be found in the database")
                    .setCorrection("Create a storage pool '" + storPoolDfn.getName().displayValue + "' on node '" +
                        node.getName().displayValue + "' first.")
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

    public final KeyValueStore loadKvs(String kvsNameStr, boolean failIfNull)
    {
        return loadKvs(LinstorParsingUtils.asKvsName(kvsNameStr), failIfNull);
    }

    public final KeyValueStore loadKvs(KeyValueStoreName kvsName, boolean failIfNull)
    {
        KeyValueStore kvs;
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

    public VolumeData loadVlm(
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

    public final ResourceGroupData loadResourceGroup(String rscGrpNameStringRef, boolean failIfNull)
    {
        return loadResourceGroup(
            LinstorParsingUtils.asRscGrpName(rscGrpNameStringRef),
            failIfNull
        );
    }

    private VolumeData loadVlm(
        NodeName nodeNameREf,
        ResourceName rscNameRef,
        VolumeNumber vlmNrRef,
        boolean failIfNullRef
    )
    {
        ResourceData rsc = loadRsc(nodeNameREf, rscNameRef, failIfNullRef);
        Volume vlm = rsc.getVolume(vlmNrRef);
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
        return (VolumeData) vlm;
    }

    public final ResourceGroupData loadResourceGroup(ResourceGroupName rscGrpNameRef, boolean failIfNull)
    {
        ResourceGroupData rscGrp;
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

    public final VolumeGroupData loadVlmGrp(String rscGrpNameStringRef, int vlmNrInt, boolean failIfNull)
    {
        return loadVlmGrp(
            LinstorParsingUtils.asRscGrpName(rscGrpNameStringRef),
            LinstorParsingUtils.asVlmNr(vlmNrInt),
            failIfNull
        );
    }

    public final VolumeGroupData loadVlmGrp(
        ResourceGroupName rscGrpNameRef,
        VolumeNumber vlmNr,
        boolean failIfNull
    )
    {
        VolumeGroupData vlmGrp = null;
        try
        {
            ResourceGroupData rscGrp = loadResourceGroup(rscGrpNameRef, failIfNull);
            if (rscGrp != null)
            {
                vlmGrp = (VolumeGroupData) rscGrp.getVolumeGroup(peerAccCtx.get(), vlmNr);
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
}
