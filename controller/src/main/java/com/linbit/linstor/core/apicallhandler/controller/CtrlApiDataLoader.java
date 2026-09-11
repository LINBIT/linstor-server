package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
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
import com.linbit.linstor.core.objects.NetInterface;
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

import jakarta.inject.Inject;

public class CtrlApiDataLoader
{
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

    public final Node loadNode(String nodeNameStr)
    {
        return loadNode(LinstorParsingUtils.asNodeName(nodeNameStr));
    }

    public final @Nullable Node loadNodeOrNull(String nodeNameStr)
    {
        return loadNodeOrNull(LinstorParsingUtils.asNodeName(nodeNameStr));
    }

    public final Node loadNode(NodeName nodeName)
    {
        return loadNode(nodeName, false);
    }

    public final @Nullable Node loadNodeOrNull(NodeName nodeName)
    {
        return loadNodeOrNull(nodeName, false);
    }

    public final Node loadNode(NodeName nodeName, boolean ignoreSearchDomain)
    {
        @Nullable Node node = loadNodeOrNull(nodeName, ignoreSearchDomain);
        if (node == null)
        {
            // report both the entered name and the one the lookup actually used if a search domain was applied
            NodeName fqdnName = applySearchDomain(nodeName, ignoreSearchDomain);
            String nodeDescription = fqdnName.equals(nodeName) ?
                "'" + nodeName.displayValue + "'" :
                "'" + nodeName.displayValue + "' (expanded to '" + fqdnName.displayValue + "' by the search domain)";
            throw new ApiRcException(ApiCallRcImpl
                .entryBuilder(
                    ApiConsts.FAIL_NOT_FOUND_NODE,
                    "Node " + nodeDescription + " not found."
                )
                .setCause("The specified node " + nodeDescription + " could not be found in the database")
                .setCorrection("Create a node with the name '" + fqdnName.displayValue + "' first.")
                .setSkipErrorReport(true)
                .build()
            );
        }
        return node;
    }

    public final @Nullable Node loadNodeOrNull(NodeName nodeName, boolean ignoreSearchDomain)
    {
        return nodeRepository.get(applySearchDomain(nodeName, ignoreSearchDomain));
    }

    private NodeName applySearchDomain(NodeName nodeName, boolean ignoreSearchDomain)
    {
        NodeName fqdnName = nodeName;
        // if node name is a short name, try to append search domain (if there is any)
        if (!ignoreSearchDomain && !nodeName.getDisplayName().contains("."))
        {
            // TODO: use user properties
            final ReadOnlyProps ctrlProps = systemConfRepository.getCtrlConfForView();
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
        return fqdnName;
    }

    public final NetInterface loadNetIf(String nodeNameStr, String netIfNameStr)
    {
        Node node = loadNode(nodeNameStr);
        @Nullable NetInterface netIf = node.getNetInterface(
            LinstorParsingUtils.asNetInterfaceName(netIfNameStr)
        );

        if (netIf == null)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_NOT_FOUND_NET_IF,
                "Node '" + nodeNameStr + "' has no network interface named '" + netIfNameStr + "'."
            ));
        }
        return netIf;
    }

    public final @Nullable NetInterface loadNetIfOrNull(String nodeNameStr, String netIfNameStr)
    {
        @Nullable Node node = loadNodeOrNull(nodeNameStr);
        @Nullable NetInterface netIf = null;
        if (node != null)
        {
            netIf = node.getNetInterface(
                LinstorParsingUtils.asNetInterfaceName(netIfNameStr)
            );
        }
        return netIf;
    }

    public final ResourceDefinition loadRscDfn(String rscNameStr)
    {
        return loadRscDfn(LinstorParsingUtils.asRscName(rscNameStr));
    }

    public final @Nullable ResourceDefinition loadRscDfnOrNull(String rscNameStr)
    {
        return loadRscDfnOrNull(LinstorParsingUtils.asRscName(rscNameStr));
    }

    public final ResourceDefinition loadRscDfn(ResourceName rscName)
    {
        @Nullable ResourceDefinition rscDfn = loadRscDfnOrNull(rscName);

        if (rscDfn == null)
        {
            throw new ApiRcException(ApiCallRcImpl
                .entryBuilder(
                    ApiConsts.FAIL_NOT_FOUND_RSC_DFN,
                    "Resource definition '" + rscName.displayValue + "' not found."
                )
                .setCause("The specified resource definition '" + rscName.displayValue +
                    "' could not be found in the database")
                .setCorrection("Create a resource definition with the name '" + rscName.displayValue + "' first.")
                .setSkipErrorReport(true)
                .build()
            );
        }

        return rscDfn;
    }

    public final @Nullable ResourceDefinition loadRscDfnOrNull(ResourceName rscName)
    {
        return resourceDefinitionRepository.get(rscName);
    }

    public VolumeDefinition loadVlmDfn(String rscNameStr, int vlmNrInt)
    {
        return loadVlmDfn(LinstorParsingUtils.asRscName(rscNameStr), LinstorParsingUtils.asVlmNr(vlmNrInt));
    }

    public @Nullable VolumeDefinition loadVlmDfnOrNull(String rscNameStr, int vlmNrInt)
    {
        return loadVlmDfnOrNull(LinstorParsingUtils.asRscName(rscNameStr), LinstorParsingUtils.asVlmNr(vlmNrInt));
    }

    public VolumeDefinition loadVlmDfn(ResourceName rscName, VolumeNumber vlmNr)
    {
        ResourceDefinition rscDfn = loadRscDfn(rscName);
        @Nullable VolumeDefinition vlmDfn = rscDfn.getVolumeDfn(vlmNr);

        if (vlmDfn == null)
        {
            throw new ApiRcException(ApiCallRcImpl
                .entryBuilder(
                    ApiConsts.FAIL_NOT_FOUND_VLM_DFN,
                    "Volume definition '" + rscName + "' with volume number '" + vlmNr + "' not found."
                )
                .setCause("The specified volume definition '" + rscName +
                    "' with volume number '" + vlmNr + "' could not be found in the database")
                .setCorrection("Create a volume definition with the name '" + rscName + "' first.")
                .setSkipErrorReport(true)
                .build()
            );
        }
        return vlmDfn;
    }

    public @Nullable VolumeDefinition loadVlmDfnOrNull(ResourceName rscName, VolumeNumber vlmNr)
    {
        @Nullable ResourceDefinition rscDfn = loadRscDfnOrNull(rscName);
        return rscDfn == null ? null : rscDfn.getVolumeDfn(vlmNr);
    }

    public Resource loadRsc(String nodeName, String rscName)
    {
        return loadRsc(LinstorParsingUtils.asNodeName(nodeName), LinstorParsingUtils.asRscName(rscName));
    }

    public @Nullable Resource loadRscOrNull(String nodeName, String rscName)
    {
        return loadRscOrNull(LinstorParsingUtils.asNodeName(nodeName), LinstorParsingUtils.asRscName(rscName));
    }

    public Resource loadRsc(NodeName nodeName, ResourceName rscName)
    {
        Node node = loadNode(nodeName);
        ResourceDefinition rscDfn = loadRscDfn(rscName);
        return loadRsc(rscDfn, node);
    }

    public @Nullable Resource loadRscOrNull(NodeName nodeName, ResourceName rscName)
    {
        @Nullable Resource result = null;
        @Nullable Node node = loadNodeOrNull(nodeName);
        @Nullable ResourceDefinition rscDfn = loadRscDfnOrNull(rscName);
        if (node != null && rscDfn != null)
        {
            result = loadRscOrNull(rscDfn, node);
        }
        return result;
    }

    public Resource loadRsc(ResourceDefinition rscDfn, String nodeNameStr)
    {
        return loadRsc(rscDfn, loadNode(nodeNameStr));
    }

    public @Nullable Resource loadRscOrNull(ResourceDefinition rscDfn, String nodeNameStr)
    {
        @Nullable Node node = loadNodeOrNull(nodeNameStr);
        return node == null ? null : loadRscOrNull(rscDfn, node);
    }

    public Resource loadRsc(ResourceDefinition rscDfn, Node node)
    {
        ResourceName rscName = rscDfn.getName();
        NodeName nodeName = node.getName();
        @Nullable Resource rsc = loadRscOrNull(rscDfn, node);
        if (rsc == null)
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
                .setSkipErrorReport(true)
                .build()
            );
        }
        return rsc;
    }

    public @Nullable Resource loadRscOrNull(ResourceDefinition rscDfn, Node node)
    {
        return node.getResource(rscDfn.getName());
    }

    public @Nullable ResourceConnection loadRscConnOrNull(
        ResourceName rscNameRef,
        NodeName nodeANameRef,
        NodeName nodeBNameRef
    )
    {
        Resource rscA = loadRsc(nodeANameRef, rscNameRef);
        Resource rscB = loadRsc(nodeBNameRef, rscNameRef);
        return rscA.getAbsResourceConnection(rscB);
    }

    public final SnapshotDefinition loadSnapshotDfn(String rscNameStr, String snapshotNameStr)
    {
        return loadSnapshotDfn(
            LinstorParsingUtils.asRscName(rscNameStr),
            LinstorParsingUtils.asSnapshotName(snapshotNameStr)
        );
    }

    public final @Nullable SnapshotDefinition loadSnapshotDfnOrNull(String rscNameStr, String snapshotNameStr)
    {
        return loadSnapshotDfnOrNull(
            LinstorParsingUtils.asRscName(rscNameStr),
            LinstorParsingUtils.asSnapshotName(snapshotNameStr)
        );
    }

    public final SnapshotDefinition loadSnapshotDfn(ResourceName rscName, SnapshotName snapshotName)
    {
        return loadSnapshotDfn(loadRscDfn(rscName), snapshotName);
    }

    public final @Nullable SnapshotDefinition loadSnapshotDfnOrNull(ResourceName rscName, SnapshotName snapshotName)
    {
        @Nullable ResourceDefinition rscDfn = loadRscDfnOrNull(rscName);
        return rscDfn == null ? null : rscDfn.getSnapshotDfn(snapshotName);
    }

    public final SnapshotDefinition loadSnapshotDfn(ResourceDefinition rscDfn, SnapshotName snapshotName)
    {
        @Nullable SnapshotDefinition snapshotDfn = loadSnapshotDfnOrNull(rscDfn, snapshotName);

        if (snapshotDfn == null)
        {
            throw new ApiRcException(ApiCallRcImpl
                .entryBuilder(
                    ApiConsts.FAIL_NOT_FOUND_SNAPSHOT_DFN,
                    "Snapshot '" + snapshotName.displayValue +
                        "' of resource '" + rscDfn.getName().displayValue + "' not found."
                )
                .setSkipErrorReport(true)
                .build()
            );
        }
        return snapshotDfn;
    }

    public final @Nullable SnapshotDefinition loadSnapshotDfnOrNull(
        ResourceDefinition rscDfn,
        SnapshotName snapshotName
    )
    {
        return rscDfn.getSnapshotDfn(snapshotName);
    }

    public Snapshot loadSnapshot(Node node, SnapshotDefinition snapshotDfn)
    {
        @Nullable Snapshot snapshot = snapshotDfn.getSnapshot(node.getName());

        if (snapshot == null)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_NOT_FOUND_SNAPSHOT,
                "Snapshot '" + snapshotDfn.getName() +
                    "' of resource '" + snapshotDfn.getResourceName() +
                    "' on node '" + node.getName() + "' not found."
            ));
        }
        return snapshot;
    }

    public SnapshotVolume loadSnapshotVlm(Snapshot snapshot, VolumeNumber vlmNr)
    {
        @Nullable SnapshotVolume snapshotVolume = snapshot.getVolume(vlmNr);

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

    public final StorPoolDefinition loadStorPoolDfn(String storPoolNameStr)
    {
        return loadStorPoolDfn(LinstorParsingUtils.asStorPoolName(storPoolNameStr));
    }

    public final @Nullable StorPoolDefinition loadStorPoolDfnOrNull(String storPoolNameStr)
    {
        return loadStorPoolDfnOrNull(LinstorParsingUtils.asStorPoolName(storPoolNameStr));
    }

    public final StorPoolDefinition loadStorPoolDfn(StorPoolName storPoolName)
    {
        @Nullable StorPoolDefinition storPoolDfn = loadStorPoolDfnOrNull(storPoolName);

        if (storPoolDfn == null)
        {
            throw new ApiRcException(ApiCallRcImpl
                .entryBuilder(
                    ApiConsts.FAIL_NOT_FOUND_STOR_POOL_DFN,
                    "Storage pool definition '" + storPoolName.displayValue + "' not found."
                )
                .setCause("The specified storage pool definition '" + storPoolName.displayValue +
                    "' could not be found in the database")
                .setCorrection("Create a storage pool definition '" + storPoolName.displayValue + "' first.")
                .setSkipErrorReport(true)
                .build()
            );
        }

        return storPoolDfn;
    }

    public final @Nullable StorPoolDefinition loadStorPoolDfnOrNull(StorPoolName storPoolName)
    {
        return storPoolDefinitionRepository.get(storPoolName);
    }

    public StorPool loadStorPool(String storPoolNameStr, String nodeNameStrRef)
    {
        StorPoolDefinition storPoolDfn = loadStorPoolDfn(storPoolNameStr);
        Node node = loadNode(nodeNameStrRef);
        return loadStorPool(storPoolDfn, node);
    }

    public @Nullable StorPool loadStorPoolOrNull(String storPoolNameStr, String nodeNameStrRef)
    {
        @Nullable StorPoolDefinition storPoolDfn = loadStorPoolDfnOrNull(storPoolNameStr);
        @Nullable Node node = loadNodeOrNull(nodeNameStrRef);

        @Nullable StorPool ret = null;
        if (storPoolDfn != null && node != null)
        {
            ret = loadStorPoolOrNull(storPoolDfn, node);
        }
        return ret;
    }

    public final StorPool loadStorPool(String storPoolNameStr, Node node)
    {
        return loadStorPool(loadStorPoolDfn(storPoolNameStr), node);
    }

    public final @Nullable StorPool loadStorPoolOrNull(String storPoolNameStr, Node node)
    {
        @Nullable StorPoolDefinition storPoolDfn = loadStorPoolDfnOrNull(storPoolNameStr);
        return storPoolDfn == null ? null : loadStorPoolOrNull(storPoolDfn, node);
    }

    public final StorPool loadStorPool(StorPoolDefinition storPoolDfn, Node node)
    {
        @Nullable StorPool storPool = loadStorPoolOrNull(storPoolDfn, node);

        if (storPool == null)
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
                    .setSkipErrorReport(true)
                    .build()
            );
        }
        return storPool;
    }

    public final @Nullable StorPool loadStorPoolOrNull(StorPoolDefinition storPoolDfn, Node node)
    {
        return node.getStorPool(storPoolDfn.getName());
    }

    public final KeyValueStore loadKvs(String kvsNameStr)
    {
        return loadKvs(LinstorParsingUtils.asKvsName(kvsNameStr));
    }

    public final @Nullable KeyValueStore loadKvsOrNull(String kvsNameStr)
    {
        return loadKvsOrNull(LinstorParsingUtils.asKvsName(kvsNameStr));
    }

    public final KeyValueStore loadKvs(KeyValueStoreName kvsName)
    {
        @Nullable KeyValueStore kvs = loadKvsOrNull(kvsName);

        if (kvs == null)
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
                .setSkipErrorReport(true)
                .build()
            );
        }
        return kvs;
    }

    public final @Nullable KeyValueStore loadKvsOrNull(KeyValueStoreName kvsName)
    {
        return kvsRepository.get(kvsName);
    }

    public Volume loadVlm(String nodeNameStrRef, String rscNameStrRef, Integer vlmNrIntRef)
    {
        return loadVlm(
            LinstorParsingUtils.asNodeName(nodeNameStrRef),
            LinstorParsingUtils.asRscName(rscNameStrRef),
            LinstorParsingUtils.asVlmNr(vlmNrIntRef)
        );
    }

    public @Nullable Volume loadVlmOrNull(String nodeNameStrRef, String rscNameStrRef, Integer vlmNrIntRef)
    {
        return loadVlmOrNull(
            LinstorParsingUtils.asNodeName(nodeNameStrRef),
            LinstorParsingUtils.asRscName(rscNameStrRef),
            LinstorParsingUtils.asVlmNr(vlmNrIntRef)
        );
    }

    private Volume loadVlm(NodeName nodeNameRef, ResourceName rscNameRef, VolumeNumber vlmNrRef)
    {
        Resource rsc = loadRsc(nodeNameRef, rscNameRef);
        @Nullable Volume vlm = rsc.getVolume(vlmNrRef);
        if (vlm == null)
        {
            throw new ApiRcException(ApiCallRcImpl
                .entryBuilder(
                    ApiConsts.FAIL_NOT_FOUND_VLM,
                    CtrlVlmApiCallHandler.getVlmDescription(nodeNameRef, rscNameRef, vlmNrRef) + " not found."
                )
                .build()
            );
        }
        return vlm;
    }

    private @Nullable Volume loadVlmOrNull(NodeName nodeNameRef, ResourceName rscNameRef, VolumeNumber vlmNrRef)
    {
        @Nullable Resource rsc = loadRscOrNull(nodeNameRef, rscNameRef);
        return rsc == null ? null : rsc.getVolume(vlmNrRef);
    }

    public final ResourceGroup loadResourceGroup(String rscGrpNameStringRef)
    {
        return loadResourceGroup(LinstorParsingUtils.asRscGrpName(rscGrpNameStringRef));
    }

    public final @Nullable ResourceGroup loadResourceGroupOrNull(String rscGrpNameStringRef)
    {
        return loadResourceGroupOrNull(LinstorParsingUtils.asRscGrpName(rscGrpNameStringRef));
    }

    public final ResourceGroup loadResourceGroup(ResourceGroupName rscGrpNameRef)
    {
        @Nullable ResourceGroup rscGrp = loadResourceGroupOrNull(rscGrpNameRef);
        if (rscGrp == null)
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
        return rscGrp;
    }

    public final @Nullable ResourceGroup loadResourceGroupOrNull(ResourceGroupName rscGrpNameRef)
    {
        return resourceGroupRepository.get(rscGrpNameRef);
    }

    public final VolumeGroup loadVlmGrp(String rscGrpNameStringRef, int vlmNrInt)
    {
        return loadVlmGrp(LinstorParsingUtils.asRscGrpName(rscGrpNameStringRef), LinstorParsingUtils.asVlmNr(vlmNrInt));
    }

    public final @Nullable VolumeGroup loadVlmGrpOrNull(String rscGrpNameStringRef, int vlmNrInt)
    {
        return loadVlmGrpOrNull(
            LinstorParsingUtils.asRscGrpName(rscGrpNameStringRef),
            LinstorParsingUtils.asVlmNr(vlmNrInt)
        );
    }

    public final VolumeGroup loadVlmGrp(ResourceGroupName rscGrpNameRef, VolumeNumber vlmNr)
    {
        ResourceGroup rscGrp = loadResourceGroup(rscGrpNameRef);
        @Nullable VolumeGroup vlmGrp = rscGrp.getVolumeGroup(vlmNr);
        if (vlmGrp == null)
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
                .setSkipErrorReport(true)
                .build()
            );
        }
        return vlmGrp;
    }

    public final @Nullable VolumeGroup loadVlmGrpOrNull(ResourceGroupName rscGrpNameRef, VolumeNumber vlmNr)
    {
        @Nullable ResourceGroup rscGrp = loadResourceGroupOrNull(rscGrpNameRef);
        return rscGrp == null ? null : rscGrp.getVolumeGroup(vlmNr);
    }

    public final ExternalFile loadExtFile(String extFileNameStr)
    {
        return loadExtFile(LinstorParsingUtils.asExtFileName(extFileNameStr));
    }

    public final @Nullable ExternalFile loadExtFileOrNull(String extFileNameStr)
    {
        return loadExtFileOrNull(LinstorParsingUtils.asExtFileName(extFileNameStr));
    }

    public final ExternalFile loadExtFile(ExternalFileName extFileName)
    {
        @Nullable ExternalFile extFile = loadExtFileOrNull(extFileName);

        if (extFile == null)
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
                .setSkipErrorReport(true)
                .build()
            );
        }
        return extFile;
    }

    public final @Nullable ExternalFile loadExtFileOrNull(ExternalFileName extFileName)
    {
        return extFileRepository.get(extFileName);
    }

    public final AbsRemote loadRemote(String remoteNameStr)
    {
        return loadRemote(LinstorParsingUtils.asRemoteName(remoteNameStr));
    }

    public final @Nullable AbsRemote loadRemoteOrNull(String remoteNameStr)
    {
        return loadRemoteOrNull(LinstorParsingUtils.asRemoteName(remoteNameStr));
    }

    public final AbsRemote loadRemote(RemoteName remoteName)
    {
        @Nullable AbsRemote remote = loadRemoteOrNull(remoteName);

        if (remote == null)
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
                    .setSkipErrorReport(true)
                    .build()
            );
        }
        return remote;
    }

    public final @Nullable AbsRemote loadRemoteOrNull(RemoteName remoteName)
    {
        return remoteRepository.get(remoteName);
    }

    public final Schedule loadSchedule(String scheduleNameStr)
    {
        return loadSchedule(LinstorParsingUtils.asScheduleName(scheduleNameStr));
    }

    public final @Nullable Schedule loadScheduleOrNull(String scheduleNameStr)
    {
        return loadScheduleOrNull(LinstorParsingUtils.asScheduleName(scheduleNameStr));
    }

    public final Schedule loadSchedule(ScheduleName scheduleName)
    {
        @Nullable Schedule schedule = loadScheduleOrNull(scheduleName);

        if (schedule == null)
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
                    .setSkipErrorReport(true)
                    .build()
            );
        }
        return schedule;
    }

    public final @Nullable Schedule loadScheduleOrNull(ScheduleName scheduleName)
    {
        return scheduleRepository.get(scheduleName);
    }
}
