package com.linbit.linstor.core.apicallhandler.controller.autoplacer;

import com.linbit.ImplementationError;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.Autoplacer.StorPoolWithScore;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.storage.kinds.ExtToolsInfo.Version;
import com.linbit.linstor.storage.utils.LayerUtils;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

@Singleton
class Selector
{
    private final AccessContext apiCtx;
    private final ErrorReporter errorReporter;
    private final SystemConfRepository sysCfgRepo;

    @Inject
    Selector(@SystemContext AccessContext apiCtxRef, ErrorReporter errorReporterRef, SystemConfRepository sysCfgRepoRef)
    {
        apiCtx = apiCtxRef;
        errorReporter = errorReporterRef;
        sysCfgRepo = sysCfgRepoRef;
    }

    public Set<StorPoolWithScore> select(
        AutoSelectFilterApi selectFilterRef,
        @Nullable ResourceDefinition rscDfnRef,
        Collection<StorPoolWithScore> storPoolWithScores
    )
        throws AccessDeniedException
    {
        StorPoolWithScore[] sortedStorPoolByScoreArr = storPoolWithScores.toArray(new StorPoolWithScore[0]);
        Arrays.sort(sortedStorPoolByScoreArr);

        for (StorPoolWithScore storPoolWithScore : sortedStorPoolByScoreArr)
        {
            errorReporter.logTrace(
                "Autoplacer.Selector: Score: %f, Storage pool '%s' on node '%s'",
                storPoolWithScore.score,
                storPoolWithScore.storPool.getName().displayValue,
                storPoolWithScore.storPool.getNode().getName().displayValue
            );
        }

        List<Node> alreadyDeployedOnNodes = new ArrayList<>();
        int alreadyDeployedDiskfulCount = 0;
        int alreadyDeployedDisklessCount = 0;
        List<SharedStorPoolName> alreadyDeployedInSharedSPNames = new ArrayList<>();
        Map<DeviceProviderKind, List</* DrbdVersion */Version>> alreadyDeployedKindsAndVersion = new HashMap<>();

        // if rscDfn is null, we are most likely in something like query-size-info
        boolean allowMixing = rscDfnRef == null || isStorPoolMixingEnabled(rscDfnRef);
        if (rscDfnRef != null)
        {
            Iterator<Resource> rscIt = rscDfnRef.iterateResource(apiCtx);
            List<String> nodeStrList = new ArrayList<>();
            while (rscIt.hasNext())
            {
                Resource rsc = rscIt.next();
                StateFlags<Flags> rscFlags = rsc.getStateFlags();
                boolean isRscDeleting = rscFlags.isSet(apiCtx, Resource.Flags.DELETE);
                boolean isRscEvicted = rscFlags.isSet(apiCtx, Resource.Flags.EVICTED);
                if (!isRscDeleting && !isRscEvicted)
                {
                    Node node = rsc.getNode();

                    List<String> skipAlreadyPlacedOnNodeNamesCheck =
                        selectFilterRef.skipAlreadyPlacedOnNodeNamesCheck();
                    Boolean skipAlreadyPlacedOnAllNodesCheck = selectFilterRef.skipAlreadyPlacedOnAllNodeCheck();

                    boolean countResource = skipAlreadyPlacedOnAllNodesCheck == null ||
                        !skipAlreadyPlacedOnAllNodesCheck;
                    countResource &= skipAlreadyPlacedOnNodeNamesCheck == null ||
                        !skipAlreadyPlacedOnNodeNamesCheck.contains(node.getName().displayValue);

                    countResource &= !(rscFlags.isSet(apiCtx, Resource.Flags.EVACUATE) ||
                        node.getFlags().isSet(apiCtx, Node.Flags.EVACUATE));

                    if (countResource)
                    {
                        alreadyDeployedOnNodes.add(node);
                        if (rscFlags.isSet(apiCtx, Resource.Flags.DISKLESS))
                        {
                            alreadyDeployedDisklessCount++;
                        }
                        else
                        {
                            alreadyDeployedDiskfulCount++;
                        }
                    }

                    nodeStrList.add(node.getName().displayValue);

                    // determine already selected provider kind
                    List<AbsRscLayerObject<Resource>> storageRscDataList = LayerUtils.getChildLayerDataByKind(
                        rsc.getLayerData(apiCtx),
                        DeviceLayerKind.STORAGE
                    );
                    // might be possible to skip this loop if rsc to be placed is diskless
                    for (AbsRscLayerObject<Resource> storageRscData : storageRscDataList)
                    {
                        if (storageRscData.getResourceNameSuffix().equals(RscLayerSuffixes.SUFFIX_DATA))
                        {
                            for (
                                VlmProviderObject<Resource> storageVlmData : storageRscData.getVlmLayerObjects()
                                    .values()
                            )
                            {
                                StorPool sp = storageVlmData.getStorPool();
                                alreadyDeployedInSharedSPNames.add(sp.getSharedStorPoolName());

                                DeviceProviderKind storageVlmProviderKind = sp.getDeviceProviderKind();
                                ExtToolsInfo drbdInfo = sp.getNode()
                                    .getPeer(apiCtx)
                                    .getExtToolsManager()
                                    .getExtToolInfo(ExtTools.DRBD9_KERNEL);
                                Version storageVlmDrbdVersion = drbdInfo == null ? null : drbdInfo.getVersion();
                                if (!storageVlmProviderKind.equals(DeviceProviderKind.DISKLESS))
                                {
                                    Set<Entry<DeviceProviderKind, List</* DrbdVersion */ Version>>> entrySet = alreadyDeployedKindsAndVersion
                                        .entrySet();
                                    for (Entry<DeviceProviderKind, List<Version>> entry : entrySet)
                                    {
                                        DeviceProviderKind alreadyDeloyedKind = entry.getKey();
                                        for (Version alreadyDeloyedDrbdVersion : entry.getValue())
                                        {

                                            boolean isMixingAllowed = DeviceProviderKind.isMixingAllowed(
                                                alreadyDeloyedKind,
                                                alreadyDeloyedDrbdVersion,
                                                storageVlmProviderKind,
                                                storageVlmDrbdVersion,
                                                allowMixing
                                            );

                                            if (!isMixingAllowed)
                                            {
                                                throw new ImplementationError(
                                                    "Multiple deployed provider kinds found for: " +
                                                        rsc
                                                );
                                            }
                                        }

                                    }
                                    alreadyDeployedKindsAndVersion.computeIfAbsent(
                                        storageVlmProviderKind,
                                        ignored -> new ArrayList<>()
                                    ).add(storageVlmDrbdVersion);
                                }
                            }
                        }
                    }
                }
            }
            if (alreadyDeployedOnNodes.isEmpty())
            {
                errorReporter.logTrace(
                    "Autoplacer.Selector: Resource '%s' not deployed yet.",
                    rscDfnRef.getName().displayValue
                );
            }
            else
            {
                errorReporter.logTrace(
                    "Autoplacer.Selector: Resource '%s' already deployed on nodes: %s",
                    rscDfnRef.getName().displayValue,
                    nodeStrList.toString()
                );
            }
        }

        Set<StorPoolWithScore> selectionResult = null;

        Set<StorPoolWithScore> currentSelection;
        int startIdx = 0;
        double selectionScore = Double.NEGATIVE_INFINITY;
        boolean keepSearchingForCandidates = true;

        SelectionManager selectionManager = new SelectionManager(
            apiCtx,
            errorReporter,
            selectFilterRef,
            alreadyDeployedOnNodes,
            alreadyDeployedDiskfulCount,
            alreadyDeployedDisklessCount,
            alreadyDeployedInSharedSPNames,
            alreadyDeployedKindsAndVersion,
            sortedStorPoolByScoreArr,
            allowMixing
        );
        final int additionalReplicaCount = selectionManager.getAdditionalRscCountToSelect();
        errorReporter.logTrace(
            "Autoplacer.Selector: Starting selection for %d additional resources",
            additionalReplicaCount
        );
        do
        {
            currentSelection = selectionManager.findSelection(startIdx);
            if (currentSelection.isEmpty())
            {
                keepSearchingForCandidates = false;
            }
            else if (currentSelection.size() == additionalReplicaCount)
            {
                double currentScore = 0;

                StringBuilder storPoolDescrForLog = new StringBuilder();
                for (StorPoolWithScore spWithScore : currentSelection)
                {
                    currentScore += spWithScore.score;
                    storPoolDescrForLog.append("StorPool '")
                        .append(spWithScore.storPool.getName().displayValue)
                        .append("' on node '")
                        .append(spWithScore.storPool.getNode().getName().displayValue)
                        .append("' with score: ")
                        .append(spWithScore.score)
                        .append(", ");
                }
                storPoolDescrForLog.setLength(storPoolDescrForLog.length() - 2);

                if (currentScore > selectionScore)
                {
                    selectionResult = currentSelection;
                    selectionScore = currentScore;
                    errorReporter.logTrace(
                        "Autoplacer.Selector: Found candidate %s with accumulated score %f",
                        storPoolDescrForLog.toString(),
                        selectionScore
                    );
                }
                else
                {
                    errorReporter.logTrace(
                        "Autoplacer.Selector: Skipping candidate %s as its accumulated score %f is lower than " +
                            "currently best candidate's %f",
                        storPoolDescrForLog.toString(),
                        currentScore,
                        selectionScore
                    );
                }
                startIdx++;
                if (startIdx <= sortedStorPoolByScoreArr.length - additionalReplicaCount)
                {
                    double nextHighestPossibleScore = 0;
                    for (int idx = 0; idx < additionalReplicaCount; idx++)
                    {
                        /*
                         * we ignore here all filters and node-assignments, etc... we just want to
                         * verify if we should be keep searching for better candidates or not
                         */
                        nextHighestPossibleScore += sortedStorPoolByScoreArr[idx + startIdx].score;
                    }
                    keepSearchingForCandidates = nextHighestPossibleScore > selectionScore;
                    if (!keepSearchingForCandidates)
                    {
                        errorReporter.logTrace(
                            "Autoplacer.Selector: Remaining candidates-combinations cannot have higher score then " +
                                "the currently chosen one. Search finished."
                        );
                    }
                    else
                    {
                        // continue the search, reset temporary maps
                        errorReporter.logTrace(
                            "Autoplacer.Selector: Continuing search for better candidates."
                        );
                    }
                }
                else
                {
                    keepSearchingForCandidates = false;
                    errorReporter.logTrace(
                        "Autoplacer.Selector: Not enough remaining storage pools left. Search finished."
                    );
                }

            }
            else
            {
                errorReporter.logTrace("Autoplacer.Selector: no more candidates found");
            }
        }
        while (currentSelection.size() == additionalReplicaCount && keepSearchingForCandidates);

        return selectionResult;
    }


    private boolean isStorPoolMixingEnabled(ResourceDefinition rscDfnRef)
    {
        PriorityProps prioProps;
        try
        {
            prioProps = new PriorityProps(
                rscDfnRef.getProps(apiCtx),
                rscDfnRef.getResourceGroup().getProps(apiCtx),
                sysCfgRepo.getCtrlConfForView(apiCtx)
            );
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return "true".equalsIgnoreCase(
            prioProps.getProp(ApiConsts.KEY_RSC_ALLOW_MIXING_DEVICE_KIND, null, "false")
        );
    }

    public @Nullable Node unselect(
        ResourceDefinition rscDfn,
        List<Node> fixedNodes,
        StorPoolWithScore[] sortedStorPoolByScoreArr,
        boolean selectDiskfulNodeRef
    )
        throws AccessDeniedException
    {
        @Nullable Node ret = null;

        Iterator<Resource> rscIt = rscDfn.iterateResource(apiCtx);
        Set<Resource> candidatesToUnselect = new HashSet<>();
        while (rscIt.hasNext())
        {
            Resource rsc = rscIt.next();
            Node node = rsc.getNode();

            if (!fixedNodes.contains(node))
            {
                boolean isDiskless = rsc.isDiskless(apiCtx);
                if (isDiskless && !selectDiskfulNodeRef || !isDiskless && selectDiskfulNodeRef)
                {
                    candidatesToUnselect.add(rsc);
                }
            }
        }


        if (ret == null)
        {
            if (sortedStorPoolByScoreArr.length > 0)
            {
                // sorts highest to lowest as defined in StorPoolWithScore' Comparator
                Arrays.sort(sortedStorPoolByScoreArr);

                // return node of storage pool with lowest score
                ret = sortedStorPoolByScoreArr[sortedStorPoolByScoreArr.length - 1].storPool.getNode();
            }
        }
        return ret;
    }
}
