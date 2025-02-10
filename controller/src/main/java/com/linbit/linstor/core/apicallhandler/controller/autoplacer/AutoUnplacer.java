package com.linbit.linstor.core.apicallhandler.controller.autoplacer;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.Autoplacer.StorPoolWithScore;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.AutoSelectorConfig;
import com.linbit.linstor.core.objects.AutoUnselectorConfig;
import com.linbit.linstor.core.objects.AutoUnselectorConfig.AutoUnselectRscConfig;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.linstor.utils.layer.LayerVlmUtils;
import com.linbit.utils.StringUtils;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;

@Singleton
public class AutoUnplacer
{
    private final AccessContext apiAccCtx;
    private final ErrorReporter errorReporter;
    private final StrategyHandler strategyHandler;

    @Inject
    public AutoUnplacer(
        @SystemContext AccessContext apiAccCtxRef,
        ErrorReporter errorReporterRef,
        StrategyHandler strategyHandlerRef
    )
    {
        apiAccCtx = apiAccCtxRef;
        errorReporter = errorReporterRef;
        strategyHandler = strategyHandlerRef;
    }

    public @Nullable Resource unplace(ResourceDefinition rscDfnRef, Collection<Resource> fixedResources)
        throws AccessDeniedException
    {
        return unplace(
            new AutoUnselectorConfig.CfgBuilder(rscDfnRef)
                .setFilterForFixedResources(fixedResources)
                .build(apiAccCtx)
        );
    }

    @SuppressWarnings("checkstyle:VariableDeclarationUsageDistance")
    public @Nullable Resource unplace(AutoUnselectorConfig cfgRef)
    {
        @Nullable Resource ret;

        // the rough idea here is to iterate through all resources and check every resource against all other (after
        // filtering diskful vs diskless). if two resources have are violating something (i.e. --replicas-on-same but
        // the two resources have different values or --replicas-on-different but the two resources have the same value,
        // etc..), add some "unselection-score" to both resources.
        // if we have "--replicas-on-same site", 2 resources are being on site "a", and the third resource is on site
        // "b" and the "unselection-score" for violating the --replicas-on-same would be lets say 10, the first two
        // resources would end up having a score of 10, while the third resource has a score of 20.

        // in the end, select the X resources with the highest score (X being the number of resources we should
        // unselect)

        try
        {
            ResourceDefinition rscDfn = cfgRef.getRscDfn();
            errorReporter.logTrace("AutoUnplacer: Finding resource to unplace for '%s'", rscDfn.getName().displayValue);

            Map<Resource, Long> unplaceScore = initializeUnplaceScoreMap(cfgRef, rscDfn);

            if (!unplaceScore.isEmpty())
            {
                AutoSelectorConfig autoPlaceConfig = rscDfn.getResourceGroup().getAutoPlaceConfig();

                @Nullable List<String> replicasOnSame = autoPlaceConfig.getReplicasOnSameList(apiAccCtx);
                @Nullable List<String> replicasOnDifferent = autoPlaceConfig.getReplicasOnDifferentList(apiAccCtx);
                @Nullable Map<String, Integer> xReplicasOnDifferent = autoPlaceConfig.getXReplicasOnDifferentMap(
                    apiAccCtx
                );
                @Nullable List<String> doNotPlaceWithRscList = autoPlaceConfig.getDoNotPlaceWithRscList(apiAccCtx);
                @Nullable String doNotPlaceWithRscRegex = autoPlaceConfig.getDoNotPlaceWithRscRegex(apiAccCtx);
                // looks weird, but just in case someone changed RG's preferred layer stack, LINSTOR could focus on
                // getting
                // rid of the resource stack with the older layer-stack
                @Nullable List<DeviceLayerKind> layerStackList = autoPlaceConfig.getLayerStackList(apiAccCtx);
                @Nullable List<String> nodeNameList = autoPlaceConfig.getNodeNameList(apiAccCtx);
                @Nullable List<DeviceProviderKind> providerList = autoPlaceConfig.getProviderList(apiAccCtx);
                @Nullable List<String> storPoolNameList = autoPlaceConfig.getStorPoolNameList(apiAccCtx);

                Iterator<Resource> rscIt = rscDfn.iterateResource(apiAccCtx);

                List<Node> alreadyDeployedNodes = new ArrayList<>();
                while (rscIt.hasNext())
                {
                    Resource rsc = rscIt.next();
                    alreadyDeployedNodes.add(rsc.getNode());
                }

                HashMap<String, Map<String, Integer>> xReplicasOnDiffMapWithValues = SelectionManager
                    .calcCurrentXReplicasOnDiffMap(
                        apiAccCtx,
                        xReplicasOnDifferent,
                        replicasOnDifferent,
                        alreadyDeployedNodes,
                        null,
                        true
                    );

                final Resource[] rscArr = unplaceScore.keySet().toArray(size -> new Resource[size]);
                for (int idx1 = 0; idx1 < rscArr.length; idx1++)
                {
                    final Resource rsc1 = rscArr[idx1];
                    final Node node1 = rsc1.getNode();
                    final ReadOnlyProps node1Props = node1.getReadOnlyProps(apiAccCtx);

                    int soloRating = 0;

                    errorReporter.logTrace("AutoUnplacer: Checking violation count for   '%s'", rsc1);

                    soloRating += getViolationsCountDoNotPlaceWith(
                        doNotPlaceWithRscList,
                        doNotPlaceWithRscRegex,
                        node1
                    );
                    soloRating += getViolationsCountLayerStack(layerStackList, rsc1);
                    soloRating += getViolationsCountNodeNameList(nodeNameList, rsc1);
                    soloRating += getViolationsCounthandleProvider(providerList, rsc1);
                    soloRating += getViolationsCountStorPoolName(storPoolNameList, rsc1);
                    soloRating += getViolationsCountReplicasOnDifferentWithValueList(replicasOnDifferent, node1Props);
                    soloRating += getViolationsCountReplicasOnSameWithValueList(replicasOnSame, node1Props);
                    soloRating += getViolationsCountXReplicasOnDifferentMap(xReplicasOnDiffMapWithValues, node1Props);

                    // TODO: maybe add some weights for the different violation types?
                    // TODO: make those weights configurable

                    add(unplaceScore, rsc1, soloRating);

                    for (int idx2 = idx1 + 1; idx2 < rscArr.length; idx2++)
                    {
                        final Resource rsc2 = rscArr[idx2];
                        final Node node2 = rsc2.getNode();
                        final ReadOnlyProps node2Props = node2.getReadOnlyProps(apiAccCtx);
                        int pairRating = 0;

                        pairRating += getViolationsCountReplicasOnSameList(
                            replicasOnSame,
                            node1Props,
                            node2Props
                        );

                        add(unplaceScore, rsc1, pairRating);
                        add(unplaceScore, rsc2, pairRating);
                    }
                    long violationScore = unplaceScore.get(rsc1);
                    errorReporter.logTrace(
                        "AutoUnplacer: Score of %d from violations for '%s'",
                        violationScore,
                        rsc1
                    );
                }


                Set<Resource> highestRatedResources = findResourceWithHighestScore(unplaceScore);
                // currently we only want to return a single resource since deleting that resource could change some
                // violations, for example XReplicasOnDifferent might change from a -1 value to a 0, which is allowed.
                // the -1 would have caused multiple resources to be deleted. if that resource is deleted other
                // violations might become more significant

                if (highestRatedResources.size() > 1)
                {
                    Map<Resource, Double> spRatingByResource = getStorPoolRatingByResource(highestRatedResources);

                    Entry<Resource, Double> firstEntry = spRatingByResource.entrySet().iterator().next();

                    Resource lowestSpRatedRsc = firstEntry.getKey();
                    double lowestSpRating = firstEntry.getValue();
                    for (Map.Entry<Resource, Double> entry : spRatingByResource.entrySet())
                    {
                        double rating = entry.getValue();
                        if (lowestSpRating > rating)
                        {
                            lowestSpRating = rating;
                            lowestSpRatedRsc = entry.getKey();
                        }
                    }
                    ret = lowestSpRatedRsc;
                }
                else
                {
                    ret = highestRatedResources.iterator().next();
                }
            }
            else
            {
                // no resources to delete since all are in a state that does not allow unplacing...
                ret = null;
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }

        if (ret != null)
        {
            errorReporter.logTrace("AutoUnplacer: Selected resource: %s", ret);
        }
        else
        {
            errorReporter.logTrace("AutoUnplacer: Selection ended with no result");
        }
        return ret;
    }

    private Map<Resource, Double> getStorPoolRatingByResource(Set<Resource> highestRatedResourcesRef)
        throws AccessDeniedException
    {
        Set<StorPool> allStorPoolList = new HashSet<>();
        Map<StorPool, Resource> spToRscLut = new HashMap<>();
        for (Resource rsc : highestRatedResourcesRef)
        {
            Set<StorPool> storPools = LayerVlmUtils.getStorPools(rsc, apiAccCtx);
            for (StorPool sp : storPools)
            {
                spToRscLut.put(sp, rsc);
                allStorPoolList.add(sp);
            }
        }
        Collection<StorPoolWithScore> storPoolRatings = strategyHandler.rate(allStorPoolList, "AutoUnplacer");

        // should stay TreeMap to ensure that with a draw in score, the result stays deterministic.
        Map<Resource, Double> ret = new TreeMap<>();
        for (StorPoolWithScore spws : storPoolRatings)
        {
            @Nullable Resource rsc = spToRscLut.get(spws.storPool);
            if (rsc == null)
            {
                throw new ImplementationError("Unknown storage pool: " + spws.storPool);
            }
            @Nullable Double value = ret.get(rsc);
            if (value == null)
            {
                value = 0.0;
            }
            value += spws.score;
            ret.put(rsc, value);
        }
        return ret;
    }

    private Map<NodeName, Double> getStorPoolRatingByNodeName(Resource[] rscArrRef) throws AccessDeniedException
    {
        Set<StorPool> allStorPoolList = new HashSet<>();
        for (Resource rsc : rscArrRef)
        {
            allStorPoolList.addAll(LayerVlmUtils.getStorPools(rsc, apiAccCtx));
        }
        Collection<StorPoolWithScore> storPoolRatings = strategyHandler.rate(allStorPoolList, "AutoUnplacer");
        Map<NodeName, Double> ret = new TreeMap<>();
        for (StorPoolWithScore spws : storPoolRatings)
        {
            NodeName nodeName = spws.storPool.getNode().getName();
            @Nullable Double value = ret.get(nodeName);
            if (value == null)
            {
                value = 0.0;
            }
            value += spws.score;
            ret.put(nodeName, value);
        }
        return ret;
    }

    private Map<Resource, Double> normalizeUnplaceScore(Map<Resource, Long> unplaceScoreRef)
    {
        Map<Resource, Double> ret = new TreeMap<>();

        long highestViolationScore = Long.MIN_VALUE;
        for (long val : unplaceScoreRef.values())
        {
            if (highestViolationScore < val)
            {
                highestViolationScore = val;
            }
        }

        final double highestViolationScoreDouble = highestViolationScore;
        for (Map.Entry<Resource, Long> entry : unplaceScoreRef.entrySet())
        {
            ret.put(entry.getKey(), ((double) entry.getValue()) / highestViolationScoreDouble);
        }

        return ret;
    }

    private void addScores(Map<Resource, Double> rscScoresRef, Map<NodeName, Double> spRatingByNodeRef)
    {
        for (Map.Entry<Resource, Double> entry : rscScoresRef.entrySet())
        {
            Resource rsc = entry.getKey();
            rscScoresRef.put(
                rsc,
                entry.getValue() + spRatingByNodeRef.get(rsc.getNode().getName())
            );
        }
    }

    private Set<Resource> findResourceWithHighestScore(Map<Resource, Long> unplaceScoreRef)
    {
        long highestScore = Long.MIN_VALUE;
        Set<Resource> rscsWithHighestScore = new TreeSet<>();
        errorReporter.logTrace("AutoUnplacer: Final scores:");
        for (Map.Entry<Resource, Long> entry : unplaceScoreRef.entrySet())
        {
            long value = entry.getValue();
            Resource rsc = entry.getKey();
            errorReporter.logTrace("AutoUnplacer:  %20s: %d", rsc, value);
            if (highestScore < value)
            {
                highestScore = value;
                rscsWithHighestScore.clear();
            }
            rscsWithHighestScore.add(rsc);
        }

        return rscsWithHighestScore;
    }


    private Map<Resource, Long> initializeUnplaceScoreMap(AutoUnselectorConfig cfgRef, ResourceDefinition rscDfn)
        throws AccessDeniedException
    {
        Iterator<Resource> rscIt = rscDfn.iterateResource(apiAccCtx);

        Map<Resource, Long> unplaceRate = new TreeMap<>();
        while (rscIt.hasNext())
        {
            Resource rsc = rscIt.next();

            StateFlags<Flags> rscFlags = rsc.getStateFlags();
            if (rscFlags.isSomeSet(apiAccCtx, Resource.Flags.DELETE, Resource.Flags.DRBD_DELETE))
            {
                errorReporter.logTrace(
                    "AutoUnplacer: Not unplacing resource on node '%s': Resource is marked for deletion",
                    rsc.getNode().getName().displayValue
                );
            }

            AutoUnselectRscConfig rscCfg = cfgRef.getBy(rsc);
            @Nullable String ignoreReason = rscCfg.getIgnoreReason();
            if (ignoreReason != null)
            {
                errorReporter.logTrace(
                    "AutoUnplacer: Not unplacing resource on node '%s': %s",
                    rsc.getNode().getName().displayValue,
                    ignoreReason
                );
            }
            else
            {
                unplaceRate.put(rsc, 0L);
            }
        }
        return unplaceRate;
    }

    private int getViolationsCountReplicasOnGenericWithValueList(
        @Nullable List<String> replicasOnGenericRef,
        ReadOnlyProps nodePropsRef,
        boolean countIfValuesEqualRef,
        String logDescriptionRef
    )
    {
        int ret = 0;
        if (replicasOnGenericRef != null && !replicasOnGenericRef.isEmpty())
        {
            for (String keyAndMaybeValue : replicasOnGenericRef)
            {
                String[] keyValue = keyAndMaybeValue.split("=", 2);
                if (keyValue.length == 2)
                {
                    // otherwise there is no specific value to check against
                    String key = keyValue[0];
                    String value = keyValue[1];

                    @Nullable String propValue = nodePropsRef.getProp(key);
                    /*
                     * Used by:
                     * * replicasOnDifferent (countIfValuesEqual = true)
                     * _ _ counts violations if value.equals(propValue)
                     * * replicasOnSame (countIfValuesEqual = false)
                     * _ _ counts violations if !value.equals(propValue)
                     */
                    if (countIfValuesEqualRef == value.equals(propValue))
                    {
                        errorReporter.logTrace(
                            "AutoUnplacer:   %s: Property '%s' %s expected to have value '%s', but has '%s'",
                            key,
                            countIfValuesEqualRef ? "is" : "is not",
                            value,
                            propValue
                        );
                        ret++;
                    }
                }
            }
            logViolationCount(logDescriptionRef, ret);
        }
        return ret;
    }

    /**
     * This method only checks the replicas-on-different settings which have a fixed value (i.e. "site=B" instead of
     * just "site"). {@link #getViolationsCountXReplicasOnDifferentMap(HashMap, ReadOnlyProps)} checks for settings
     * with open values (i.e. "site" without "=B")
     *
     * @param replicasOnDifferentRef
     * @param nodePropsRef
     * @return
     */
    private int getViolationsCountReplicasOnDifferentWithValueList(
        @Nullable List<String> replicasOnDifferentRef,
        ReadOnlyProps nodePropsRef
    )
    {
        return getViolationsCountReplicasOnGenericWithValueList(
            replicasOnDifferentRef,
            nodePropsRef,
            true,
            "ReplicasOnDifferent (fixed value)"
        );
    }

    /**
     * This method only checks the replicas-on-same settings which have a fixed value (i.e. "site=B" instead of
     * just "site"). {@link #getViolationsCountReplicasOnSameList(List, ReadOnlyProps, ReadOnlyProps)} checks for
     * settings with open values (i.e. "site" without "=B")
     *
     * @param replicasOnDifferentRef
     * @param nodePropsRef
     * @return
     */
    private int getViolationsCountReplicasOnSameWithValueList(
        @Nullable List<String> replicasOnSameRef,
        ReadOnlyProps nodePropsRef
    )
    {
        return getViolationsCountReplicasOnGenericWithValueList(
            replicasOnSameRef,
            nodePropsRef,
            false,
            "ReplicasOnSame (fixed value)"
        );
    }

    private int getViolationsCountXReplicasOnDifferentMap(
        HashMap<String, Map<String, Integer>> xReplicasOnDiffMapWithValuesRef,
        ReadOnlyProps nodePropsRef
    )
    {
        int violations = 0;
        for (Map.Entry<String, Map<String, Integer>> entry : xReplicasOnDiffMapWithValuesRef.entrySet())
        {
            String propKey = entry.getKey();
            @Nullable String nodeValue = nodePropsRef.getProp(propKey);
            if (nodeValue != null)
            {
                @Nullable Integer allowedCount = entry.getValue().get(nodeValue);
                if (allowedCount == null || allowedCount < 0)
                {
                    errorReporter.logTrace(
                        "AutoUnplacer:   XReplicasOnDifferent: Property %s allowed %d times",
                        propKey,
                        allowedCount // string format should be fine with null values, even for %d output
                    );
                    violations++;
                }
            } // if the node has no value for this property it is always counted as OK
        }
        logViolationCount("XReplicasOnDifferent", violations);
        return violations;
    }

    private int getViolationsCountDoNotPlaceWith(
        @Nullable List<String> doNotPlaceWithRscListRef,
        @Nullable String doNotPlaceWithRscRegexRef,
        Node nodeRef
    )
        throws AccessDeniedException
    {
        int violations = 0;
        @Nullable Predicate<ResourceName> testCombined = null;
        if (doNotPlaceWithRscListRef != null && !doNotPlaceWithRscListRef.isEmpty())
        {
            Set<String> avoidRscNamesUpper = new HashSet<>(StringUtils.toUpperList(doNotPlaceWithRscListRef));
            testCombined = rscName -> avoidRscNamesUpper.contains(rscName.value);
        }
        if (doNotPlaceWithRscRegexRef != null)
        {
            Pattern avoidRscNamesPattern = Pattern.compile(doNotPlaceWithRscRegexRef, Pattern.CASE_INSENSITIVE);
            Predicate<ResourceName> predicate = rscName -> avoidRscNamesPattern.matcher(rscName.displayValue).matches();
            if (testCombined != null)
            {
                testCombined = testCombined.and(predicate);
            }
            else
            {
                testCombined = predicate;
            }
        }

        if (testCombined != null)
        {
            violations = (int) nodeRef.streamResources(apiAccCtx)
                .map(rsc -> rsc.getResourceDefinition().getName())
                .filter(testCombined)
                .count();
            logViolationCount("DoNotPlaceWith", violations);
        }

        return violations;
    }

    private int getViolationsCountLayerStack(@Nullable List<DeviceLayerKind> expectedLayerStackListRef, Resource rscRef)
        throws AccessDeniedException
    {
        int violations = 0;
        if (expectedLayerStackListRef != null && !expectedLayerStackListRef.isEmpty())
        {
            List<DeviceLayerKind> layerStack = LayerRscUtils.getLayerStack(rscRef.getLayerData(apiAccCtx), apiAccCtx);
            if (!layerStack.equals(expectedLayerStackListRef))
            {
                violations = 1;
            }
            logViolationCount("LayerStack", violations);
        }
        return violations;
    }

    private int getViolationsCountNodeNameList(@Nullable List<String> nodeNameListRef, Resource rscRef)
    {
        int violations = 0;
        if (nodeNameListRef != null && !nodeNameListRef.isEmpty())
        {
            List<String> upperList = StringUtils.toUpperList(nodeNameListRef);
            NodeName nodeName = rscRef.getNode().getName();
            if (!upperList.contains(nodeName.value))
            {
                violations = 1;
            }
            logViolationCount("NodeNames", violations);
        }
        return violations;
    }

    private int getViolationsCounthandleProvider(@Nullable List<DeviceProviderKind> providerListRef, Resource rscRef)
        throws AccessDeniedException
    {
        return genericSpCheck(
            providerListRef,
            rscRef,
            UnaryOperator.identity(),
            StorPool::getDeviceProviderKind,
            "ProviderKind"
        );
    }

    private int getViolationsCountStorPoolName(@Nullable List<String> storPoolNameListRef, Resource rscRef)
        throws AccessDeniedException
    {
        return genericSpCheck(
            storPoolNameListRef,
            rscRef,
            StringUtils::toUpperList,
            sp -> sp.getName().value,
            "StorPoolNames"
        );
    }

    private <T> int genericSpCheck(
        @Nullable List<T> expectedListRef,
        Resource rscRef,
        UnaryOperator<List<T>> listMapperRef,
        Function<StorPool, T> typeMapperRef,
        String logDescriptionRef
    )
        throws AccessDeniedException
    {
        int violations = 0;
        if (expectedListRef != null && !expectedListRef.isEmpty())
        {
            List<T> mappedExpectedList = listMapperRef.apply(expectedListRef);
            Set<StorPool> storPools = LayerVlmUtils.getStorPools(rscRef, apiAccCtx);
            for (StorPool sp : storPools)
            {
                if (!mappedExpectedList.contains(typeMapperRef.apply(sp)))
                {
                    errorReporter.logTrace("AutoUnplacer:   %s: %s", logDescriptionRef, sp);
                    violations++;
                }
            }
            logViolationCount(logDescriptionRef, violations);
        }
        return violations;
    }

    private int getViolationsCountReplicasOnSameList(
        @Nullable List<String> replicasOnSameRef,
        ReadOnlyProps node1PropsRef,
        ReadOnlyProps node2PropsRef
    )
    {
        int violations = 0;
        if (replicasOnSameRef != null && !replicasOnSameRef.isEmpty())
        {
            for (String key : replicasOnSameRef)
            {
                @Nullable String node1Value = node1PropsRef.getProp(key);
                @Nullable String node2Value = node2PropsRef.getProp(key);
                if (!Objects.equals(node1Value, node2Value))
                {
                    errorReporter.logTrace("AutoUnplacer:   ReplicasOnSame (dynamic): Property '%s' differs", key);
                    violations++;
                }
            }
            logViolationCount("ReplicasOnSame (dynamic)", violations);
        }
        return violations;
    }

    private void logViolationCount(String logDescriptionRef, int violationScoreRef)
    {
        if (violationScoreRef > 0)
        {
            errorReporter.logTrace(
                "AutoUnplacer:  Score from violating check %s: %d",
                logDescriptionRef,
                violationScoreRef
            );
        }
    }

    private void add(Map<Resource, Long> unplaceRateRef, Resource rscRef, long additionalRatingRef)
    {
        unplaceRateRef.put(rscRef, unplaceRateRef.get(rscRef) + additionalRatingRef);
    }
}
