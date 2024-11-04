package com.linbit.linstor.core.apicallhandler.controller.autoplacer;

import com.linbit.ImplementationError;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.core.CoreModule.StorPoolDefinitionMap;
import com.linbit.linstor.core.apicallhandler.controller.CtrlPropsHelper;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityAutoPoolSelectorUtils;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.utils.NullableUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.storage.kinds.ExtToolsInfo.Version;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;
import com.linbit.utils.StringUtils;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.regex.Pattern;

@Singleton
public class StorPoolFilter
{
    private final AccessContext apiAccCtx;
    private final Provider<AccessContext> peerAccCtx;
    private final StorPoolDefinitionMap storPoolDfnMap;
    private final ErrorReporter errorReporter;
    private final CtrlPropsHelper ctrlPropsHelper;

    @Inject
    StorPoolFilter(
        @SystemContext AccessContext apiAccCtxRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        StorPoolDefinitionMap storPoolDfnMapRef,
        ErrorReporter errorReporterRef,
        CtrlPropsHelper ctrlPropsHelperRef
    )
    {
        apiAccCtx = apiAccCtxRef;
        peerAccCtx = peerAccCtxRef;
        storPoolDfnMap = storPoolDfnMapRef;
        errorReporter = errorReporterRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
    }

    /**
     * Returns a list of storage pools that are
     * <ul>
     * <li>accessible (via Peer's {@link AccessContext})</li>
     * <li>diskful if parameter is true, diskless otherwise</li>
     * <li>online</li>
     * </ul>
     *
     * @return
     */
    public ArrayList<StorPool> listAvailableStorPools(boolean diskful)
    {
        ArrayList<StorPool> ret = new ArrayList<>();
        try
        {
            AccessContext peerCtx = peerAccCtx.get();
            for (StorPoolDefinition storPoolDfn : storPoolDfnMap.values())
            {
                // check storPoolDfn access
                if (storPoolDfn.getObjProt().queryAccess(peerCtx).hasAccess(AccessType.USE))
                {
                    Iterator<StorPool> storPoolsIt = storPoolDfn.iterateStorPools(apiAccCtx);
                    while (storPoolsIt.hasNext())
                    {
                        StorPool storPool = storPoolsIt.next();

                        if (
                            storPool.getDeviceProviderKind().hasBackingDevice() == diskful &&
                                // have USE access
                                storPool.getNode().getObjProt().queryAccess(peerCtx).hasAccess(AccessType.USE) &&
                                // peer is online
                                storPool.getNode().getPeer(apiAccCtx).isOnline()
                        )
                        {
                            ret.add(storPool);
                        }
                    }
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return ret;
    }

    /**
     * This method does not change the content of the given list of storage pools
     * Instead a new list is returned containing only a filtered subset of the given list of storage pools.
     * The filters are:
     * <ul>
     * <li>storage pool must have enough free space</li>
     * <li>restriction by node names</li>
     * <li>restriction by storage pool names</li>
     * <li>if a specific value is given, node properties must match a given value</li>
     * <li>if a specific value is given, node properties must not match a given value</li>
     * <li>node must support all necessary layers</li>
     * <li>storage pool's provider kind must be contained in the filtered provider kind list</li>
     * </ul>
     *
     * @param selectFilter
     * @param availableStorPoolsRef
     * @param rscDfnRef
     * @param disklessTypeRef
     * @param freeCapacitiesRef
     * @param rscDfn
     *
     * @return
     *
     * @throws AccessDeniedException
     */
    public ArrayList<StorPool> filter(
        AutoSelectFilterApi selectFilter,
        List<StorPool> availableStorPoolsRef,
        @Nullable ResourceDefinition rscDfnRef,
        long sizeInKib,
        @Nullable Resource.Flags disklessTypeRef
    )
        throws AccessDeniedException
    {
        List<String> skipAlreadyPlacedOnNodeNamesCheck = NullableUtils.mapNullable(
            selectFilter.skipAlreadyPlacedOnNodeNamesCheck(),
            () -> new ArrayList<>(),
            StringUtils::toUpperList
        );
        boolean skipAlreadyPlacedOnAllNodesCheck = NullableUtils.orElse(
            selectFilter.skipAlreadyPlacedOnAllNodeCheck(),
            false
        );

        ArrayList<ReadOnlyProps> alreadyDeployedNodesProps = new ArrayList<>();
        if (rscDfnRef != null && !skipAlreadyPlacedOnAllNodesCheck)
        {
            Iterator<Resource> rscIt = rscDfnRef.iterateResource(apiAccCtx);
            while (rscIt.hasNext())
            {
                Resource rsc = rscIt.next();
                Node node = rsc.getNode();
                if (!skipAlreadyPlacedOnNodeNamesCheck.contains(node.getName().value) &&
                    rsc.getStateFlags().isUnset(apiAccCtx, Resource.Flags.EVACUATE, Resource.Flags.EVICTED) &&
                    node.getFlags().isUnset(apiAccCtx, Node.Flags.EVACUATE, Node.Flags.EVICTED))
                {
                    alreadyDeployedNodesProps.add(node.getProps(apiAccCtx));
                }
            }
        }
        boolean diskful = disklessTypeRef == null;

        List<String> filterNodeNameList = selectFilter.getNodeNameList();
        List<String> filterStorPoolNameList;
        if (diskful)
        {
            filterStorPoolNameList = selectFilter.getStorPoolNameList();
        }
        else
        {
            filterStorPoolNameList = selectFilter.getStorPoolDisklessNameList();
        }
        List<String> filterDoNotPlaceWithRscList = selectFilter.getDoNotPlaceWithRscList();
        String filterDoNotPlaceWithRscRegex = selectFilter.getDoNotPlaceWithRscRegex();
        Map<String, String> filterNodePropsSame = extractFixedSameProperties(
            selectFilter.getReplicasOnSameList(),
            alreadyDeployedNodesProps
        );
        Map<String, Map<String, Integer>> filterNodePropsDifferent = extractFixedDifferentProperties(
            selectFilter.getReplicasOnDifferentList(),
            alreadyDeployedNodesProps,
            selectFilter.getXReplicasOnDifferentMap()
        );
        List<DeviceLayerKind> filterLayerList = selectFilter.getLayerStackList();
        List<DeviceProviderKind> filterProviderList = selectFilter.getProviderList();
        Map<ExtTools, ExtToolsInfo.Version> requiredVersion = selectFilter.getRequiredExtTools();

        logIfNotEmpty("filtering mode: %s", diskful ? "diskful" : disklessTypeRef.name());
        logIfNotEmpty("filter node names: %s", filterNodeNameList);
        logIfNotEmpty("filter stor pool names: %s", filterStorPoolNameList);
        logIfNotEmpty("filter do not place with rsc: %s", filterDoNotPlaceWithRscList);
        logIfNotEmpty("filter do not place with rsc regex: %s", filterDoNotPlaceWithRscRegex);
        logIfNotEmpty("filter node properties same: %s", filterNodePropsSame);
        logIfNotEmpty("filter node properties different: %s", filterNodePropsDifferent);
        logIfNotEmpty("filter layer list: %s", filterLayerList);
        logIfNotEmpty("filter provider list: %s", filterProviderList);
        logIfNotEmpty("skip already placed on node names %s", skipAlreadyPlacedOnNodeNamesCheck);
        logIfNotEmpty("skip already placed on ALL node: %b", skipAlreadyPlacedOnAllNodesCheck);
        logIfNotEmpty("required external tools on node: %s", requiredVersion);

        if (disklessTypeRef != null)
        {
            if (disklessTypeRef.equals(Resource.Flags.NVME_INITIATOR))
            {
                if (filterLayerList == null || !filterLayerList.contains(DeviceLayerKind.NVME))
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_INVLD_LAYER_STACK,
                            "You have to specify a --layer-list containing at least 'nvme' when autoplacing an " +
                                "nvme-initiator"
                        )
                    );
                }
            }
            else
            if (disklessTypeRef.equals(Resource.Flags.DRBD_DISKLESS))
            {
                /*
                 * other as in NVME, we can assume here that DRBD is the topmost layer, as nothing is allowed above
                 * DRBD
                 */
                if (filterLayerList == null)
                {
                    filterLayerList = new ArrayList<>(Arrays.asList(DeviceLayerKind.DRBD));
                }
                else if (!filterLayerList.contains(DeviceLayerKind.DRBD))
                {
                    ArrayList<DeviceLayerKind> tmpList = new ArrayList<>();
                    tmpList.add(DeviceLayerKind.DRBD);
                    tmpList.addAll(filterLayerList);

                    filterLayerList = tmpList;
                }
            }
            else if (disklessTypeRef.equals(Resource.Flags.EBS_INITIATOR))
            {
                if (filterProviderList == null || !filterProviderList.contains(DeviceProviderKind.EBS_INIT))
                {
                    if (filterProviderList == null)
                    {
                        filterProviderList = new ArrayList<>();
                    }
                    else
                    {
                        // copy the list since we are modifying it. Also to prevent UnsupportedOperationException in
                        // case we were called with Collection.unmodifyableList or singleton or something like that
                        filterProviderList = new ArrayList<>(filterProviderList);
                    }
                    filterProviderList.add(DeviceProviderKind.EBS_INIT);
                }
            }
        }
        else
        {
            /*
             * Special case for diskful nvme layer:
             * The autoplacer can safely ignore all layers "above" nvme, as the autoplacer will only place nvme-targets.
             * A satellite acting as an nvme-target does not need to support any layers above nvme.
             */

            if (filterLayerList != null && filterLayerList.contains(DeviceLayerKind.NVME))
            {
                filterLayerList = new ArrayList<>(
                    filterLayerList.subList(
                        filterLayerList.indexOf(DeviceLayerKind.NVME), // list will include NVMe
                        filterLayerList.size() // and everything after NVMe
                    )
                );
            }
        }

        ArrayList<StorPool> filteredList = new ArrayList<>();
        Map<Node, Boolean> nodeMatchesMap = new HashMap<>();
        for (StorPool sp : availableStorPoolsRef)
        {
            boolean storPoolMatches = true;

            Node node = sp.getNode();
            String nodeDisplayValue = node.getName().displayValue;

            Boolean nodeMatches = nodeMatchesMap.get(node);
            if (nodeMatches == null)
            {
                nodeMatches = !node.getFlags().isSet(apiAccCtx, Node.Flags.DELETE);
                if (!nodeMatches)
                {
                    errorReporter.logTrace(
                        "Autoplacer.Filter: Disqualifying node '%s' as it is currently being deleted",
                        nodeDisplayValue,
                        filterNodeNameList
                    );
                }
                nodeMatches = !node.getFlags().isSet(apiAccCtx, Node.Flags.EVACUATE);
                if (!nodeMatches)
                {
                    errorReporter.logTrace(
                        "Autoplacer.Filter: Disqualifying node '%s' as it is currently being evacuated",
                        nodeDisplayValue,
                        filterNodeNameList
                    );
                }

                ReadOnlyProps nodeProps = node.getProps(apiAccCtx);
                if (nodeMatches && filterNodeNameList != null && !filterNodeNameList.isEmpty())
                {
                    boolean nodeNameFound = false;
                    for (String nodeNameStr : filterNodeNameList)
                    {
                        if (nodeNameStr.equalsIgnoreCase(nodeDisplayValue))
                        {
                            nodeNameFound = true;
                            break;
                        }
                    }
                    nodeMatches = nodeNameFound;
                    if (!nodeNameFound)
                    {
                        errorReporter.logTrace(
                            "Autoplacer.Filter: Disqualifying node '%s' as it does not match node name filter %s",
                            nodeDisplayValue,
                            filterNodeNameList
                        );
                    }
                }
                if (nodeMatches && !filterNodePropsSame.isEmpty())
                {
                    for (Entry<String, String> matchEntry : filterNodePropsSame.entrySet())
                    {
                        String nodeVal = nodeProps.getProp(matchEntry.getKey());
                        String valueToMatch = matchEntry.getValue();
                        if (nodeVal == null)
                        {
                            nodeMatches = false;
                            errorReporter.logTrace(
                                "Autoplacer.Filter: Disqualifying node '%s' as it does not have the property '%s' " +
                                    "set (required by replicas-on-same)",
                                nodeDisplayValue,
                                matchEntry.getKey(),
                                valueToMatch,
                                nodeVal
                            );
                            break;
                        }
                        else
                        if (valueToMatch != null && !nodeVal.equals(valueToMatch))
                        {
                            nodeMatches = false;
                            errorReporter.logTrace(
                                "Autoplacer.Filter: Disqualifying node '%s' as it does not match fixed same " +
                                    "property '%s'. Value required: '%s', but was: '%s'",
                                nodeDisplayValue,
                                matchEntry.getKey(),
                                valueToMatch,
                                nodeVal
                            );
                            break;
                        }
                    }
                }
                if (nodeMatches && !filterNodePropsDifferent.isEmpty())
                {
                    boolean anyMatch = false;
                    for (Entry<String, Map<String, Integer>> diffPropEntry : filterNodePropsDifferent.entrySet())
                    {
                        String key = diffPropEntry.getKey();
                        @Nullable String val = nodeProps.getProp(key);
                        if (val != null)
                        {
                            Map<String, Integer> valuesWithRemainingCount = diffPropEntry.getValue();
                            @Nullable Integer remainingCount = valuesWithRemainingCount.get(val);
                            if (remainingCount != null && remainingCount <= 0)
                            {
                                anyMatch = true;
                                errorReporter.logTrace(
                                    "Autoplacer.Filter: Disqualifying node '%s' as it has a prohibited value for " +
                                        "property '%s'. Value(s) prohibited: %s, but node has: '%s'",
                                    nodeDisplayValue,
                                    key,
                                    valuesWithRemainingCount.keySet(),
                                    val
                                );
                                break;
                            }
                        }
                    }
                    nodeMatches = !anyMatch;
                }

                if (nodeMatches)
                {
                    PriorityProps prioProps = new PriorityProps(nodeProps, ctrlPropsHelper.getCtrlPropsForView());
                    String allowAutoPlace = prioProps.getProp(ApiConsts.KEY_AUTOPLACE_ALLOW_TARGET);
                    if (allowAutoPlace != null && allowAutoPlace.equalsIgnoreCase("false"))
                    {
                        errorReporter.logTrace(
                            "Autoplacer.Filter: Disqualifying node '%s' as the property '%s' (set on either this " +
                                "node or on controller level) prevents this node from being targeted by the autoplacer",
                            nodeDisplayValue,
                            ApiConsts.KEY_AUTOPLACE_ALLOW_TARGET
                        );
                        nodeMatches = false;
                    }
                }

                if (nodeMatches && filterLayerList != null && !filterLayerList.isEmpty())
                {
                    ExtToolsManager extToolsManager = node.getPeer(apiAccCtx).getExtToolsManager();
                    Set<DeviceLayerKind> supportedLayers = extToolsManager.getSupportedLayers();
                    for (DeviceLayerKind layer : filterLayerList)
                    {
                        if (!supportedLayers.contains(layer))
                        {
                            nodeMatches = false;
                            errorReporter.logTrace(
                                "Autoplacer.Filter: Disqualifying node '%s' as it does not support required " +
                                    "layer '%s'",
                                nodeDisplayValue,
                                layer.name()
                            );
                            break;
                        }
                    }
                }

                if (nodeMatches &&
                    (
                        filterDoNotPlaceWithRscList != null && !filterDoNotPlaceWithRscList.isEmpty() ||
                        filterDoNotPlaceWithRscRegex != null
                    )
                )
                {
                    nodeMatches = nodeDoNoPlaceWithFilters(
                        filterDoNotPlaceWithRscList,
                        filterDoNotPlaceWithRscRegex,
                        skipAlreadyPlacedOnNodeNamesCheck,
                        skipAlreadyPlacedOnAllNodesCheck,
                        node,
                        nodeDisplayValue
                    );
                }
                if (nodeMatches && requiredVersion != null)
                {
                    ExtToolsManager extToolsMgr = node.getPeer(apiAccCtx).getExtToolsManager();
                    for (Entry<ExtTools, ExtToolsInfo.Version> entry : requiredVersion.entrySet())
                    {
                        ExtTools extTool = entry.getKey();
                        ExtToolsInfo extToolInfo = extToolsMgr.getExtToolInfo(extTool);
                        Version version = entry.getValue();
                        if (extToolInfo == null || !extToolInfo.isSupported() ||
                            (version != null && !extToolInfo.hasVersionOrHigher(version)))
                        {
                            errorReporter.logTrace(
                                "Autoplacer.Filter: Disqualifying node '%s' as it does not have extTool %s installed",
                                nodeDisplayValue,
                                version == null ?
                                    extTool.name() :
                                    extTool.name() + " (>= " + version + ")"
                            );
                            nodeMatches = false;
                            // no break to log all missing extTools if necessary
                        }
                    }
                }

                nodeMatchesMap.put(node, nodeMatches);
            }

            if (nodeMatches)
            {
                if (diskful)
                {
                    long freeCapacity = FreeCapacityAutoPoolSelectorUtils
                        .getFreeCapacityCurrentEstimationPrivileged(
                            apiAccCtx,
                            null,
                            sp,
                            ctrlPropsHelper.getCtrlPropsForView(),
                            true
                        )
                        .orElse(0L);
                    storPoolMatches = freeCapacity >= sizeInKib;
                    if (!storPoolMatches)
                    {
                        errorReporter.logTrace(
                            "Autoplacer.Filter: Disqualifying storage pool '%s' on node '%s' as the storage pool " +
                                "does not have enough free space",
                            sp.getName().displayValue,
                            sp.getNode().getName().displayValue
                        );
                    }
                }
                else
                {
                    storPoolMatches = !sp.getDeviceProviderKind().hasBackingDevice();
                }

                if (storPoolMatches && filterStorPoolNameList != null && !filterStorPoolNameList.isEmpty())
                {
                    boolean storPoolNameFound = false;
                    for (String storPoolNameStr : filterStorPoolNameList)
                    {
                        if (storPoolNameStr.equalsIgnoreCase(sp.getName().displayValue))
                        {
                            storPoolNameFound = true;
                            break;
                        }
                    }
                    storPoolMatches = storPoolNameFound;

                    if (!storPoolNameFound)
                    {
                        errorReporter.logTrace(
                            "Autoplacer.Filter: Disqualifying storage pool '%s' on node '%s' as the storage pool " +
                                "does not match the given name filter",
                            sp.getName().displayValue,
                            sp.getNode().getName().displayValue
                        );
                    }
                }
                if (storPoolMatches && filterProviderList != null && !filterProviderList.isEmpty())
                {
                    storPoolMatches = filterProviderList.contains(sp.getDeviceProviderKind());
                    if (!storPoolMatches)
                    {
                        errorReporter.logTrace(
                            "Autoplacer.Filter: Disqualifying storage pool '%s' on node '%s' as the storage pool " +
                                "does not match the given device provider filter",
                            sp.getName().displayValue,
                            sp.getNode().getName().displayValue
                        );
                    }
                }

                if (storPoolMatches)
                {
                    String allowAutoPlace = sp.getProps(apiAccCtx).getProp(ApiConsts.KEY_AUTOPLACE_ALLOW_TARGET);
                    if (allowAutoPlace != null && allowAutoPlace.equalsIgnoreCase("false"))
                    {
                        errorReporter.logTrace(
                            "Autoplacer.Filter: Disqualifying storage pool '%s' on node '%s' as the property '%s' " +
                                "(set on this storage pool) prevents this node from being targeted by the autoplacer",
                            sp.getName().displayValue,
                            sp.getNode().getName().displayValue,
                            ApiConsts.KEY_AUTOPLACE_ALLOW_TARGET
                        );
                        storPoolMatches = false;
                    }
                }

                if (storPoolMatches)
                {
                    filteredList.add(sp);
                }
            }
            else
            {
                errorReporter.logTrace(
                    "Autoplacer.Filter: Disqualifying storage pool '%s' on node '%s' as node is already disqualified",
                    sp.getName().displayValue,
                    sp.getNode().getName().displayValue
                );
            }
        }
        return filteredList;
    }

    private Boolean nodeDoNoPlaceWithFilters(
        List<String> filterDoNotPlaceWithRscList,
        String filterDoNotPlaceWithRscRegex,
        List<String> skipAlreadyPlacedOnNodeNamesCheck,
        Boolean skipAlreadyPlacedOnAllNodesCheck,
        Node node,
        String nodeDisplayValue
    )
        throws AccessDeniedException
    {
        boolean ret = true;
        Predicate<String> containedInList;
        Predicate<String> matchesRegex;
        if (filterDoNotPlaceWithRscList != null)
        {
            containedInList = str ->
            {
                boolean contained = false;
                for (String rscName : filterDoNotPlaceWithRscList)
                {
                    if (str.equalsIgnoreCase(rscName))
                    {
                        contained = true;
                        break;
                    }
                }
                return contained;
            };
        }
        else
        {
            containedInList = ignored -> false;
        }

        if (filterDoNotPlaceWithRscRegex != null)
        {
            Pattern regex = Pattern.compile(filterDoNotPlaceWithRscRegex, Pattern.CASE_INSENSITIVE);
            matchesRegex = str -> regex.matcher(str).find();
        }
        else
        {
            matchesRegex = ignored -> false;
        }

        if (skipAlreadyPlacedOnAllNodesCheck ||
            !skipAlreadyPlacedOnNodeNamesCheck.contains(nodeDisplayValue.toUpperCase()))
        {
            Iterator<Resource> iterateResources = node.iterateResources(apiAccCtx);
            while (ret && iterateResources.hasNext())
            {
                Resource rsc = iterateResources.next();
                if (!rsc.getStateFlags().isSet(apiAccCtx, Resource.Flags.DELETE))
                {
                    String rscName = rsc.getResourceDefinition().getName().displayValue;

                    boolean hasRscDeployed = matchesRegex.test(rscName) || containedInList.test(rscName);
                    ret = !hasRscDeployed;
                    if (hasRscDeployed)
                    {
                        errorReporter.logTrace(
                            "Autoplacer.Filter: Disqualifying node '%s' as it has resource '%s' deployed",
                            nodeDisplayValue,
                            rscName
                        );
                    }
                }
            }
        }
        return ret;
    }

    @SuppressWarnings("rawtypes")
    private void logIfNotEmpty(String format, @Nullable Object obj)
    {
        boolean log = false;
        if (obj != null)
        {
            if (obj instanceof Collection)
            {
                log = !((Collection) obj).isEmpty();
            }
            else if (obj instanceof Map)
            {
                log = !((Map) obj).isEmpty();
            }
            else
            {
                log = true;
            }
        }
        if (log)
        {
            errorReporter.logTrace("Autoplacer.Filter: " + format, obj);
        }
    }

    /**
     * The input is a List of String, where each element could be a simple key (i.e. "key") but also a
     * pair of key-value (i.e. "key=value").
     * This method extracts only the key-value pairs and returns them as a map. All other simple keys will
     * be ignored.
     *
     * @param propsList
     * @param rscDfnRef
     *
     * @return
     */
    private Map<String, String> extractFixedSameProperties(
        List<String> propsList,
        List<ReadOnlyProps> alreadyDeployedNodeProps
    )
    {
        Map<String, String> ret = new TreeMap<>();
        if (propsList != null)
        {
            for (String elem : propsList)
            {
                int idx = elem.indexOf("=");
                if (idx != -1)
                {
                    String key = elem.substring(0, idx);
                    String value = elem.substring(idx + 1);
                    ret.put(key, value);
                }
                else
                {
                    HashSet<String> nodeValues = new HashSet<>();
                    for (ReadOnlyProps alreadyDeployedNodeProp : alreadyDeployedNodeProps)
                    {
                        String currentValue = alreadyDeployedNodeProp.getProp(elem);
                        if (currentValue != null)
                        {
                            nodeValues.add(currentValue);
                        }
                    }

                    if (nodeValues.size() > 1)
                    {
                        throw new ApiRcException(
                            ApiCallRcImpl.simpleEntry(
                                ApiConsts.FAIL_UNDECIDABLE_AUTOPLACMENT,
                                "The property in --replicas-on-same '" + elem + "' is already set " +
                                    "on already deployed nodes with different values. Autoplacer cannot decide " +
                                    "which value to continue with. Linstor found the following conflicting values: " +
                                    nodeValues
                            )
                        );
                    }
                    if (nodeValues.size() == 1)
                    {
                        ret.put(elem, nodeValues.iterator().next());
                    }
                    else
                    {
                        ret.put(elem, null); // not fixed value, but we still need to make sure all nodes have SOME
                                             // value set here
                    }
                }
            }
        }
        return ret;
    }

    /**
     * Basically the same as the {@link #extractFixedSameProperties(List, List)}, but this time we return a map of
     * values and the number of their allowed usages (0 means prohibited, > 0 means {@link Selector} has to deal with
     * it)
     */
    private Map<String, Map<String, Integer>> extractFixedDifferentProperties(
        @Nullable List<String> origReplicasOnDifferentList,
        List<ReadOnlyProps> alreadyDeployedNodeProps,
        @Nullable Map<String, Integer> xReplicasOnDifferentRef
    )
    {
        // if we have keys in the xReplicasOndifferentRef map that do not exist in the origReplOnDiffList, we want to
        // add those keys to the list to also process them.

        // however, building such a dedicated list would be inefficient since we would need to iterate once through the
        // map and with a nested loop over all elements of the list, since we cannot use a simple list.contains(key)
        // call, since the list might contain "Aux/test=bla" entries, whereas the xReplicasOnDifferentMap would in such
        // a case only contain "Aux/test" as a key

        // therefore we copy the map, so we can remove the entries one by one as we process the
        // origReplicasOnDifferentList. after we are done with the list, we see if the map still has elements and
        // process the remaining entries, just as if they were part of the list without the "=bla" part

        Map<String, Integer> nonNullXReplicasOnDiffMap;
        Set<String> xReplicasOndifferentToProcess;
        if (xReplicasOnDifferentRef == null)
        {
            nonNullXReplicasOnDiffMap = Collections.emptyMap();
            xReplicasOndifferentToProcess = new HashSet<>();
        }
        else
        {
            nonNullXReplicasOnDiffMap = xReplicasOnDifferentRef;
            xReplicasOndifferentToProcess = new HashSet<>(xReplicasOnDifferentRef.keySet());
        }

        Map<String, Map<String, Integer>> ret = new TreeMap<>();
        if (origReplicasOnDifferentList != null)
        {
            for (String replOnDiffElement : origReplicasOnDifferentList)
            {
                String key;
                @Nullable String fixedValueToAvoid = null;
                int idx = replOnDiffElement.indexOf("=");
                if (idx != -1)
                {
                    key = replOnDiffElement.substring(0, idx);
                    fixedValueToAvoid = replOnDiffElement.substring(idx + 1);
                }
                else
                {
                    key = replOnDiffElement;
                }

                processReplOnDiff(alreadyDeployedNodeProps, nonNullXReplicasOnDiffMap, ret, key, fixedValueToAvoid);
                xReplicasOndifferentToProcess.remove(key);
            }
        }

        for (String unprocessedXReplOnDiffKey : xReplicasOndifferentToProcess)
        {
            processReplOnDiff(
                alreadyDeployedNodeProps,
                nonNullXReplicasOnDiffMap,
                ret,
                unprocessedXReplOnDiffKey,
                null
            );
        }

        return ret;
    }

    private void processReplOnDiff(
        List<ReadOnlyProps> alreadyDeployedNodeProps,
        Map<String, Integer> xReplicasOnDifferentRef,
        Map<String, Map<String, Integer>> ret,
        String key,
        @Nullable String fixedValueToAvoid
    )
    {
        Map<String, Integer> usedValuesMap = ret.get(key);
        if (usedValuesMap == null)
        {
            usedValuesMap = new TreeMap<>();
            ret.put(key, usedValuesMap);
        }
        if (fixedValueToAvoid != null)
        {
            usedValuesMap.put(fixedValueToAvoid, 0);
        }

        for (ReadOnlyProps alreadyDeployedNodeProp : alreadyDeployedNodeProps)
        {
            @Nullable String currentValue = alreadyDeployedNodeProp.getProp(key);
            if (currentValue != null)
            {
                @Nullable Integer remainingCount = usedValuesMap.get(currentValue);
                if (remainingCount == null)
                {
                    remainingCount = xReplicasOnDifferentRef.getOrDefault(
                        key,
                        SelectionManager.X_REPLICAS_DFLT_COUNT
                    );
                }
                usedValuesMap.put(currentValue, remainingCount - 1);
            }
        }
    }
}
