package com.linbit.linstor.core.apicallhandler.controller.autoplacer;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.core.CoreModule.StorPoolDefinitionMap;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;

import javax.inject.Inject;
import javax.inject.Provider;
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
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.regex.Pattern;

@Singleton
class StorPoolFilter
{
    private final AccessContext apiAccCtx;
    private final Provider<AccessContext> peerAccCtx;
    private final StorPoolDefinitionMap storPoolDfnMap;
    private final ErrorReporter errorReporter;

    @Inject
    public StorPoolFilter(
        @SystemContext AccessContext apiAccCtxRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        StorPoolDefinitionMap storPoolDfnMapRef,
        ErrorReporter errorReporterRef
    )
    {
        apiAccCtx = apiAccCtxRef;
        peerAccCtx = peerAccCtxRef;
        storPoolDfnMap = storPoolDfnMapRef;
        errorReporter = errorReporterRef;
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
                                storPool.getNode().getObjProt().queryAccess(peerCtx).hasAccess(AccessType.USE) && // access
                                storPool.getNode().getPeer(apiAccCtx).isConnected() // online
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
    ArrayList<StorPool> filter(
        AutoSelectFilterApi selectFilter,
        List<StorPool> availableStorPoolsRef,
        ResourceDefinition rscDfnRef,
        long sizeInKib,
        Resource.Flags disklessTypeRef
    )
        throws AccessDeniedException
    {
        ArrayList<StorPool> filteredList = new ArrayList<>();

        Map<Node, Boolean> nodeMatchesMap = new HashMap<>();

        ArrayList<Props> alreadyDeployedNodesProps = new ArrayList<>();
        if (rscDfnRef != null)
        {
            Iterator<Resource> rscIt = rscDfnRef.iterateResource(apiAccCtx);
            while (rscIt.hasNext())
            {
                alreadyDeployedNodesProps.add(rscIt.next().getNode().getProps(apiAccCtx));
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
        Map<String, String> filterNodePropsMatch = extractFixedMatchingProperties(
            selectFilter.getReplicasOnSameList(),
            alreadyDeployedNodesProps
        );
        Map<String, ArrayList<String>> filterNodePropsMismatch = extractFixedMismatchingProperties(
            selectFilter.getReplicasOnDifferentList(),
            alreadyDeployedNodesProps
        );
        List<DeviceLayerKind> filterLayerList = selectFilter.getLayerStackList();
        List<DeviceProviderKind> filterProviderList = selectFilter.getProviderList();
        List<String> skipAlreadyPlacedOnNodeNamesCheck = selectFilter.skipAlreadyPlacedOnNodeNamesCheck();

        logIfNotEmpty("filtering mode: %s", diskful ? "diskful" : disklessTypeRef.name());
        logIfNotEmpty("filter node names: %s", filterNodeNameList);
        logIfNotEmpty("filter stor pool names: %s", filterStorPoolNameList);
        logIfNotEmpty("filter do not place with rsc: %s", filterDoNotPlaceWithRscList);
        logIfNotEmpty("filter do not place with rsc regex: %s", filterDoNotPlaceWithRscRegex);
        logIfNotEmpty("filter node properties match: %s", filterNodePropsMatch);
        logIfNotEmpty("filter node properties mismatch: %s", filterNodePropsMismatch);
        logIfNotEmpty("filter layer list: %s", filterLayerList);
        logIfNotEmpty("filter provider list: %s", filterProviderList);
        logIfNotEmpty("skip already placed on node names %s", skipAlreadyPlacedOnNodeNamesCheck);

        if (disklessTypeRef != null)
        {
            if (disklessTypeRef.equals(Resource.Flags.NVME_INITIATOR))
            {
                if (filterLayerList == null || !filterLayerList.contains(DeviceLayerKind.NVME))
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_INVLD_LAYER_STACK,
                            "You have to specify a --layer-list containing at least 'nvme' when autoplacing an nvme-initiator"
                        )
                    );
                }
            }
            else if (disklessTypeRef.equals(Resource.Flags.DRBD_DISKLESS))
            {
                if (filterLayerList == null || !filterLayerList.contains(DeviceLayerKind.DRBD))
                {
                    /*
                     * other as in NVME, we can assume here that DRBD is the topmost layer, as nothing is allowed above
                     * DRBD
                     */
                    if (filterLayerList == null)
                    {
                        filterLayerList = new ArrayList<>(Arrays.asList(DeviceLayerKind.DRBD));
                    }
                    else
                    {
                        ArrayList<DeviceLayerKind> tmpList = new ArrayList<>();
                        tmpList.add(DeviceLayerKind.DRBD);
                        tmpList.addAll(filterLayerList);

                        filterLayerList = tmpList;
                    }
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

        if (skipAlreadyPlacedOnNodeNamesCheck == null)
        {
            // just to prevent NPE
            skipAlreadyPlacedOnNodeNamesCheck = new ArrayList<>();
        }

        for (StorPool sp : availableStorPoolsRef)
        {
            boolean storPoolMatches = true;

            Node node = sp.getNode();
            String nodeDisplayValue = node.getName().displayValue;

            Boolean nodeMatches = nodeMatchesMap.get(node);
            if (nodeMatches == null)
            {
                nodeMatches = true;
                Props nodeProps = node.getProps(apiAccCtx);
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
                if (nodeMatches && filterNodePropsMatch != null && !filterNodePropsMatch.isEmpty())
                {
                    for (Entry<String, String> matchEntry : filterNodePropsMatch.entrySet())
                    {
                        String nodeVal = nodeProps.getProp(matchEntry.getKey());
                        String valueToMatch = matchEntry.getValue();
                        if (nodeVal == null)
                        {
                            nodeMatches = false;
                            errorReporter.logTrace(
                                "Autoplacer.Filter: Disqualifying node '%s' as it does not have the property '%s' set (required by replicas-on-same)",
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
                                "Autoplacer.Filter: Disqualifying node '%s' as it does not match fixed same property '%s'. Value required: '%s', but was: '%s'",
                                nodeDisplayValue,
                                matchEntry.getKey(),
                                valueToMatch,
                                nodeVal
                            );
                            break;
                        }
                    }
                }
                if (nodeMatches && filterNodePropsMismatch != null && !filterNodePropsMismatch.isEmpty())
                {
                    boolean anyMatch = false;
                    for (Entry<String, ArrayList<String>> mismatchEntry : filterNodePropsMismatch.entrySet())
                    {
                        String val = nodeProps.getProp(mismatchEntry.getKey());
                        if (mismatchEntry.getValue().contains(val))
                        {
                            anyMatch = true;
                            errorReporter.logTrace(
                                "Autoplacer.Filter: Disqualifying node '%s' as it does not match fixed different property '%s'. Value prohibited: %s, but node has: '%s'",
                                nodeDisplayValue,
                                mismatchEntry.getKey(),
                                mismatchEntry.getValue(),
                                val
                            );
                            break;
                        }
                    }
                    nodeMatches = !anyMatch;
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
                                "Autoplacer.Filter: Disqualifying node '%s' as it does not support required layer '%s'",
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

                    if (!skipAlreadyPlacedOnNodeNamesCheck.contains(nodeDisplayValue))
                    {
                        Iterator<Resource> iterateResources = node.iterateResources(apiAccCtx);
                        while (nodeMatches && iterateResources.hasNext())
                        {
                            Resource rsc = iterateResources.next();
                            if (!rsc.getStateFlags().isSet(apiAccCtx, Resource.Flags.DELETE))
                            {
                                String rscName = rsc.getDefinition().getName().displayValue;

                                boolean hasRscDeployed = matchesRegex.test(rscName) || containedInList.test(rscName);
                                nodeMatches = !hasRscDeployed;
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
                }

                nodeMatchesMap.put(node, nodeMatches);
            }

            if (nodeMatches)
            {
                if (diskful)
                {
                    storPoolMatches = sp.getFreeSpaceTracker().getFreeCapacityLastUpdated(apiAccCtx)
                        .orElse(0L) >= sizeInKib;
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
                            "Autoplacer.Filter: Disqualifying storage pool '%s' on node '%s' as the storage pool does not match the given name filter",
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
                            "Autoplacer.Filter: Disqualifying storage pool '%s' on node '%s' as the storage pool does not match the given device provider filter",
                            sp.getName().displayValue,
                            sp.getNode().getName().displayValue
                        );
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

    @SuppressWarnings("rawtypes")
    private void logIfNotEmpty(String format, Object obj)
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
    private Map<String, String> extractFixedMatchingProperties(
        List<String> propsList,
        List<Props> alreadyDeployedNodeProps
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
                    for (Props alreadyDeployedNodeProp : alreadyDeployedNodeProps)
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
                                "The propert property in --replicas-on-same '" + elem + "' is already set " +
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
     * Basically the same as the {@link #extractFixedMatchingProperties(List)}, but this time we return a list of values
     *
     * @param propsList
     *
     * @return
     */
    private Map<String, ArrayList<String>> extractFixedMismatchingProperties(
        List<String> propsList,
        List<Props> alreadyDeployedNodeProps
    )
    {
        Map<String, ArrayList<String>> ret = new TreeMap<>();
        if (propsList != null)
        {
            for (String elem : propsList)
            {
                String key;
                String fixedValueToAvoid = null;
                int idx = elem.indexOf("=");
                if (idx != -1)
                {
                    key = elem.substring(0, idx);
                    fixedValueToAvoid = elem.substring(idx + 1);
                }
                else
                {
                    key = elem;
                }

                ArrayList<String> fixedValuesList = ret.get(key);
                if (fixedValuesList == null)
                {
                    fixedValuesList = new ArrayList<>();
                    ret.put(key, fixedValuesList);
                }
                if (fixedValueToAvoid != null)
                {
                    fixedValuesList.add(fixedValueToAvoid);
                }
                for (Props alreadyDeployedNodeProp : alreadyDeployedNodeProps)
                {
                    String currentValue = alreadyDeployedNodeProp.getProp(elem);
                    if (currentValue != null)
                    {
                        fixedValuesList.add(currentValue);
                    }
                }

            }
        }
        return ret;
    }
}
