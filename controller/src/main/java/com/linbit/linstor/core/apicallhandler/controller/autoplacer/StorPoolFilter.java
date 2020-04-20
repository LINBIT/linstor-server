package com.linbit.linstor.core.apicallhandler.controller.autoplacer;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.core.CoreModule.StorPoolDefinitionMap;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
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
import java.util.HashMap;
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

    @Inject
    public StorPoolFilter(
        @SystemContext AccessContext apiAccCtxRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        StorPoolDefinitionMap storPoolDfnMapRef
    )
    {
        apiAccCtx = apiAccCtxRef;
        peerAccCtx = peerAccCtxRef;
        storPoolDfnMap = storPoolDfnMapRef;
    }

    /**
     * Returns a list of storage pools that are
     * <ul>
     * <li>accessible (via Peer's {@link AccessContext})</li>
     * <li>diskful</li>
     * <li>online</li>
     * </ul>
     *
     * @return
     */
    public ArrayList<StorPool> listAvailableStorPools()
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
                            storPool.getDeviceProviderKind().hasBackingDevice() && // diskful
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
        long sizeInKib
    )
        throws AccessDeniedException
    {
        ArrayList<StorPool> filteredList = new ArrayList<>();

        Map<Node, Boolean> nodeMatchesMap = new HashMap<>();

        List<String> filterNodeNameList = selectFilter.getNodeNameList();
        List<String> filterStorPoolNameList = selectFilter.getStorPoolNameList();
        List<String> filterDoNotPlaceWithRscList = selectFilter.getDoNotPlaceWithRscList();
        String filterDoNotPlaceWithRscRegex = selectFilter.getDoNotPlaceWithRscRegex();
        Map<String, String> filterNodePropsMatch = extractFixedProperties(selectFilter.getReplicasOnSameList());
        Map<String, String> filterNodePropsMismatch = extractFixedProperties(selectFilter.getReplicasOnDifferentList());
        List<DeviceLayerKind> filterLayerList = selectFilter.getLayerStackList();
        List<DeviceProviderKind> filterProviderList = selectFilter.getProviderList();

        for (StorPool sp : availableStorPoolsRef)
        {
            boolean storPoolMatches = true;

            Node node = sp.getNode();

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
                        if (nodeNameStr.equalsIgnoreCase(node.getName().displayValue))
                        {
                            nodeNameFound = true;
                            break;
                        }
                    }
                    nodeMatches = nodeNameFound;
                }
                if (nodeMatches && filterNodePropsMatch != null && !filterNodePropsMatch.isEmpty())
                {
                    for (Entry<String, String> matchEntry : filterNodePropsMatch.entrySet())
                    {
                        String val = nodeProps.getProp(matchEntry.getKey());
                        if (!matchEntry.getValue().equals(val))
                        {
                            nodeMatches = false;
                            break;
                        }
                    }
                }
                if (nodeMatches && filterNodePropsMismatch != null && !filterNodePropsMismatch.isEmpty())
                {
                    boolean anyMatch = false;
                    for (Entry<String, String> mismatchEntry : filterNodePropsMismatch.entrySet())
                    {
                        String val = nodeProps.getProp(mismatchEntry.getKey());
                        if (mismatchEntry.getValue().equals(val))
                        {
                            anyMatch = true;
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

                    Iterator<Resource> iterateResources = node.iterateResources(apiAccCtx);
                    while (nodeMatches && iterateResources.hasNext())
                    {
                        String rscName = iterateResources.next().getDefinition().getName().value;
                        nodeMatches = !matchesRegex.test(rscName) && !containedInList.test(rscName);
                    }
                }

                nodeMatchesMap.put(node, nodeMatches);
            }

            if (nodeMatches)
            {
                storPoolMatches = sp.getFreeSpaceTracker().getFreeCapacityLastUpdated(apiAccCtx)
                    .orElse(0L) >= sizeInKib;

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
                }
                if (storPoolMatches && filterProviderList != null && !filterProviderList.isEmpty())
                {
                    storPoolMatches = filterProviderList.contains(sp.getDeviceProviderKind());
                }

                if (storPoolMatches)
                {
                    filteredList.add(sp);
                }
            }
        }
        return filteredList;
    }

    /**
     * The input is a List of String, where each element could be a simple key (i.e. "key") but also a
     * pair of key-value (i.e. "key=value").
     * This method extracts only the key-value pairs and returns them as a map. All other simple keys will
     * be ignored.
     *
     * @param list
     *
     * @return
     */
    private Map<String, String> extractFixedProperties(List<String> list)
    {
        Map<String, String> ret = new TreeMap<>();
        for (String elem : list)
        {
            int idx = elem.indexOf("=");
            if (idx != -1)
            {
                String key = elem.substring(0, idx);
                String value = elem.substring(idx + 1);
                ret.put(key, value);
            }
        }
        return ret;
    }
}
