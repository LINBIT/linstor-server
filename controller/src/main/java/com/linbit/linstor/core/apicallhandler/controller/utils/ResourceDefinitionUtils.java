package com.linbit.linstor.core.apicallhandler.controller.utils;

import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.controller.CtrlCbtSessionApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotDeleteApiCallHandler;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.tasks.AutoSnapshotTask;
import com.linbit.linstor.utils.layer.LayerVlmUtils;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Predicate;

import reactor.core.publisher.Flux;

public class ResourceDefinitionUtils
{
    public static int getResourceCount(
        ResourceDefinition rscDfn,
        Predicate<Resource> predicate
    )
    {
        int count = 0;
        Iterator<Resource> rscIt = rscDfn.iterateResource();
        while (rscIt.hasNext())
        {
            Resource rsc = rscIt.next();
            if (predicate.test(rsc))
            {
                count++;
            }
        }
        return count;
    }

    /**
     * Ensures that no shared storage pool backing the given rsc-dfn has its resource active on more
     * than one node, as is the case during the dual-active window of a live migration. The data
     * shared by those resources cannot be safely modified while multiple nodes are accessing it -
     * refuse with a clear error instead of failing on the satellites.
     *
     * @param errObjDescrRef the object the refused action targets, e.g. "Volume definition 0 of 'rsc'"
     * @param actionDescrRef the refused action as a passive verb, e.g. "resized", "cloned"
     */
    public static void ensureSharedDataNotActiveOnMultipleNodes(
        ResourceDefinition rscDfnRef,
        String errObjDescrRef,
        String actionDescrRef
    )
    {
        Map<SharedStorPoolName, Set<NodeName>> activeNodesBySharedName = new TreeMap<>();
        Iterator<Resource> rscIt = rscDfnRef.iterateResource();
        while (rscIt.hasNext())
        {
            Resource rsc = rscIt.next();
            if (rsc.getStateFlags().isUnset(Resource.Flags.INACTIVE))
            {
                for (StorPool sp : LayerVlmUtils.getStorPools(rsc, true))
                {
                    SharedStorPoolName sharedSpName = sp.getSharedStorPoolName();
                    if (sp.isShared())
                    {
                        activeNodesBySharedName
                            .computeIfAbsent(sharedSpName, ignored -> new TreeSet<>())
                            .add(rsc.getNode().getName());
                    }
                }
            }
        }
        for (Map.Entry<SharedStorPoolName, Set<NodeName>> entry : activeNodesBySharedName.entrySet())
        {
            Set<NodeName> activeNodes = entry.getValue();
            if (activeNodes.size() > 1)
            {
                StringBuilder nodeList = new StringBuilder();
                for (NodeName nodeName : activeNodes)
                {
                    nodeList.append("'").append(nodeName.displayValue).append("', ");
                }
                nodeList.setLength(nodeList.length() - 2); // cut last ", "

                throw new ApiRcException(
                    ApiCallRcImpl.entryBuilder(
                        ApiConsts.FAIL_IN_USE,
                        errObjDescrRef + " cannot be " + actionDescrRef +
                            " while its resource is active on multiple nodes (" + nodeList +
                            ") of the shared storage pool '" + entry.getKey() + "'"
                    )
                        .setCause(
                            "The data shared by these resources cannot be " + actionDescrRef +
                                " while more than one node uses it, e.g. during a live migration."
                        )
                        .setCorrection(
                            "Finish the live migration and remove the migration-source resource " +
                                "(unmake-available) first."
                        )
                        .setSkipErrorReport(true)
                        .build()
                );
            }
        }
    }

    public static Flux<ApiCallRc> handleAutoSnapProps(
        AutoSnapshotTask autoSnapshotTaskRef,
        CtrlSnapshotDeleteApiCallHandler ctrlSnapDeleteHandlerRef,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        Set<String> deletedNamespaces,
        Collection<ResourceDefinition> affectedRscDfnListRef,
        ReadOnlyProps ctrlProps,
        boolean forceCheckAllRscDfnRef
    )
    {
        Flux<ApiCallRc> retFlux = Flux.empty();
        String autoSnapKey = ApiConsts.NAMESPC_AUTO_SNAPSHOT + "/" + ApiConsts.KEY_RUN_EVERY;
        String autoSnapVal = overrideProps.get(autoSnapKey);
        boolean namespaceDeleted = deletedNamespaces.contains(ApiConsts.NAMESPC_AUTO_SNAPSHOT);

        Set<ResourceDefinition> modifiedRscDfnSet = new HashSet<>();
        if (autoSnapVal != null || forceCheckAllRscDfnRef)
        {
            for (ResourceDefinition rscDfn : affectedRscDfnListRef)
            {
                PriorityProps prioProps = new PriorityProps(
                    rscDfn.getProps(),
                    rscDfn.getResourceGroup().getProps(),
                    ctrlProps
                );
                String prioPropVal = prioProps.getProp(autoSnapKey);
                // the temporary resource of a CBT session must not get snapshots
                if (prioPropVal != null && !CtrlCbtSessionApiCallHandler.isCbtSessionRscDfn(rscDfn))
                {
                    retFlux = retFlux.concatWith(
                        autoSnapshotTaskRef.addAutoSnapshotting(
                            rscDfn.getName().displayValue,
                            Long.parseLong(prioPropVal)
                        )
                    );
                    modifiedRscDfnSet.add(rscDfn);
                }
            }
        }
        if (deletePropKeys.contains(autoSnapKey) || namespaceDeleted || forceCheckAllRscDfnRef)
        {
            for (ResourceDefinition rscDfn : affectedRscDfnListRef)
            {
                if (!modifiedRscDfnSet.contains(rscDfn))
                {
                    PriorityProps prioProps = new PriorityProps(
                        rscDfn.getProps(),
                        rscDfn.getResourceGroup().getProps(),
                        ctrlProps
                    );
                    String prioPropVal = prioProps.getProp(autoSnapKey);
                    if (prioPropVal == null || CtrlCbtSessionApiCallHandler.isCbtSessionRscDfn(rscDfn))
                    {
                        autoSnapshotTaskRef.removeAutoSnapshotting(rscDfn.getName().displayValue);
                    }
                    else
                    {
                        retFlux = retFlux.concatWith(
                            autoSnapshotTaskRef.addAutoSnapshotting(
                                rscDfn.getName().displayValue,
                                Long.parseLong(prioPropVal)
                            )
                        );
                    }
                    modifiedRscDfnSet.add(rscDfn);
                }
            }
        }

        String autoSnapKeepKey = ApiConsts.NAMESPC_AUTO_SNAPSHOT + "/" + ApiConsts.KEY_KEEP;
        if (overrideProps.containsKey(autoSnapKeepKey) || deletePropKeys.contains(autoSnapKeepKey) ||
            forceCheckAllRscDfnRef)
        {
            for (ResourceDefinition rscDfn : affectedRscDfnListRef)
            {
                if (!modifiedRscDfnSet.contains(rscDfn))
                {
                    retFlux = retFlux.concatWith(ctrlSnapDeleteHandlerRef.cleanupOldAutoSnapshots(rscDfn));
                }
            }
        }
        return retFlux;
    }
}
