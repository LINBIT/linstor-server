package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.TransactionMgr;
import com.linbit.linstor.Node;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.CoreModule.ResourceDefinitionMap;
import com.linbit.linstor.core.CoreModule.StorPoolDefinitionMap;
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.utils.StreamUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;

public class CtrlRscAutoPlaceApiCallHandler extends AbsApiCallHandler
{
    private final ThreadLocal<String> currentRscName = new ThreadLocal<>();

    private final ResourceDefinitionMap rscDfnMap;
    private final StorPoolDefinitionMap storPoolDfnMap;
    private final CtrlRscApiCallHandler rscApiCallHandler;

    @Inject
    public CtrlRscAutoPlaceApiCallHandler(
        ErrorReporter errorReporterRef,
        DbConnectionPool dbConnectionPoolRef,
        CtrlStltSerializer interComSerializer,
        @ApiContext AccessContext apiCtxRef,
        // @Named(ControllerSecurityModule.STOR_POOL_DFN_MAP_PROT) ObjectProtection storPoolDfnMapProtRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        CoreModule.StorPoolDefinitionMap storPoolDfnMapRef,
        CtrlObjectFactories objectFactories,
        CtrlRscApiCallHandler rscApiCallHandlerRef
    )
    {
        super(
            errorReporterRef,
            dbConnectionPoolRef,
            apiCtxRef,
            ApiConsts.MASK_RSC,
            interComSerializer,
            objectFactories
        );
        super.setNullOnAutoClose(
            currentRscName
        );
        rscDfnMap = rscDfnMapRef;
        storPoolDfnMap = storPoolDfnMapRef;
        rscApiCallHandler = rscApiCallHandlerRef;
    }

    public ApiCallRc autoPlace(
        AccessContext accCtx,
        Peer client,
        String rscNameStr,
        int placeCount,
        String storPoolNameStr,
        List<String> notPlaceWithRscListRef,
        String notPlaceWithRscRegexStr
    )
    {
        // TODO extract this method into an own interface implementation
        // that the controller can choose between different auto-place strategies
        List<String> notPlaceWithRscList = notPlaceWithRscListRef.stream()
            .map(rscNameStrTmp -> rscNameStrTmp.toUpperCase())
            .collect(Collectors.toList());

        if (notPlaceWithRscRegexStr != null)
        {
            Pattern notPlaceWithRscRegexPattern = Pattern.compile(
                notPlaceWithRscRegexStr,
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL
            );
            notPlaceWithRscList.addAll(
                rscDfnMap.keySet().stream()
                    .map(rscName -> rscName.value)
                    .filter(rscName -> notPlaceWithRscRegexPattern.matcher(rscName).find())
                    .collect(Collectors.toList())
            );
        }

        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try (
            AbsApiCallHandler basicallyThis = setContext(
                accCtx,
                client,
                ApiCallType.CREATE,
                apiCallRc,
                null, // create new transMgr
                rscNameStr
            );
        )
        {
            // calculate the estimated size of the given resource
            final long rscSize = calculateResourceDefinitionSize(rscNameStr);

            // build a map of storage pools the user has access to and have enough free space
            Map<StorPoolName, List<StorPool>> storPools = storPoolDfnMap.values().stream()
                .filter(storPoolDfn -> storPoolDfn.getObjProt().queryAccess(accCtx).hasAccess(AccessType.USE))
                .flatMap(storPoolDfn ->
                    StreamUtils.toStream(
                        getStorPoolIterator(storPoolDfn)
                    )
                )
                .filter(storPool -> storPool.getNode().getObjProt().queryAccess(accCtx).hasAccess(AccessType.USE))
                .filter(storPool -> getFreeSpace(storPool) >= rscSize)
                .collect(
                    Collectors.groupingBy(storPool -> storPool.getName())
                );

            // apply the storage pool filter
            if (storPoolNameStr != null)
            {
                StorPoolName storPoolName = asStorPoolName(storPoolNameStr);
                storPools.keySet().retainAll(Arrays.asList(storPoolName));
            }

            List<Candidate> candidates;
            // try to consider the "do not place with resource" argument
            candidates = filterCandidates(placeCount, notPlaceWithRscList, storPools);
            if (candidates.isEmpty())
            {
                // if that didn't work, try to ignore the "do not place with resource" argument
                // but we have to avoid the storage pools used by the given set of resources.
                for (Entry<StorPoolName, List<StorPool>> entry : storPools.entrySet())
                {
                    List<StorPool> storPoolsToRemove = new ArrayList<>();
                    for (StorPool storPool : entry.getValue())
                    {
                        Collection<Volume> volumes = storPool.getVolumes(apiCtx);
                        for (Volume vlm : volumes)
                        {
                            if (notPlaceWithRscList.contains(vlm.getResourceDefinition().getName().value))
                            {
                                storPoolsToRemove.add(storPool);
                                break;
                            }
                        }
                    }
                    entry.getValue().removeAll(storPoolsToRemove);
                }

                // retry with the reduced storPools but without "do not place with resource" limitation
                candidates = filterCandidates(placeCount, Collections.emptyList(), storPools);
            }

            if (candidates.isEmpty())
            {
                addAnswer(
                    "Not enough available nodes",
                    null, // cause
                    "Not enough nodes fulfilling the following auto-place criteria:\n" +
                    " * has a deployed storage pool named '" + storPoolNameStr + "'\n" +
                    " * the storage pool '" + storPoolNameStr + "' has to have at least '" +
                    rscSize + "' free space\n" +
                    " * the current access context has enough privileges to use the node and the storage pool",
                    null, // correction.... "you must construct additional servers"
                    ApiConsts.FAIL_NOT_ENOUGH_NODES
                );
                throw new ApiCallHandlerFailedException();
            }
            else
            {
                // we might have a list of candidates and have to choose.
                Collections.sort(
                    candidates,
                    this::mostRemainingSpaceStrategy
                );

                Candidate candidate = candidates.get(0);

                Map<String, String> rscPropsMap = new TreeMap<>();
                rscPropsMap.put(ApiConsts.KEY_STOR_POOL_NAME, candidate.storPoolName.displayValue);

                for (Node node : candidate.nodes)
                {
                    rscApiCallHandler.createResource(
                        accCtx,
                        client,
                        node.getName().displayValue,
                        rscNameStr,
                        Collections.emptyList(),
                        rscPropsMap,
                        Collections.emptyList(),
                        currentTransMgr.get(),
                        apiCallRc
                    );
                }
                reportSuccess(
                    "Resource '" + rscNameStr + "' successfully autoplaced on " + placeCount + " nodes",
                    "Used storage pool: '" + candidate.storPoolName.displayValue + "'\n" +
                    "Used nodes: '" + candidate.nodes.stream()
                        .map(node -> node.getName().displayValue)
                        .collect(Collectors.joining("', '")) + "'"
                );
            }
        }
        catch (ApiCallHandlerFailedException ignore)
        {
            // a report and a corresponding api-response already created. nothing to do here
        }
        catch (Exception | ImplementationError exc)
        {
            reportStatic(
                exc,
                ApiCallType.CREATE,
                getObjectDescriptionInline(rscNameStr),
                getObjRefs(rscNameStr),
                getVariables(rscNameStr),
                apiCallRc,
                accCtx,
                client
            );
        }
        return apiCallRc;
    }

    private List<Candidate> filterCandidates(
        final int placeCount,
        List<String> notPlaceWithRscList,
        Map<StorPoolName, List<StorPool>> storPools
    )
    {
        List<Candidate> ret = new ArrayList<>();
        for (Entry<StorPoolName, List<StorPool>> entry: storPools.entrySet())
        {
            List<Node> nodeCandidates = entry.getValue().stream()
                .sorted((sp1, sp2) -> Long.compare(
                        getFreeSpace(sp1),
                        getFreeSpace(sp2)
                    )
                )
                .map(storPool -> storPool.getNode())
                .filter(node -> hasNoResourceOf(node, notPlaceWithRscList))
                .limit(placeCount)
                .collect(Collectors.toList());

            if (nodeCandidates.size() == placeCount)
            {
                ret.add(new Candidate(entry.getKey(), nodeCandidates));
            }
        }
        return ret;
    }

    private int mostRemainingSpaceStrategy(Candidate cand1, Candidate cand2)
    {
        // the node-lists are already sorted by their storPools.
        // that means, we only have to compare the freeSpace of the first nodes of cand1 and cand2
        int cmp = 0;
        try
        {
            cmp = Long.compare(
                cand2.nodes.get(0).getStorPool(currentAccCtx.get(), cand2.storPoolName)
                    .getFreeSpace(currentAccCtx.get()),
                cand1.nodes.get(0).getStorPool(currentAccCtx.get(), cand1.storPoolName)
                    .getFreeSpace(currentAccCtx.get())
            );
            // compare(cand2, cand1) so that the candidate with more free space comes before the other
        }
        catch (AccessDeniedException exc)
        {
            // this exception should have been thrown long ago
            throw asImplError(exc);
        }
        return cmp;
    }

    private long calculateResourceDefinitionSize(String rscNameStr)
    {
        long size = 0;
        try
        {
            ResourceDefinitionData rscDfn = loadRscDfn(rscNameStr, true);
            Iterator<VolumeDefinition> vlmDfnIt = rscDfn.iterateVolumeDfn(currentAccCtx.get());
            while (vlmDfnIt.hasNext())
            {
                VolumeDefinition vlmDfn = vlmDfnIt.next();
                size += vlmDfn.getVolumeSize(currentAccCtx.get());
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "access " + CtrlRscDfnApiCallHandler.getObjectDescriptionInline(rscNameStr),
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        return size;
    }

    private Iterator<StorPool> getStorPoolIterator(StorPoolDefinition storPoolDfn)
    {
        Iterator<StorPool> iterator;
        try
        {
            iterator = storPoolDfn.iterateStorPools(currentAccCtx.get());
        }
        catch (AccessDeniedException exc)
        {
            throw asAccDeniedExc(
                exc,
                "iterate storage pools of storage pool definition '" + storPoolDfn.getName().displayValue + "'.",
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL_DFN
            );
        }
        return iterator;
    }

    private long getFreeSpace(StorPool storPool)
    {
        long freeSpace;
        try
        {
            freeSpace = storPool.getFreeSpace(currentAccCtx.get());
        }
        catch (AccessDeniedException exc)
        {
            throw asAccDeniedExc(
                exc,
                "query free space of " + CtrlStorPoolApiCallHandler.getObjectDescriptionInline(storPool),
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL
            );
        }
        return freeSpace;
    }

    private boolean hasNoResourceOf(Node node, List<String> notPlaceWithRscList)
    {
        boolean hasNoResourceOf = false;
        try
        {
            Stream<Resource> deployedRscStream = StreamUtils.toStream(
                node.iterateResources(currentAccCtx.get())
            );
            hasNoResourceOf = deployedRscStream
                .map(rsc -> rsc.getDefinition().getName().value)
                .noneMatch(deployedRscStr -> notPlaceWithRscList.contains(deployedRscStr));
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
        return hasNoResourceOf;
    }

    private AbsApiCallHandler setContext(
        AccessContext accCtx,
        Peer peer,
        ApiCallType type,
        ApiCallRcImpl apiCallRc,
        TransactionMgr transMgr,
        String rscNameStr
    )
    {
        super.setContext(
            accCtx,
            peer,
            type,
            apiCallRc,
            transMgr,
            getObjRefs(rscNameStr),
            getVariables(rscNameStr)
        );
        currentRscName.set(rscNameStr);
        return this;
    }

    private Map<String, String> getObjRefs(String rscNameStr)
    {
        Map<String, String> map = new TreeMap<>();
        map.put(ApiConsts.KEY_RSC_DFN, rscNameStr);
        return map;
    }

    private Map<String, String> getVariables(String rscNameStr)
    {
        Map<String, String> map = new TreeMap<>();
        map.put(ApiConsts.KEY_RSC_NAME, rscNameStr);
        return map;
    }

    @Override
    protected String getObjectDescription()
    {
        return "Auto-placing resource: " + currentRscName.get();
    }

    @Override
    protected String getObjectDescriptionInline()
    {
        return getObjectDescriptionInline(currentRscName.get());
    }

    private String getObjectDescriptionInline(String rscNameStr)
    {
        return "auto-placing resource: '" + rscNameStr + "'";
    }

    private class Candidate
    {
        StorPoolName storPoolName;
        List<Node> nodes;

        Candidate(StorPoolName storPoolNameRef, List<Node> nodesRef)
        {
            storPoolName = storPoolNameRef;
            nodes = nodesRef;
        }
    }
}
