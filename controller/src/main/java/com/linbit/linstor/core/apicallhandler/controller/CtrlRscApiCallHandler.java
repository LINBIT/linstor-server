package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.RscConnPojo;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlPropsHelper.PropertyChangedListener;
import com.linbit.linstor.core.apicallhandler.controller.helpers.PropsChangedListenerBuilder;
import com.linbit.linstor.core.apicallhandler.controller.helpers.ResourceList;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.apis.ResourceConnectionApi;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.ResourceConnectionKey;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.satellitestate.SatelliteState;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuardFactory;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.getRscDfnDescriptionInline;
import static com.linbit.locks.LockGuardFactory.LockObj.NODES_MAP;
import static com.linbit.locks.LockGuardFactory.LockObj.RSC_DFN_MAP;
import static com.linbit.locks.LockGuardFactory.LockObj.STOR_POOL_DFN_MAP;
import static com.linbit.locks.LockGuardFactory.LockType.WRITE;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.locks.Lock;

import reactor.core.publisher.Flux;

import static java.util.stream.Collectors.toList;

@Singleton
public class CtrlRscApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final ResourceDefinitionRepository resourceDefinitionRepository;
    private final NodeRepository nodeRepository;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final ResponseConverter responseConverter;
    private final Provider<Peer> peer;
    private final Provider<AccessContext> peerAccCtx;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final StltConfigAccessor stltCfgAccessor;
    private final Provider<PropsChangedListenerBuilder> propsChangeListenerBuilderProvider;

    @Inject
    public CtrlRscApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        ResourceDefinitionRepository resourceDefinitionRepositoryRef,
        NodeRepository nodeRepositoryRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ResponseConverter responseConverterRef,
        Provider<Peer> peerRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        StltConfigAccessor stltCfgAccessorRef,
        Provider<PropsChangedListenerBuilder> propsChangeListenerBuilderProviderRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        resourceDefinitionRepository = resourceDefinitionRepositoryRef;
        nodeRepository = nodeRepositoryRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        responseConverter = responseConverterRef;
        peer = peerRef;
        peerAccCtx = peerAccCtxRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        stltCfgAccessor = stltCfgAccessorRef;
        propsChangeListenerBuilderProvider = propsChangeListenerBuilderProviderRef;
    }

    public Flux<ApiCallRc> modify(
        @Nullable UUID rscUuid,
        String nodeNameStr,
        String rscNameStr,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        Set<String> deletePropNamespaces
    )
    {
        ResponseContext context = makeRscContext(
            ApiOperation.makeModifyOperation(),
            nodeNameStr,
            rscNameStr
        );

        return scopeRunner
            .fluxInTransactionalScope(
                "Modify resource",
                lockGuardFactory.buildDeferred(
                    WRITE,
                    NODES_MAP, RSC_DFN_MAP, STOR_POOL_DFN_MAP
                ),
                () -> modifyInTransaction(
                    rscUuid,
                    nodeNameStr,
                    rscNameStr,
                    overrideProps,
                    deletePropKeys,
                    deletePropNamespaces,
                    context
                )
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> modifyInTransaction(
        @Nullable UUID rscUuid,
        String nodeNameStr,
        String rscNameStr,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        Set<String> deletePropNamespacesRef,
        ResponseContext context
    )
    {
        Flux<ApiCallRc> flux = Flux.empty();
        ApiCallRcImpl apiCallRcs = new ApiCallRcImpl();
        boolean notifyStlts = false;

        List<Flux<ApiCallRc>> specialPropFluxes = new ArrayList<>();
        try
        {
            Resource rsc = ctrlApiDataLoader.loadRsc(nodeNameStr, rscNameStr, true);

            if (rscUuid != null && !rscUuid.equals(rsc.getUuid()))
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UUID_RSC,
                    "UUID-check failed"
                ));
            }

            Map<String, PropertyChangedListener> propsChangedListeners = propsChangeListenerBuilderProvider.get()
                .buildPropsChangedListeners(peerAccCtx.get(), rsc, specialPropFluxes);

            Props props = ctrlPropsHelper.getProps(rsc);

            // check if specified preferred network interface exists
            ctrlPropsHelper.checkPrefNic(
                apiCtx,
                rsc.getNode(),
                overrideProps.get(ApiConsts.KEY_STOR_POOL_PREF_NIC),
                ApiConsts.MASK_RSC
            );
            ctrlPropsHelper.checkPrefNic(
                apiCtx,
                rsc.getNode(),
                overrideProps.get(ApiConsts.NAMESPC_NVME + "/" + ApiConsts.KEY_PREF_NIC),
                ApiConsts.MASK_RSC
            );

            notifyStlts = ctrlPropsHelper.fillProperties(
                apiCallRcs,
                LinStorObject.RSC,
                overrideProps,
                props,
                ApiConsts.FAIL_ACC_DENIED_RSC,
                Collections.emptyList(),
                propsChangedListeners
            ) || notifyStlts;
            notifyStlts = ctrlPropsHelper.remove(
                apiCallRcs,
                LinStorObject.RSC,
                props,
                deletePropKeys,
                deletePropNamespacesRef,
                Collections.emptyList(),
                propsChangedListeners
            ) || notifyStlts;

            ctrlTransactionHelper.commit();

            responseConverter.addWithOp(apiCallRcs, context, ApiSuccessUtils.defaultModifiedEntry(
                rsc.getUuid(), getRscDescriptionInline(rsc)));

            if (notifyStlts)
            {
                flux = ctrlSatelliteUpdateCaller
                        .updateSatellites(rsc.getResourceDefinition(), Flux.empty())
                        .flatMap(updateTuple -> updateTuple == null ? Flux.empty() : updateTuple.getT2());
            }
        }
        catch (Exception | ImplementationError exc)
        {
            apiCallRcs = responseConverter.reportException(peer.get(), context, exc);
        }

        return Flux.just((ApiCallRc) apiCallRcs)
            .concatWith(flux)
            .concatWith(Flux.merge(specialPropFluxes));
    }

    ResourceList listResources(
        String rscNameStr,
        List<String> filterNodes
    )
    {
        // fake load and fail if not exists
        ctrlApiDataLoader.loadRscDfn(rscNameStr, true);

        List<String> rscList = new ArrayList<>();
        rscList.add(rscNameStr);
        return listResources(filterNodes, rscList);
    }

    ResourceList listResources(
        List<String> filterNodes,
        List<String> filterResources
    )
    {
        final ResourceList rscList = new ResourceList();
        try
        {
            final List<String> upperFilterNodes = filterNodes.stream().map(String::toUpperCase).collect(toList());
            final List<String> upperFilterResources =
                filterResources.stream().map(String::toUpperCase).collect(toList());

            resourceDefinitionRepository.getMapForView(peerAccCtx.get()).values().stream()
                .filter(rscDfn -> upperFilterResources.isEmpty() ||
                    upperFilterResources.contains(rscDfn.getName().value))
                .forEach(rscDfn ->
                {
                    try
                    {
                        for (Resource rsc : rscDfn.streamResource(peerAccCtx.get())
                            .filter(rsc -> upperFilterNodes.isEmpty() ||
                                upperFilterNodes.contains(rsc.getNode().getName().value))
                            .collect(toList()))
                        {
                            rscList.addResource(
                                rsc.getApiData(
                                    peerAccCtx.get(),
                                    null,
                                    null,
                                    rsc.getEffectiveProps(apiCtx, stltCfgAccessor)
                                )
                            );
                            // fullSyncId and updateId null, as they are not going to be serialized anyways
                        }
                    }
                    catch (AccessDeniedException accDeniedExc)
                    {
                        // don't add storpooldfn without access
                    }
                }
                );

            // get resource states of all nodes
            for (final Node node : nodeRepository.getMapForView(peerAccCtx.get()).values())
            {
                if (upperFilterNodes.isEmpty() || upperFilterNodes.contains(node.getName().value))
                {
                    final Peer curPeer = node.getPeer(peerAccCtx.get());
                    if (curPeer != null)
                    {
                        Lock readLock = curPeer.getSatelliteStateLock().readLock();
                        readLock.lock();
                        try
                        {
                            final SatelliteState satelliteState = curPeer.getSatelliteState();

                            if (satelliteState != null)
                            {
                                final SatelliteState filterStates = new SatelliteState(satelliteState);

                                // states are already complete, we remove all resource that are not interesting from
                                // our clone
                                Set<ResourceName> removeSet = new TreeSet<>();
                                for (ResourceName rscName : filterStates.getResourceStates().keySet())
                                {
                                    if (!(upperFilterResources.isEmpty() ||
                                          upperFilterResources.contains(rscName.value)))
                                    {
                                        removeSet.add(rscName);
                                    }
                                }
                                removeSet.forEach(rscName -> filterStates.getResourceStates().remove(rscName));
                                rscList.putSatelliteState(node.getName(), filterStates);
                            }
                        }
                        finally
                        {
                            readLock.unlock();
                        }
                    }
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            // for now return an empty list.
            errorReporter.reportError(accDeniedExc);
        }

        return rscList;
    }

    List<ResourceConnectionApi> listResourceConnections(final String rscNameString)
    {
        ResourceName rscName = null;
        List<ResourceConnectionApi> rscConns = new ArrayList<>();
        try
        {
            rscName = new ResourceName(rscNameString);
            ResourceDefinition rscDfn = resourceDefinitionRepository.get(apiCtx, rscName);

            if (rscDfn  != null)
            {
                final Map<ResourceConnectionKey, ResourceConnectionApi> rscConMap = new TreeMap<>();

                // Build an array of all resources of the resource definition
                Resource[] rscList = new Resource[rscDfn.getResourceCount()];
                {
                    Iterator<Resource> rscIter = rscDfn.iterateResource(apiCtx);
                    for (int idx = 0; rscIter.hasNext(); ++idx)
                    {
                        rscList[idx] = rscIter.next();
                    }
                }

                // Collect resource connection from all resources, avoiding duplicates
                for (Resource rsc : rscList)
                {
                    List<ResourceConnection> rscConList = rsc.getAbsResourceConnections(apiCtx);
                    for (ResourceConnection rscCon : rscConList)
                    {
                        ResourceConnectionKey rscConKey = new ResourceConnectionKey(
                            rscCon.getSourceResource(apiCtx), rscCon.getTargetResource(apiCtx)
                        );
                        if (!rscConMap.containsKey(rscConKey))
                        {
                            rscConMap.put(rscConKey, rscCon.getApiData(apiCtx));
                        }
                    }
                }

                // Construct empty resource connections for all resource pairs that did not have
                // a resource connection defined already
                for (int outerIdx = 0; outerIdx < rscList.length; ++outerIdx)
                {
                    for (int innerIdx = outerIdx + 1; innerIdx < rscList.length; ++innerIdx)
                    {
                        ResourceConnectionKey rscConKey = new ResourceConnectionKey(
                            rscList[outerIdx], rscList[innerIdx]
                        );
                        if (!rscConMap.containsKey(rscConKey))
                        {
                            rscConMap.put(
                                rscConKey,
                                new RscConnPojo(
                                    UUID.randomUUID(),
                                    rscConKey.getSourceNodeName().getDisplayName(),
                                    rscConKey.getTargetNodeName().getDisplayName(),
                                    rscDfn.getName().getDisplayName(),
                                    new HashMap<>(),
                                    0,
                                    null,
                                    null
                                )
                            );
                        }
                    }
                }

                rscConns.addAll(rscConMap.values());
            }
            else
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_NOT_FOUND_RSC_DFN,
                        String.format("Resource definition '%s' not found.", rscNameString)
                    )
                );
            }
        }
        catch (InvalidNameException exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_RSC_NAME,
                    "Invalid resource name used"
                ),
            exc);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access " + getRscDfnDescriptionInline(rscName.displayValue),
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }

        return rscConns;
    }

    public static String getRscDescription(Resource resource)
    {
        return getRscDescription(
            resource.getNode().getName().displayValue, resource.getResourceDefinition().getName().displayValue);
    }

    public static String getRscDescription(String nodeNameStr, String rscNameStr)
    {
        return "Node: " + nodeNameStr + ", Resource: " + rscNameStr;
    }

    public static String getRscDescriptionInline(Resource rsc)
    {
        return getRscDescriptionInline(rsc.getNode(), rsc.getResourceDefinition());
    }

    public static String getRscDescriptionInline(Node node, ResourceDefinition rscDfn)
    {
        return getRscDescriptionInline(node.getName().displayValue, rscDfn.getName().displayValue);
    }

    public static String getRscDescriptionInline(String nodeNameStr, String rscNameStr)
    {
        return "resource '" + rscNameStr + "' on node '" + nodeNameStr + "'";
    }

    static ResponseContext makeRscContext(
        ApiOperation operation,
        String nodeNameStr,
        String rscNameStr
    )
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_NODE, nodeNameStr);
        objRefs.put(ApiConsts.KEY_RSC_DFN, rscNameStr);

        return new ResponseContext(
            operation,
            getRscDescription(nodeNameStr, rscNameStr),
            getRscDescriptionInline(nodeNameStr, rscNameStr),
            ApiConsts.MASK_RSC,
            objRefs
        );
    }
}
