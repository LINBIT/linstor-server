package com.linbit.linstor.core.apicallhandler.controller.autohelper;

import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.api.pojo.AutoSelectFilterPojo;
import com.linbit.linstor.api.pojo.builder.AutoSelectFilterBuilder;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscAutoPlaceApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.Autoplacer;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.utils.PairNonNull;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

@Singleton
public class CtrlRscAutoRePlaceRscHelper implements AutoHelper
{
    private final SystemConfRepository systemConfRepo;
    /**
     * Set of ResourceDefinitions that need autoRePlacement. RscDfns might survive in this set over multiple
     * {@link #manage(AutoHelperContext)} calls, depending on whether or not the autoplacement succeeded.
     * This set will also be extended with {@link AutoHelperContext#needRePlaceRsc} if given/non-empty.
     */
    private final HashSet<ResourceDefinition> needRePlaceRsc = new HashSet<>();
    private final HashSet<ResourceDefinition> needDiskfulRsc = new HashSet<>();
    private final CtrlRscAutoPlaceApiCallHandler autoPlaceHandler;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final ErrorReporter errorReporter;

    private final Autoplacer autoplacer;

    @Inject
    public CtrlRscAutoRePlaceRscHelper(
        SystemConfRepository systemConfRepoRef,
        CtrlRscAutoPlaceApiCallHandler autoPlaceHandlerRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        ErrorReporter errorReporterRef,
        Autoplacer autoplacerRef
    )
    {
        systemConfRepo = systemConfRepoRef;
        autoPlaceHandler = autoPlaceHandlerRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        errorReporter = errorReporterRef;
        autoplacer = autoplacerRef;
    }

    @Override
    public AutoHelperType getType()
    {
        return AutoHelperType.AUTO_RE_PLACE;
    }

    @Override
    public void manage(AutoHelperContext ctx)
    {
        ResourceDefinition rscDfn = ctx.rscDfn;
        needRePlaceRsc.addAll(ctx.needRePlaceRsc);
        if (needRePlaceRsc.contains(rscDfn))
        {
            if (countDiskfulRsc(rscDfn) == 0)
            {
                if (!needDiskfulRsc.contains(rscDfn))
                {
                    needDiskfulRsc.add(rscDfn);
                    errorReporter.logWarning(
                        "There are no diskful resources of resource definition %s that are connected",
                        rscDfn.getName()
                    );
                }
            }
            else
            {
                PriorityProps props;
                int minReplicaCount;
                int placeCount;
                int curReplicaCount = 0;
                props = new PriorityProps(
                    rscDfn.getProps(),
                    rscDfn.getResourceGroup().getProps(),
                    systemConfRepo.getCtrlConfForView()
                );
                placeCount = rscDfn.getResourceGroup().getAutoPlaceConfig().getReplicaCount();
                minReplicaCount = Integer.parseInt(
                    props.getProp(
                        ApiConsts.KEY_AUTO_EVICT_MIN_REPLICA_COUNT,
                        ApiConsts.NAMESPC_DRBD_OPTIONS,
                        "" + placeCount
                    )
                );
                if (placeCount < minReplicaCount) // minReplicaCount should be smaller than placeCount
                {
                    minReplicaCount = placeCount;
                }
                List<String> disklessNodeNames = new ArrayList<>();
                Iterator<Resource> itres = rscDfn.iterateResource();
                while (itres.hasNext())
                {
                    Resource res = itres.next();
                    if (LayerRscUtils.getLayerStack(res).contains(DeviceLayerKind.DRBD) &&
                        !res.getNode().getFlags().isSet(Node.Flags.EVICTED)
                    )
                    {
                        StateFlags<Flags> flags = res.getStateFlags();
                        boolean countAsActive = !flags.isSet(Resource.Flags.DELETE) &&
                            !flags.isSet(Resource.Flags.INACTIVE);
                        boolean isDiskless = flags.isSet(Resource.Flags.DRBD_DISKLESS);
                        if (isDiskless)
                        {
                            disklessNodeNames.add(res.getNode().getName().displayValue);
                        }
                        else if (countAsActive)
                        {
                            curReplicaCount++;
                        }
                    }
                }
                if (curReplicaCount < minReplicaCount)
                {
                    AutoSelectFilterApi selectFilter = AutoSelectFilterPojo.merge(
                        new AutoSelectFilterBuilder()
                            .setAdditionalPlaceCount(minReplicaCount - curReplicaCount)
                            .setDoNotPlaceWithRscList(Collections.singletonList(rscDfn.getName().displayValue))
                            .setLayerStackList(Collections.singletonList(DeviceLayerKind.DRBD))
                            .setSkipAlreadyPlacedOnNodeNamesCheck(disklessNodeNames)
                            .build(),
                        rscDfn.getResourceGroup().getAutoPlaceConfig().getApiData()
                    );
                    try
                    {
                        Flux<ApiCallRc> flux = scopeRunner.fluxInTransactionalScope(
                            "evict resources",
                            lockGuardFactory.createDeferred().write(LockObj.RSC_DFN_MAP).build(),
                            () ->
                            {
                                Iterator<Resource> itr = rscDfn.iterateResource();
                                List<NodeName> nodeNameOfEvictedResources = new ArrayList<>();
                                while (itr.hasNext())
                                {
                                    Resource rsc = itr.next();
                                    StateFlags<Flags> rscFlags = rsc.getStateFlags();
                                    if (rsc.getNode().getFlags().isSet(Node.Flags.EVICTED) &&
                                        !rscFlags.isSet(Resource.Flags.EVICTED))
                                    {
                                        if (rscFlags.isSet(Resource.Flags.INACTIVE))
                                        {
                                            rscFlags.enableFlags(
                                                Resource.Flags.INACTIVE_BEFORE_EVICTION
                                            );
                                        }
                                        rscFlags.enableFlags(Resource.Flags.EVICTED);
                                    }
                                }
                                ctrlTransactionHelper.commit();
                                Flux<Tuple2<NodeName, Flux<ApiCallRc>>> updateFlux = ctrlSatelliteUpdateCaller
                                    .updateSatellites(rscDfn, ignored -> Flux.empty(), Flux.empty());
                                return updateFlux.transform(
                                    updateResponses -> CtrlResponseUtils.combineResponses(
                                        errorReporter,
                                        updateResponses,
                                        rscDfn.getName(),
                                        nodeNameOfEvictedResources,
                                        "Resource {1} was evicted from {0}",
                                        "Notified {0} about evicting resource {1} from node(s) " +
                                            nodeNameOfEvictedResources
                                    )
                                );
                            }
                        );
                        long size = getVlmSize(rscDfn);
                        errorReporter.logDebug(
                            "Auto-evict: Auto-placing '%s' on %d additional nodes",
                            rscDfn.getName(),
                            minReplicaCount - curReplicaCount
                        );
                        Set<StorPool> candidate = autoplacer.autoPlace(selectFilter, rscDfn, size);
                        if (candidate != null)
                        {
                            PairNonNull<List<Flux<ApiCallRc>>, Set<Resource>> deployedResources = autoPlaceHandler
                                .createResources(
                                    new ResponseContext(
                                        ApiOperation.makeDeleteOperation(),
                                        "Auto-evicting resource: " + rscDfn.getName(),
                                        "auto-evicting resource: " + rscDfn.getName(),
                                        ApiConsts.MASK_DEL,
                                        new HashMap<>()
                                    ),
                                    new ApiCallRcImpl(),
                                    rscDfn.getName().toString(),
                                    selectFilter.getDisklessOnRemaining(),
                                    candidate,
                                    null,
                                    selectFilter.getLayerStackList(),
                                    selectFilter.getDrbdPortCount(),
                                    false // we want diskful replacements for our evicted resource, not diskless.
                                );
                            ctrlTransactionHelper.commit();
                            ctx.additionalFluxList
                                .add(Flux.merge(deployedResources.objA)
                                    .concatWith(flux)
                                    .doOnComplete(() ->
                                    {
                                        needRePlaceRsc.remove(rscDfn);
                                        needDiskfulRsc.remove(rscDfn);
                                    })
                                );
                            ctx.requiresUpdateFlux = true;
                        }
                        else
                        {
                            errorReporter.logWarning(
                                "Not enough space on nodes for eviction of resource %s", rscDfn.getName()
                            );
                        }
                    }
                    catch (ApiRcException exc)
                    {
                        // Ignored, try again later
                    }
                }
                else
                {
                    needRePlaceRsc.remove(rscDfn);
                    needDiskfulRsc.remove(rscDfn);
                }
            }
        }
    }

    private int countDiskfulRsc(ResourceDefinition rscDfn)
    {
        int ct = 0;
        for (Resource rsc : rscDfn.streamResource().collect(Collectors.toList()))
        {
            if (rsc.getNode().getPeer().isOnline() &&
                !rsc.getStateFlags().isSet(Resource.Flags.DRBD_DISKLESS))
            {
                ct++;
            }
        }
        return ct;
    }

    private long getVlmSize(ResourceDefinition rscDfn)
    {
        long size = 0;
        Iterator<VolumeDefinition> vlmDfnIt = rscDfn.iterateVolumeDfn();
        while (vlmDfnIt.hasNext())
        {
            VolumeDefinition vlmDfn = vlmDfnIt.next();
            size += vlmDfn.getVolumeSize();
        }
        return size;
    }
}
