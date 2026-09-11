package com.linbit.linstor.core.apicallhandler.controller.autohelper;

import com.linbit.ImplementationError;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.api.pojo.AutoSelectFilterPojo;
import com.linbit.linstor.api.pojo.builder.AutoSelectFilterBuilder;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscCrtApiHelper;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscToggleDiskApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscToggleDiskApiCallHandler.ToggleOp;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.Autoplacer;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.SelectionException;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.SelectionManager;
import com.linbit.linstor.core.apicallhandler.controller.utils.ResourceDataUtils;
import com.linbit.linstor.core.apicallhandler.controller.utils.ResourceDataUtils.DrbdResourceResult;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.resource.CtrlRscLayerDataFactory;
import com.linbit.linstor.layer.storage.BlockSizeConsts;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.utils.layer.DrbdLayerUtils;
import com.linbit.linstor.utils.layer.LayerVlmUtils;
import com.linbit.locks.LockGuard;

import static com.linbit.linstor.api.ApiConsts.KEY_DRBD_AUTO_ADD_QUORUM_TIEBREAKER;
import static com.linbit.linstor.api.ApiConsts.NAMESPC_DRBD_OPTIONS;
import static com.linbit.linstor.api.ApiConsts.VAL_FALSE;
import static com.linbit.linstor.api.ApiConsts.VAL_TRUE;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.getRscDfnDescriptionInline;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Predicate;

import reactor.core.publisher.Flux;

@Singleton
class CtrlRscAutoTieBreakerHelper implements AutoHelper
{
    private static final Autoplacer.StorPoolWithScore[] NO_SORTED_SPS = new Autoplacer.StorPoolWithScore[0];
    private final SystemConfRepository systemConfRepository;
    private final CtrlRscLayerDataFactory layerDataHelper;
    private final CtrlRscCrtApiHelper rscCrtApiHelper;
    private final ScopeRunner scopeRunner;
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock rscDfnMapLock;
    private final ResponseConverter responseConverter;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlRscToggleDiskApiCallHandler rscToggleDiskHelper;
    private final Autoplacer autoplacer;

    private final ErrorReporter errorReporter;

    @Inject
    CtrlRscAutoTieBreakerHelper(
        SystemConfRepository systemConfRepositoryRef,
        ScopeRunner scopeRunnerRef,
        CtrlRscLayerDataFactory layerDataHelperRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        CtrlRscCrtApiHelper rscCrtApiHelperRef,
        ResponseConverter responseConverterRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlRscToggleDiskApiCallHandler rscToggleDiskHelperRef,
        Autoplacer autoplacerRef,
        ErrorReporter errorReporterRef
    )
    {
        systemConfRepository = systemConfRepositoryRef;
        scopeRunner = scopeRunnerRef;
        layerDataHelper = layerDataHelperRef;
        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        rscCrtApiHelper = rscCrtApiHelperRef;
        responseConverter = responseConverterRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        rscToggleDiskHelper = rscToggleDiskHelperRef;
        autoplacer = autoplacerRef;
        errorReporter = errorReporterRef;
    }

    @Override
    public AutoHelperType getType()
    {
        return AutoHelperType.TIEBREAKER;
    }

    @Override
    public void manage(AutoHelperContext ctx)
    {
        try
        {
            boolean isAutoTieBreakerEnabled = isAutoTieBreakerEnabled(ctx.rscDfn);
            if (!isAutoTieBreakerEnabled && ctx.keepTiebreaker)
            {
                enableAutoTieBreaker(ctx);
                isAutoTieBreakerEnabled = true;
            }
            @Nullable Resource tieBreaker = getTieBreaker(ctx.rscDfn);
            boolean shouldTieBreakerExist = isAutoTieBreakerEnabled && shouldTieBreakerExist(ctx.rscDfn);
            if (shouldTieBreakerExist)
            {
                if (tieBreaker == null)
                {
                    Resource takeover = null;
                    Iterator<Resource> rscIt = ctx.rscDfn.iterateResource();
                    while (rscIt.hasNext())
                    {
                        Resource rsc = rscIt.next();
                        if (isSomeFlagSet(rsc, Resource.Flags.DRBD_DELETE, Resource.Flags.DELETE) &&
                            !isSomeFlagSet(rsc.getNode(), Node.Flags.EVICTED, Node.Flags.EVACUATE) &&
                            couldTakeover(rsc, ctx))
                        {
                            if (isFlagSet(rsc, Resource.Flags.DRBD_DISKLESS))
                            {
                                takeover = rsc;
                                break;
                            }
                            else
                            {
                                takeover = rsc;
                                // keep looking for diskless resource
                            }
                        }
                    }
                    if (takeover != null)
                    {
                        takeover(takeover, ctx);
                    }
                    else
                    {
                        Set<String> relaxedReplicasOnSameKeys = new HashSet<>();
                        @Nullable StorPool storPool = getStorPoolForTieBreaker(ctx, null, relaxedReplicasOnSameKeys);
                        if (storPool == null)
                        {
                            ctx.responses.addEntries(
                                ApiCallRcImpl.singleApiCallRc(
                                    ApiConsts.WARN_NOT_ENOUGH_NODES_FOR_TIE_BREAKER,
                                    String.format(
                                        "Could not find suitable node to automatically create a tie breaking " +
                                            "resource for '%s'.",
                                        ctx.rscDfn.getName().displayValue
                                    )
                                )
                            );
                        }
                        else
                        {
                            if (!relaxedReplicasOnSameKeys.isEmpty())
                            {
                                ctx.responses.addEntries(
                                    ApiCallRcImpl.singleApiCallRc(
                                        ApiConsts.WARN_TIE_BREAKER_RULE_RELAXED,
                                        String.format(
                                            "Tie breaker for '%s' is being placed on node '%s' while ignoring the " +
                                                "--replicas-on-same rule(s) %s, because the already deployed " +
                                                "resources already violate that rule. The tie breaker should be " +
                                                "moved to a compliant node once the violation is resolved.",
                                            ctx.rscDfn.getName().displayValue,
                                            storPool.getNode().getName().displayValue,
                                            relaxedReplicasOnSameKeys
                                        )
                                    )
                                );
                            }
                            // we can ignore the .objA of the returned pair as we are just about to create
                            // a tiebreaker
                            tieBreaker = rscCrtApiHelper.createResourceDb(
                                storPool.getNode().getName().displayValue,
                                ctx.rscDfn.getName().displayValue,
                                Resource.Flags.TIE_BREAKER.flagValue,
                                Collections.singletonMap(
                                    ApiConsts.KEY_STOR_POOL_NAME,
                                    storPool.getName().displayValue
                                ),
                                Collections.emptyList(),
                                null,
                                null,
                                null,
                                Collections.emptyMap(),
                                Collections.emptyList(),
                                null,
                                // tiebreaker should never be client-only. that would go entirely against the purpose
                                // of tiebreaker :)
                                false
                            ).objB.extractApiCallRc(ctx.responses);

                            ctx.responses.addEntries(
                                ApiCallRcImpl.singleApiCallRc(
                                    ApiConsts.INFO_TIE_BREAKER_CREATED,
                                    String.format(
                                        "Tie breaker resource '%s' created on %s",
                                        ctx.rscDfn.getName().displayValue,
                                        storPool.getNode().getName().displayValue
                                    )
                                )
                            );

                            ctx.resourcesToCreate.add(tieBreaker);
                            // adding a rsc to resourcesToCreate will be handled with .deployResources which also
                            // includes an updateSatellites in the end. in other words, we do not need
                            // to set requiresUpdateFlux to true here
                        }
                    }
                }
                else
                {
                    if (isFlagSet(tieBreaker, Resource.Flags.DRBD_DELETE) &&
                        tieBreaker.getNode().getFlags().isUnset(Node.Flags.EVACUATE))
                    {
                        // user requested to delete tiebreaker.
                        if (ctx.keepTiebreaker)
                        {
                            // however, --keep-tiebreaker overrules this deletion now
                            // the takeover will remove the DELETE flags
                            takeover(tieBreaker, ctx);
                        }
                        else
                        {
                            tieBreaker.getResourceDefinition()
                                .getProps()
                                .setProp(
                                    KEY_DRBD_AUTO_ADD_QUORUM_TIEBREAKER,
                                    VAL_FALSE,
                                    NAMESPC_DRBD_OPTIONS
                                );
                            ctx.responses.addEntries(
                                ApiCallRcImpl.singleApiCallRc(
                                    ApiConsts.INFO_PROP_SET,
                                    "Disabling auto-tiebreaker on resource-definition '" +
                                        tieBreaker.getResourceDefinition().getName() +
                                        "' as tiebreaker resource was manually deleted"
                                )
                            );
                        }
                    }
                }
            }
            else
            {
                if (tieBreaker != null)
                {
                    Peer tiePeer = tieBreaker.getNode().getPeer();
                    // only delete the tiebreaker if node is online
                    if (tiePeer.isFullSyncApplied() && tiePeer.isOnline())
                    {
                        // this cannot be the last diskful rsc of any rscDfn, so no need to notify scheduled shipping
                        tieBreaker.markDeleted();
                        ctx.responses.addEntries(
                            ApiCallRcImpl.singleApiCallRc(
                                ApiConsts.INFO_TIE_BREAKER_DELETING,
                                "Tie breaker marked for deletion"
                            )
                        );

                        ctx.nodeNamesForDelete.add(tieBreaker.getNode().getName());
                        // adding a rsc to nodeNamesForDelete will be handled with .updateSatellitesForResourceDelete
                        // which also includes an updateSatellites in the end. in other words, we do not need
                        // to set requiresUpdateFlux to true here
                    }
                }
            }
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
        catch (InvalidKeyException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private boolean couldTakeover(Resource rscRef, AutoHelperContext ctxRef)
    {
        boolean couldTakeover = false;

        if (rscRef.getStateFlags().isSet(Resource.Flags.DRBD_DISKLESS))
        {
            final ResourceDefinition rscDfn = ctxRef.rscDfn;
            final Predicate<StorPool> isEligibleTieBreakerStorPool = getEligibleTieBreakerStorPoolPredicate(rscDfn);
            couldTakeover = LayerVlmUtils.getStorPools(rscRef)
                .stream()
                .allMatch(isEligibleTieBreakerStorPool);
        }
        else
        {
            // will be null if rscRef already violates some RG settings. This rsc should not be taken over, but rather
            // find a new node for the tiebreaker
            @Nullable StorPool storPoolForTieBreaker = getStorPoolForTieBreaker(
                ctxRef,
                Collections.singleton(rscRef.getNode()),
                null
            );
            couldTakeover = storPoolForTieBreaker != null;
        }
        return couldTakeover;
    }

    private void takeover(
        Resource rsc,
        AutoHelperContext ctx
    )
    {
        StateFlags<Flags> flags = rsc.getStateFlags();
        try
        {
            flags.disableFlags(Resource.Flags.DRBD_DELETE, Resource.Flags.DELETE);
            DrbdLayerUtils.setClientFlag(rsc, false);

            Iterator<Volume> vlmsIt = rsc.iterateVolumes();
            while (vlmsIt.hasNext())
            {
                Volume vlm = vlmsIt.next();
                vlm.getFlags().disableFlags(Volume.Flags.DRBD_DELETE, Volume.Flags.DELETE);
            }

            // just to be sure
            ResourceDataUtils.recalculateVolatileRscData(layerDataHelper, rsc);

            if (flags.isSet(Resource.Flags.DRBD_DISKLESS))
            {
                ctx.additionalFluxList.add(setTiebreakerFlag(rsc));
                ctx.requiresUpdateFlux = true;
                ctx.preventUpdateSatellitesForResourceDelete = true;
            }
            else
            {
                ctx.additionalFluxList.add(
                    rscToggleDiskHelper.resourceToggleDisk(
                        rsc.getNode().getName().displayValue,
                        rsc.getResourceDefinition().getName().displayValue,
                        null,
                        null,
                        null,
                        ToggleOp.INTO_DRBD_TIEBREAKER,
                        null
                    )
                );

                ctx.preventUpdateSatellitesForResourceDelete = true;
                ctx.requiresUpdateFlux = false; // resourceToggleDisk already performs stltUpdate
            }
            ctx.responses.addEntries(
                ApiCallRcImpl.singleApiCallRc(
                    ApiConsts.INFO_TIE_BREAKER_TAKEOVER,
                    "The given resource will not be deleted but will be taken over as a " +
                        "linstor managed tiebreaker resource."
                )
            );
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
    }

    private boolean isAutoTieBreakerEnabled(ResourceDefinition rscDfn)
    {
        boolean autoTieBreakerEnabled;
        try
        {
            String autoTieBreakerProp = getPrioProps(rscDfn)
                .getProp(KEY_DRBD_AUTO_ADD_QUORUM_TIEBREAKER, NAMESPC_DRBD_OPTIONS);
            autoTieBreakerEnabled = ApiConsts.VAL_TRUE.equalsIgnoreCase(autoTieBreakerProp);
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        return autoTieBreakerEnabled;
    }

    private void enableAutoTieBreaker(AutoHelperContext ctxRef) throws DatabaseException
    {
        try
        {
            ctxRef.rscDfn.getProps()
                .setProp(KEY_DRBD_AUTO_ADD_QUORUM_TIEBREAKER, VAL_TRUE, NAMESPC_DRBD_OPTIONS);
            ctxRef.responses.add(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.INFO_PROP_SET,
                    "Autotiebreaker automatically enabled due to --keep-tiebreaker"
                )
            );
        }
        catch (InvalidKeyException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private @Nullable Resource getTieBreaker(ResourceDefinition rscDfn)
    {
        Resource tieBreaker = null;
        Iterator<Resource> rscIt = rscDfn.iterateResource();
        while (rscIt.hasNext())
        {
            Resource rsc = rscIt.next();
            if (rsc.getStateFlags().isSet(Resource.Flags.TIE_BREAKER))
            {
                tieBreaker = rsc;
                break;
            }
        }
        return tieBreaker;
    }

    private boolean shouldTieBreakerExist(ResourceDefinition rscDfn)
    {
        long diskfulDrbdCount = 0;
        long disklessDrbdCount = 0;

        if (CtrlRscAutoQuorumHelper.isQuorumEnabled(getPrioProps(rscDfn)) ||
            CtrlRscAutoQuorumHelper.isAutoQuorumEnabled(getPrioProps(rscDfn)))
        {
            Predicate<StorPool> isEligibleTieBreakerStorPool = getEligibleTieBreakerStorPoolPredicate(rscDfn);

            Iterator<Resource> rscIt = rscDfn.iterateResource();
            while (rscIt.hasNext())
            {
                Resource rsc = rscIt.next();

                StateFlags<Resource.Flags> rscFlags = rsc.getStateFlags();

                if (isDrbdResource(rsc))
                {
                    if (rscFlags.isSet(Resource.Flags.DRBD_DISKLESS))
                    {
                        boolean eligibleStoragePool = LayerVlmUtils.getStorPools(rsc)
                            .stream()
                            .allMatch(isEligibleTieBreakerStorPool);
                        if (eligibleStoragePool && !rscFlags.isSet(Resource.Flags.TIE_BREAKER) &&
                            !DrbdLayerUtils.isDrbdClient(rsc))
                        {
                            // We only count non-tie-breaker diskless resource, which are also "eligible"
                            // to be tiebreakers, i.e. the autoplacer could have chosen this node, assuming
                            // it only saw the diskful resources.
                            disklessDrbdCount++;
                        }
                    }
                    else
                    {
                        diskfulDrbdCount++;
                    }
                }
            }
        }
        return diskfulDrbdCount >= 2 && diskfulDrbdCount % 2 == 0 && disklessDrbdCount == 0;
    }

    private Predicate<StorPool> getEligibleTieBreakerStorPoolPredicate(ResourceDefinition rscDfn)
    {
        List<Node> alreadyDeployedDiskfulNodes = new ArrayList<>();

        Iterator<Resource> rscIt = rscDfn.iterateResource();
        while (rscIt.hasNext())
        {
            Resource rsc = rscIt.next();

            if (!rsc.isDiskless())
            {
                alreadyDeployedDiskfulNodes.add(rsc.getNode());
            }
        }

        AutoSelectFilterPojo mergedAutoSelectFilter = AutoSelectFilterPojo.merge(
            new AutoSelectFilterBuilder()
                .setPlaceCount(0)
                .setAdditionalPlaceCount(1)
                .setDoNotPlaceWithRscList(Collections.singletonList(rscDfn.getName().displayValue))
                .setLayerStackList(Collections.singletonList(DeviceLayerKind.DRBD))
                .setDisklessType(Resource.Flags.DRBD_DISKLESS.name())
                .build(),
            rscDfn.getResourceGroup().getAutoPlaceConfig().getApiData()
        );

        Predicate<StorPool> ret;
        try
        {
            final SelectionManager selectionManager = new SelectionManager(
                errorReporter,
                mergedAutoSelectFilter,
                alreadyDeployedDiskfulNodes,
                alreadyDeployedDiskfulNodes.size(),
                0,
                Collections.emptyList(),
                Collections.emptyMap(),
                NO_SORTED_SPS,
                false, // not that it matters for tiebreaker selection
                true,
                BlockSizeConsts.DFLT_PHY_IO_SIZE
            );
            ret = selectionManager::isAllowed;
        }
        catch (SelectionException selExc)
        {
            // The resource group's autoplace rules are currently undecidable - e.g. a --replicas-on-same
            // property is already violated by the deployed diskful resources. This eligibility check must never
            // abort the operation that triggered it (e.g. a manual 'resource create'), so we degrade gracefully
            // and treat no storage pool as an eligible tiebreaker location. Actually placing a tiebreaker in such
            // a situation - by relaxing the offending rule - is handled separately in getStorPoolForTieBreaker.
            errorReporter.logTrace(
                "Auto-tiebreaker: cannot evaluate tiebreaker eligibility for %s because the autoplace rules are " +
                    "currently undecidable: %s",
                getRscDfnDescriptionInline(rscDfn),
                selExc.getMessage()
            );
            ret = ignoredStorPool -> false;
        }

        return ret;
    }

    private boolean isDrbdResource(Resource rsc)
    {
        final DrbdResourceResult result = ResourceDataUtils.isDrbdResource(rsc);
        final StateFlags<Resource.Flags> rscFlags = rsc.getStateFlags();

        return result != DrbdResourceResult.NO_DRBD &&
            rscFlags.isUnset(Resource.Flags.DELETE) &&
            rscFlags.isUnset(Resource.Flags.DRBD_DELETE) &&
            rscFlags.isUnset(Resource.Flags.INACTIVE);
    }

    private PriorityProps getPrioProps(ResourceDefinition rscDfn)
    {
        return new PriorityProps(
            rscDfn.getProps(),
            rscDfn.getResourceGroup().getProps(),
            systemConfRepository.getCtrlConfForView()
        );
    }

    private @Nullable StorPool getStorPoolForTieBreaker(
        AutoHelperContext ctx,
        @Nullable Collection<Node> nodesToChooseFromRef,
        @Nullable Set<String> relaxedReplicasOnSameKeysOut
    )
    {
        @Nullable StorPool storPool = null;
        // --replicas-on-same keys that the autoplacer reported as undecidable and that we therefore drop
        // when retrying the placement (see the catch (SelectionException) below). "Any node is better than
        // no tie breaker" when the resource group's rules are already violated by the deployed resources.
        Set<String> relaxedReplicasOnSameKeys = new HashSet<>();

        List<String> filterNodeNamesList = new ArrayList<>();
        if (nodesToChooseFromRef != null)
        {
            for (Node node : nodesToChooseFromRef)
            {
                filterNodeNamesList.add(node.getName().displayValue);
            }
        }

        while (storPool == null)
        {
            AutoSelectFilterApi apiData = ctx.rscDfn.getResourceGroup().getAutoPlaceConfig().getApiData();
            @Nullable Map<String, Integer> xReplicasOnDifferentMap = apiData.getXReplicasOnDifferentMap();
            @Nullable Map<String, Integer> xReplOnDiffCopyOrNull;
            if (xReplicasOnDifferentMap != null)
            {
                xReplOnDiffCopyOrNull = new HashMap<>(xReplicasOnDifferentMap);
                // in order to force the tiebreaker to be put on a new datacenter, we want to (temporarily) override
                // all values of the xReplicasOnDifferentMap with 1
                for (Entry<String, Integer> entry : xReplOnDiffCopyOrNull.entrySet())
                {
                    entry.setValue(1);
                }
            }
            else
            {
                xReplOnDiffCopyOrNull = null;
            }
            AutoSelectFilterPojo mergedAutoSelectFilterPojo = AutoSelectFilterPojo.merge(
                new AutoSelectFilterBuilder()
                    .setPlaceCount(0)
                    .setAdditionalPlaceCount(1)
                    .setNodeNameList(filterNodeNamesList)
                    .setSkipAlreadyPlacedOnNodeNamesCheck(
                        filterNodeNamesList.isEmpty() ? null : filterNodeNamesList
                    )
                    .setDoNotPlaceWithRscList(Collections.singletonList(ctx.rscDfn.getName().displayValue))
                    .setLayerStackList(Collections.singletonList(DeviceLayerKind.DRBD))
                    .setDisklessType(Resource.Flags.DRBD_DISKLESS.name())
                    .setXReplicasOnDifferentMap(xReplOnDiffCopyOrNull)
                    .build(),
                ctx.rscDfn.getResourceGroup().getAutoPlaceConfig().getApiData(),
                ctx.selectFilter
            );
            /*
             * There are two possibilities:
             * 1) we just autoplaced at least 1 diskful resource so we reached the 2 diskful + 0 diskless condition
             * that we now place a tiebreaker. That means that if the ctx.selectFilter.getStorPoolNamesList is not
             * null it will contain the name(s) of disk*ful* storage pool(s). We need to ignore those as otherwise
             * we will not be able to find a diskless storage pool for our tiebreaker
             * 2) we just autoplaced at least 1 diskless resource. In this case, the condition of 2 diskful and
             * *0* diskless cannot be fulfilled, so we cannot be here :)
             *
             * That means, it should be always safe to also ignore the storPoolNameList of the ctx.selectFilter as
             * well as from the resourcegroup (i.e. the merged result)
             */
            // TODO: This is no longer true...
            mergedAutoSelectFilterPojo.setStorPoolNameList(null);

            @Nullable Set<StorPool> autoplaceResult = autoplaceWithRelaxedRetries(
                ctx,
                relaxedReplicasOnSameKeys,
                mergedAutoSelectFilterPojo
            );

            if (autoplaceResult != null && !autoplaceResult.isEmpty())
            {
                storPool = autoplaceResult.iterator().next();
                if (!supportsTieBreaker(storPool.getNode()))
                {
                    /*
                     * autoplacer only checks if DRBD >= 9 is supported, but we need >= 9.0.19
                     */
                    filterNodeNamesList.remove(storPool.getNode().getName().displayValue);
                    storPool = null;
                }
            }
            else
            {
                break;
            }
        }

        if (storPool != null && relaxedReplicasOnSameKeysOut != null)
        {
            relaxedReplicasOnSameKeysOut.addAll(relaxedReplicasOnSameKeys);
        }
        return storPool;
    }

    private Set<StorPool> autoplaceWithRelaxedRetries(
        AutoHelperContext ctx,
        Set<String> relaxedReplicasOnSameKeys,
        AutoSelectFilterPojo mergedAutoSelectFilterPojo
    )
    {
        // If a previous iteration hit an undecidable --replicas-on-same rule, drop the offending
        // key(s) before retrying. All other rules (e.g. --replicas-on-different) stay in effect.
        if (!relaxedReplicasOnSameKeys.isEmpty())
        {
            dropReplicasOnSameKeys(mergedAutoSelectFilterPojo, relaxedReplicasOnSameKeys);
        }

        @Nullable Set<StorPool> autoplaceResult = null;
        boolean retryAutoplace = true;
        while (retryAutoplace)
        {
            retryAutoplace = false;
            try
            {
                autoplaceResult = autoplacer.autoPlace(
                    mergedAutoSelectFilterPojo,
                    ctx.rscDfn,
                    0 // doesn't matter as we are diskless
                );
            }
            catch (SelectionException selExc)
            {
                Set<String> conflictingKeys = selExc.getConflictingKeys(
                    SelectionException.RuleType.REPLICAS_ON_SAME
                );
                // Only retry if there is a *new* key to relax. If we already relaxed everything the
                // autoplacer complains about (or the conflict is one we do not know how to relax),
                // propagate the exception instead of looping forever.
                if (conflictingKeys.isEmpty() || relaxedReplicasOnSameKeys.containsAll(conflictingKeys))
                {
                    throw selExc;
                }
                relaxedReplicasOnSameKeys.addAll(conflictingKeys);
                dropReplicasOnSameKeys(mergedAutoSelectFilterPojo, relaxedReplicasOnSameKeys);
                retryAutoplace = true;
            }
        }

        return autoplaceResult;
    }

    /**
     * Removes all entries of the given keys from the filter's {@code --replicas-on-same} list, matching both
     * bare {@code key} and {@code key=value} forms.
     */
    private static void dropReplicasOnSameKeys(AutoSelectFilterPojo filterPojo, Set<String> keysToDrop)
    {
        @Nullable List<String> replicasOnSameList = filterPojo.getReplicasOnSameList();
        if (replicasOnSameList != null)
        {
            List<String> reduced = new ArrayList<>();
            for (String entry : replicasOnSameList)
            {
                int assignIdx = entry.indexOf("=");
                String key = assignIdx == -1 ? entry : entry.substring(0, assignIdx);
                if (!keysToDrop.contains(key))
                {
                    reduced.add(entry);
                }
            }
            filterPojo.setReplicasOnSameList(reduced);
        }
    }

    private boolean supportsTieBreaker(Node node)
    {
        return node.getPeer()
            .getExtToolsManager()
            .getExtToolInfo(ExtTools.DRBD9_KERNEL)
            .isSupportedAndHasVersionOrHigher(new ExtToolsInfo.Version(9, 0, 19));
    }

    private boolean isFlagSet(Resource rsc, Resource.Flags... flags)
    {
        boolean isFlagSet;
        isFlagSet = rsc.getStateFlags().isSet(flags);
        return isFlagSet;
    }

    private boolean isSomeFlagSet(Resource rsc, Resource.Flags... flags)
    {
        boolean isFlagSet;
        isFlagSet = rsc.getStateFlags().isSomeSet(flags);
        return isFlagSet;
    }

    private boolean isSomeFlagSet(Node node, Node.Flags... flags)
    {
        boolean isFlagSet;
        isFlagSet = node.getFlags().isSomeSet(flags);
        return isFlagSet;
    }

    /**
     * Sets the tiebreaker flag in a transaction and commits the transaction.
     * Does NOT update satellites
     *
     */
    public Flux<ApiCallRc> setTiebreakerFlag(Resource tiebreaker)
    {
        ResponseContext context = CtrlRscApiCallHandler.makeRscContext(
            ApiOperation.makeModifyOperation(),
            tiebreaker.getNode().getName().getDisplayName(),
            tiebreaker.getResourceDefinition().getName().getDisplayName()
        );

        return scopeRunner
            .fluxInTransactionalScope(
                "Setting tiebreaker flag",
                LockGuard.createDeferred(nodesMapLock.writeLock(), rscDfnMapLock.writeLock()),
                () -> setTiebreakerFlagInTransaction(tiebreaker)
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> setTiebreakerFlagInTransaction(Resource tiebreakerRef)
    {
        try
        {
            tiebreakerRef.getStateFlags().enableFlags(Resource.Flags.TIE_BREAKER);

            ctrlTransactionHelper.commit();
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
        return Flux.empty();
    }
}
