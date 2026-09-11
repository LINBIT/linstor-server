package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.SharedResourceManager;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import reactor.core.publisher.Flux;

/**
 * Reverts a make-available, especially one issued with auto_manage_dual_primary for a live migration:
 * removes the resource from the given node if that is possible without losing data (diskless resources
 * and redundant copies in a shared storage pool; tiebreaker and diskful resources are kept) and reverts
 * the dual-primary DRBD net options (allow-two-primaries, protocol) including the internal live-migrate
 * markers.
 *
 * Calling this for a resource or resource-definition that does not exist (anymore) is a successful no-op,
 * so automation clients can always issue this call when a volume is detached from a node.
 */
@Singleton
public class CtrlRscUnmakeAvailableApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final ResponseConverter responseConverter;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlApiDataLoader dataLoader;
    private final SharedResourceManager sharedRscMgr;
    private final CtrlRscLiveMigrateHelper liveMigrateHelper;
    private final CtrlRscDeleteApiCallHandler ctrlRscDeleteApiCallHandler;
    private final CtrlRscDeleteApiHelper ctrlRscDeleteApiHelper;
    private final CtrlRscActivateApiCallHandler ctrlRscActivateApiCallHandler;
    private final CtrlSatelliteUpdateCaller stltUpdateCaller;

    @Inject
    public CtrlRscUnmakeAvailableApiCallHandler(
        ErrorReporter errorReporterRef,
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        ResponseConverter responseConverterRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlApiDataLoader dataLoaderRef,
        SharedResourceManager sharedRscMgrRef,
        CtrlRscLiveMigrateHelper liveMigrateHelperRef,
        CtrlRscDeleteApiCallHandler ctrlRscDeleteApiCallHandlerRef,
        CtrlRscDeleteApiHelper ctrlRscDeleteApiHelperRef,
        CtrlRscActivateApiCallHandler ctrlRscActivateApiCallHandlerRef,
        CtrlSatelliteUpdateCaller stltUpdateCallerRef
    )
    {
        errorReporter = errorReporterRef;
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        responseConverter = responseConverterRef;
        lockGuardFactory = lockGuardFactoryRef;
        dataLoader = dataLoaderRef;
        sharedRscMgr = sharedRscMgrRef;
        liveMigrateHelper = liveMigrateHelperRef;
        ctrlRscDeleteApiCallHandler = ctrlRscDeleteApiCallHandlerRef;
        ctrlRscDeleteApiHelper = ctrlRscDeleteApiHelperRef;
        ctrlRscActivateApiCallHandler = ctrlRscActivateApiCallHandlerRef;
        stltUpdateCaller = stltUpdateCallerRef;
    }

    public Flux<ApiCallRc> unmakeResourceAvailable(String nodeNameRef, String rscNameRef)
    {
        ResponseContext context = makeContext(nodeNameRef, rscNameRef);

        return scopeRunner.fluxInTransactionalScope(
                "Unmake resource available",
                lockGuardFactory.buildDeferred(
                    LockType.WRITE,
                    LockObj.NODES_MAP,
                    LockObj.RSC_DFN_MAP
                ),
                () -> unmakeRscAvailableInTransaction(nodeNameRef, rscNameRef)
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> unmakeRscAvailableInTransaction(String nodeNameRef, String rscNameRef)
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        @Nullable ResourceDefinition rscDfn = dataLoader.loadRscDfnOrNull(rscNameRef);
        if (rscDfn == null)
        {
            return Flux.just(
                ApiCallRcImpl.singleApiCallRc(
                    ApiConsts.WARN_NOT_FOUND,
                    "Resource definition '" + rscNameRef + "' not found. Nothing to do."
                )
            );
        }
        dataLoader.loadNode(nodeNameRef); // unknown node is most likely a typo, fail
        @Nullable Resource rsc = dataLoader.loadRscOrNull(nodeNameRef, rscNameRef);

        Flux<ApiCallRc> deleteFlux = Flux.empty();
        if (rsc != null)
        {
            // never revert the dual-primary settings or remove the resource while it is still in use
            ctrlRscDeleteApiHelper.ensureNotInUse(rsc);

            TreeSet<Resource> sharedRscs = sharedRscMgr.getSharedResources(rsc);
            if (!sharedRscs.isEmpty())
            {
                boolean otherUsable = false;
                for (Resource sharedRsc : sharedRscs)
                {
                    if (!sharedRsc.getStateFlags().isSomeSet(
                        Resource.Flags.DELETE,
                        Resource.Flags.INACTIVE_PERMANENTLY
                    ))
                    {
                        otherUsable = true;
                        break;
                    }
                }
                if (otherUsable)
                {
                    /*
                     * The data lives in the shared LV which is still referenced by another resource.
                     * Deactivate first so the satellite releases the volume without touching the shared
                     * data, deleting the then inactive resource only removes the LINSTOR object.
                     */
                    deleteFlux = ctrlRscActivateApiCallHandler.deactivateRsc(nodeNameRef, rscNameRef)
                        .concatWith(ctrlRscDeleteApiCallHandler.deleteResource(nodeNameRef, rscNameRef));
                }
                else
                {
                    responses.addEntry(
                        ApiCallRcImpl.entryBuilder(
                            ApiConsts.MASK_WARN,
                            "Resource '" + rscNameRef + "' is the last usable resource of its shared " +
                                "storage pool on node '" + nodeNameRef + "', not deleting it"
                        )
                            .setCause("Deleting the last resource of a shared storage pool would delete the data.")
                            .setCorrection("Delete the resource explicitly if the data is no longer needed.")
                            .setSkipErrorReport(true)
                            .build()
                    );
                }
            }
            else if (rsc.getStateFlags().isSet(Resource.Flags.TIE_BREAKER))
            {
                responses.addEntry(
                    "Resource '" + rscNameRef + "' on node '" + nodeNameRef +
                        "' is a tiebreaker and is left in place",
                    ApiConsts.MASK_INFO
                );
            }
            else if (rsc.isDiskless())
            {
                // keepTiebreaker: the auto-helper may turn the diskless resource into a tiebreaker
                // instead of deleting it
                deleteFlux = ctrlRscDeleteApiCallHandler.deleteResource(nodeNameRef, rscNameRef, true);
            }
            else
            {
                responses.addEntry(
                    ApiCallRcImpl.entryBuilder(
                        ApiConsts.MASK_WARN,
                        "Resource '" + rscNameRef + "' is diskful on node '" + nodeNameRef +
                            "', not deleting it"
                    )
                        .setCause(
                            "Deleting a diskful resource could remove the last up-to-date replica " +
                                "or break quorum."
                        )
                        .setCorrection(
                            "Delete the resource explicitly or use toggle-disk/migrate-disk if the " +
                                "local replica is no longer wanted."
                        )
                        .setSkipErrorReport(true)
                        .build()
                );
            }
        }
        else
        {
            responses.addEntry(
                "Resource '" + rscNameRef + "' is not deployed on node '" + nodeNameRef +
                    "'. Nothing to delete.",
                ApiConsts.MASK_INFO
            );
        }

        boolean propsChanged = liveMigrateHelper.cleanupDualPrimaryProps(rscDfn, rsc, responses);

        ctrlTransactionHelper.commit();

        Flux<ApiCallRc> flux = Flux.just((ApiCallRc) responses);
        if (propsChanged)
        {
            flux = flux.concatWith(
                stltUpdateCaller.updateSatellites(rscDfn, Flux.empty())
                    .transform(
                        updateResponses -> CtrlResponseUtils.combineResponses(
                            errorReporter,
                            updateResponses,
                            rscDfn.getName(),
                            "Reverted live-migration DRBD net options on {0}"
                        )
                    )
            );
        }
        return flux.concatWith(deleteFlux);
    }

    private ResponseContext makeContext(String nodeName, String rscName)
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_NODE, nodeName);
        objRefs.put(ApiConsts.KEY_RSC_DFN, rscName);

        return new ResponseContext(
            ApiOperation.makeDeleteOperation(),
            "Node: " + nodeName + ", Resource: '" + rscName + "'",
            "resource '" + rscName + "' on node " + nodeName + "",
            ApiConsts.MASK_RSC,
            objRefs
        );
    }
}
