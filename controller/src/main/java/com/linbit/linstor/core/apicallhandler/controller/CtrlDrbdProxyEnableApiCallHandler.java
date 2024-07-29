package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.OperationDescription;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.locks.LockGuard;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscConnectionApiCallHandler.getResourceConnectionDescriptionInline;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlDrbdProxyEnableApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final ResponseConverter responseConverter;
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock rscDfnMapLock;
    private final CtrlDrbdProxyHelper drbdProxyHelper;

    @Inject
    public CtrlDrbdProxyEnableApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        ResponseConverter responseConverterRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        CtrlDrbdProxyHelper drbdProxyHelperRef,
        ErrorReporter errorReporterRef
    )
    {
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        responseConverter = responseConverterRef;
        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        drbdProxyHelper = drbdProxyHelperRef;
        errorReporter = errorReporterRef;
    }

    public Flux<ApiCallRc> enableProxy(
        @Nullable UUID rscConnUuid,
        String nodeName1,
        String nodeName2,
        String rscNameStr,
        Integer port
    )
    {
        ResponseContext context = makeDrbdProxyContext(
            new ApiOperation(ApiConsts.MASK_MOD,
                new OperationDescription("enabling", "enabling")),
            nodeName1,
            nodeName2,
            rscNameStr
        );

        return scopeRunner
            .fluxInTransactionalScope(
                "Enable proxy",
                LockGuard.createDeferred(
                    nodesMapLock.writeLock(),
                    rscDfnMapLock.writeLock()
                ),
                () -> enableProxyInTransaction(
                    rscConnUuid,
                    nodeName1,
                    nodeName2,
                    rscNameStr,
                    port
                )
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> enableProxyInTransaction(
        @Nullable UUID rscConnUuid,
        String nodeName1,
        String nodeName2,
        String rscNameStr,
        Integer port
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscNameStr, true);

        ResourceConnection rscConn = drbdProxyHelper.enableProxy(
            rscConnUuid,
            nodeName1,
            nodeName2,
            rscNameStr,
            port
        );

        ctrlTransactionHelper.commit();

        Flux<ApiCallRc> satelliteUpdateResponses = ctrlSatelliteUpdateCaller.updateSatellites(rscDfn, Flux.empty())
            .transform(updateResponses -> CtrlResponseUtils.combineResponses(
                errorReporter,
                updateResponses,
                rscDfn.getName(),
                "Notified {0} of proxy connection for {1}"
            ));

        responses.addEntry(ApiCallRcImpl.simpleEntry(
            ApiConsts.MODIFIED,
            "DRBD Proxy enabled on " + getResourceConnectionDescriptionInline(apiCtx, rscConn)
        ));

        return Flux
            .<ApiCallRc>just(responses)
            .concatWith(satelliteUpdateResponses)
            .onErrorResume(CtrlResponseUtils.DelayedApiRcException.class, ignored -> Flux.empty());
    }

    static ResponseContext makeDrbdProxyContext(
        ApiOperation operation,
        String nodeName1Str,
        String nodeName2Str,
        String rscNameStr
    )
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_1ST_NODE, nodeName1Str);
        objRefs.put(ApiConsts.KEY_2ND_NODE, nodeName2Str);
        objRefs.put(ApiConsts.KEY_RSC_DFN, rscNameStr);

        String objectDescription =
            "DRBD Proxy on " + getResourceConnectionDescriptionInline(nodeName1Str, nodeName2Str, rscNameStr);

        return new ResponseContext(
            operation,
            objectDescription,
            objectDescription,
            ApiConsts.MASK_RSC_CONN,
            objRefs
        );
    }
}
