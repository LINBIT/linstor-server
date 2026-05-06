package com.linbit.linstor.core.apicallhandler.controller.internal;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.range.Range;
import com.linbit.linstor.range.RangeUtils;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerUtils;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.MDC;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Reacts to {@link ApiConsts#FAIL_PORT_BLOCKED_DRBD_ADJUST} signals from the satellite:
 *
 * <ul>
 *   <li>{@link #beforeRetry} adds the satellite-reported blocked ports to the node's
 *       {@link ApiConsts#KEY_TCP_PORTS_BLOCKED} property (so the pool's blocked range
 *       persistently excludes them across retries and controller restarts), then repicks the
 *       DRBD ports in {@code DrbdRscData} so the satellite's next adjust uses fresh numbers.</li>
 *   <li>{@link #afterSuccess} re-issues {@code updateSatellites(rscDfn, null)} so peers learn
 *       the new ports. Offline peers fall through the existing {@code updateResource}
 *       offline route (queued for retry on reconnect).</li>
 * </ul>
 *
 * <p>Per-resource attempt state lives in the dispatcher's Reactor pipeline ({@code totalRetries}
 * + {@code AtomicReference}). No separate per-resource state map needed.
 */
@Singleton
public class DrbdAdjustBlockedPortHandler
    implements SatelliteRetcodeHandler<DrbdAdjustBlockedPortHandler.RepickHistory>
{
    private static final int MAX_RETRIES = 5;

    /**
     * Per-Flux state for this handler: every port the satellite has reported as blocked across
     * the entire retry sequence. Used by {@link #describeExhaustion} to enumerate the cumulative
     * set for the user (and available to {@link #afterSuccess} if a future variant ever wants to
     * surface it).
     */
    public static final class RepickHistory
    {
        final List<Integer> blockedPortsTried = new ArrayList<>();
    }

    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final Provider<CtrlSatelliteUpdateCaller> updateCaller;

    @Inject
    public DrbdAdjustBlockedPortHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        Provider<CtrlSatelliteUpdateCaller> updateCallerRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        updateCaller = updateCallerRef;
    }

    @Override
    public long retcode()
    {
        return ApiConsts.FAIL_PORT_BLOCKED_DRBD_ADJUST;
    }

    @Override
    public int maxRetries()
    {
        return MAX_RETRIES;
    }

    @Override
    public RepickHistory newContext()
    {
        return new RepickHistory();
    }

    @Override
    public Mono<Void> beforeRetry(Resource rsc, ApiCallRc signalRc, RepickHistory history)
    {
        Mono<Void> ret;
        List<Integer> blocked = extractBlockedPorts(signalRc);
        if (blocked.isEmpty())
        {
            errorReporter.logWarning(
                "FAIL_PORT_BLOCKED_DRBD_ADJUST for '%s' carried no '%s' objref; nothing to repick.",
                rsc.getResourceDefinition().getName().displayValue,
                ApiConsts.KEY_BLOCKED_PORTS_LIST
            );
            ret = Mono.empty();
        }
        else
        {
            history.blockedPortsTried.addAll(blocked);

            ret = scopeRunner.fluxInTransactionalScope(
                "Repick DRBD port after blocked-port signal",
                lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.NODES_MAP, LockObj.RSC_DFN_MAP),
                () -> repickPortsInTransaction(rsc, blocked),
                MDC.getCopyOfContextMap()
            ).then();
        }
        return ret;
    }

    private Flux<ApiCallRc> repickPortsInTransaction(Resource rsc, List<Integer> blockedPorts)
    {
        try
        {
            Node node = rsc.getNode();
            errorReporter.logTrace(
                "Repicking DRBD ports for resource '%s' on node '%s'; satellite reported blocked %s.",
                rsc.getResourceDefinition().getName().displayValue,
                node.getName().displayValue,
                blockedPorts
            );

            // (1) Persist blocked ports in the node's TcpPortsBlocked property, so the pool's
            // blocked range excludes them on this retry and on any future allocation. Idempotent:
            // already-blocked ports are de-duplicated.
            appendBlockedPortsToNodeProp(node, blockedPorts);

            // (2) Refresh the pool's blocked range from the property.
            DynamicNumberPool tcpPortPool = node.getTcpPortPool(apiCtx);
            tcpPortPool.reloadBlockedRange();

            // (3) For each DrbdRscData of this Resource, deallocate any port that's in the blocked
            // list, then auto-allocate replacements (autoAllocate respects the refreshed blocked
            // range).
            int repicked = repickPortsInDrbdRscData(rsc, tcpPortPool, blockedPorts);

            ctrlTransactionHelper.commit();

            errorReporter.logWarning(
                "Repicked %d DRBD port%s for resource '%s' on node '%s'.",
                repicked,
                repicked == 1 ? "" : "s",
                rsc.getResourceDefinition().getName().displayValue,
                node.getName().displayValue
            );
            return Flux.empty();
        }
        catch (AccessDeniedException | DatabaseException | InvalidKeyException |
            InvalidValueException | ExhaustedPoolException | ValueOutOfRangeException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private void appendBlockedPortsToNodeProp(Node node, List<Integer> blockedPorts)
        throws AccessDeniedException, DatabaseException, InvalidKeyException, InvalidValueException
    {
        Props nodeProps = node.getProps(apiCtx);
        @Nullable String existing = nodeProps.getProp(ApiConsts.KEY_TCP_PORTS_BLOCKED);

        List<Range> list = Range.parseList(existing);

        for (Integer port : blockedPorts)
        {
            list = RangeUtils.merge(list, new Range(port, port));
        }
        nodeProps.setProp(ApiConsts.KEY_TCP_PORTS_BLOCKED, RangeUtils.render(list));
        node.getTcpPortPool(apiCtx).reloadBlockedRange();
    }

    private int repickPortsInDrbdRscData(Resource rsc, DynamicNumberPool tcpPortPool, List<Integer> blockedPorts)
        throws AccessDeniedException, ExhaustedPoolException, ValueOutOfRangeException
    {
        Set<Integer> blockedSet = Set.copyOf(blockedPorts);
        int totalRepicked = 0;

        List<DrbdRscData<Resource>> drbdRscList = collectDrbdRscData(rsc);
        for (DrbdRscData<Resource> drbdRsc : drbdRscList)
        {
            @Nullable Collection<TcpPortNumber> rscPorts = drbdRsc.getTcpPortList();
            if (rscPorts != null)
            {
                List<TcpPortNumber> toReplace = new ArrayList<>();
                for (TcpPortNumber port : rscPorts)
                {
                    if (blockedSet.contains(port.value))
                    {
                        toReplace.add(port);
                    }
                }

                for (TcpPortNumber port : toReplace)
                {
                    rscPorts.remove(port);
                    tcpPortPool.deallocate(port.value);
                }
                for (int ignored = 0; ignored < toReplace.size(); ignored++)
                {
                    int newPort = tcpPortPool.autoAllocate();
                    rscPorts.add(new TcpPortNumber(newPort));
                }
                totalRepicked += toReplace.size();
            }
        }
        return totalRepicked;
    }

    @Override
    public Flux<ApiCallRc> afterSuccess(Resource rsc, RepickHistory history)
    {
        ResourceDefinition rscDfn = rsc.getResourceDefinition();
        Set<NodeName> nodeNames = collectNodeNames(rscDfn);

        // NOTE for future handlers using this as a reference: from the LINSTOR client's
        // perspective a successful resource create is supposed to be a clean success, regardless
        // of how many internal retries were needed. We therefore deliberately do NOT surface the
        // retry history here. If a future handler decides its case is worth telling the user
        // about, it can prepend an INFO entry to the returned Flux, e.g.:
        //
        // return Flux.<ApiCallRc>just(<your response here>).concatWith(<existing combineResponses chain>);

        return updateCaller.get()
            .updateSatellites(rscDfn, /* nextStepRef */ null)
            .transform(
                updateResponses -> CtrlResponseUtils.combineResponses(
                    errorReporter,
                    updateResponses,
                    rscDfn.getName(),
                    nodeNames,
                    "Propagated repicked DRBD port for {1} to {0}",
                    "Propagated repicked DRBD port for {1} to {0}"
                )
            );
    }

    @Override
    public ApiCallRc.RcEntry describeExhaustion(Resource rsc, RepickHistory history, int attempts)
    {
        String rscName = rsc.getResourceDefinition().getName().displayValue;
        String nodeName = rsc.getNode().getName().displayValue;
        return ApiCallRcImpl.entryBuilder(
            ApiConsts.FAIL_INVLD_RSC_PORT,
            String.format(
                "Could not allocate a usable TCP port for DRBD resource '%s' on node '%s'. " +
                    "After %d retries the satellite is still reporting blocked ports. " +
                    "Cumulative list of port(s) the satellite reported as blocked across " +
                    "all attempts: %s.",
                rscName,
                nodeName,
                attempts,
                history.blockedPortsTried
            )
        )
            .setCause(String.format("Another process on node '%s' is bound to the port(s) LINSTOR picked.", nodeName))
            .build();
    }

    private List<DrbdRscData<Resource>> collectDrbdRscData(Resource rsc) throws AccessDeniedException
    {
        List<DrbdRscData<Resource>> out = new ArrayList<>();
        List<AbsRscLayerObject<Resource>> drbdRscDataSet = LayerUtils.getChildLayerDataByKind(
            rsc.getLayerData(apiCtx),
            DeviceLayerKind.DRBD
        );
        for (AbsRscLayerObject<Resource> layer : drbdRscDataSet)
        {
            out.add((DrbdRscData<Resource>) layer);
        }
        return out;
    }

    private Set<NodeName> collectNodeNames(ResourceDefinition rscDfn)
    {
        try
        {
            Set<NodeName> names = new LinkedHashSet<>();
            Iterator<Resource> it = rscDfn.iterateResource(apiCtx);
            while (it.hasNext())
            {
                names.add(it.next().getNode().getName());
            }
            return names;
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private static List<Integer> extractBlockedPorts(ApiCallRc signalRc)
    {
        @Nullable String csv = null;
        for (ApiCallRc.RcEntry entry : signalRc)
        {
            if (entry.getReturnCode() == ApiConsts.FAIL_PORT_BLOCKED_DRBD_ADJUST)
            {
                csv = entry.getObjRefs().get(ApiConsts.KEY_BLOCKED_PORTS_LIST);
                break;
            }
        }

        List<Integer> out = new ArrayList<>();
        if (csv != null && !csv.isBlank())
        {
            for (String part : csv.split(",", -1))
            {
                String trimmed = part.trim();
                if (!trimmed.isEmpty())
                {
                    try
                    {
                        out.add(Integer.parseInt(trimmed));
                    }
                    catch (NumberFormatException ignored)
                    {
                        // skip malformed entries
                    }
                }
            }
        }
        return out;
    }
}
