package com.linbit.linstor.tasks;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotCrtApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotShippingApiCallHandler;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.PeerController;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;

import reactor.core.publisher.Flux;
import reactor.util.context.Context;

@Singleton
public class AutoSnapshotTask implements TaskScheduleService.Task
{
    private static final long TASK_TIMEOUT = 10_000;
    private static final int MIN_TO_MS = 60_000;
    private static final String AUTO_SNAPSHOT_API_NAME = "AutoSnapshot";
    private static final String AUTO_SHIPPING_API_NAME = "AutoSnapshotShipping";

    private final CtrlSnapshotCrtApiCallHandler ctrlSnapshotCrtApiCallHandler;
    private final CtrlSnapshotShippingApiCallHandler ctrlSnapshotShippingApiCallHandler;

    /*
     * Only used to populate configSet when initializing
     */
    private final ResourceDefinitionRepository rscDfnRepo;
    private final AccessContext sysCtx;

    private final SortedSet<AutoSnapshotConfig> configSet = new TreeSet<>();

    @Inject
    public AutoSnapshotTask(
        CtrlSnapshotCrtApiCallHandler ctrlSnapshotCrtApiCallHandlerRef,
        CtrlSnapshotShippingApiCallHandler ctrlSnapshotShippingApiCallHandlerRef,
        ResourceDefinitionRepository rscDfnRepoRef,
        @SystemContext AccessContext sysCtxRef
    )
    {
        ctrlSnapshotCrtApiCallHandler = ctrlSnapshotCrtApiCallHandlerRef;
        ctrlSnapshotShippingApiCallHandler = ctrlSnapshotShippingApiCallHandlerRef;
        rscDfnRepo = rscDfnRepoRef;
        sysCtx = sysCtxRef;

    }

    @Override
    public void initialize()
    {
        try
        {
            for (ResourceDefinition rscDfn : rscDfnRepo.getMapForView(sysCtx).values())
            {
                Props rscDfnProps = rscDfn.getProps(sysCtx);
                String autoShipping = rscDfnProps.getProp(
                    ApiConsts.KEY_RUN_EVERY,
                    ApiConsts.NAMESPC_SNAPSHOT_SHIPPING
                );
                if (autoShipping != null)
                {
                    AutoSnapshotConfig cfg = new AutoSnapshotConfig(
                        rscDfn.getName().displayValue,
                        Long.parseLong(autoShipping) * MIN_TO_MS,
                        true
                    );
                    configSet.add(cfg);
                    // delay the first run to give the nodes some time to connect
                    cfg.nextRerunAt += TASK_TIMEOUT;
                }

                String autoSnapshot = rscDfnProps.getProp(
                    ApiConsts.KEY_RUN_EVERY,
                    ApiConsts.NAMESPC_AUTO_SNAPSHOT
                );
                if (autoSnapshot != null)
                {
                    AutoSnapshotConfig cfg = new AutoSnapshotConfig(
                        rscDfn.getName().displayValue,
                        Long.parseLong(autoSnapshot) * MIN_TO_MS,
                        false
                    );
                    configSet.add(cfg);
                    // delay the first run to give the nodes some time to connect
                    cfg.nextRerunAt += TASK_TIMEOUT;
                }
            }
        }
        catch (InvalidKeyException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }

    }

    public Flux<ApiCallRc> addAutoSnapshotting(String rscName, long runEveryInMinRef)
    {
        return synchronizedAdd(rscName, runEveryInMinRef, false);
    }

    public Flux<ApiCallRc> addAutoSnapshotShipping(String rscName, long runEveryInMinRef)
    {
        return synchronizedAdd(rscName, runEveryInMinRef, true);
    }

    public void removeAutoSnapshotting(String rscName)
    {
        synchronizedRemove(rscName, false);
    }

    public void removeAutoSnapshotShipping(String rscName)
    {
        synchronizedRemove(rscName, true);
    }

    private Flux<ApiCallRc> synchronizedAdd(String rscNameRef, long runEveryInMinRef, boolean shippingRef)
    {
        Flux<ApiCallRc> ret = Flux.empty();
        synchronized (configSet)
        {
            boolean found = false;
            for (AutoSnapshotConfig cfg : configSet)
            {
                if (cfg.rscName.equalsIgnoreCase(rscNameRef) && cfg.shipping == shippingRef)
                {
                    cfg.setRunEvery(runEveryInMinRef * MIN_TO_MS);
                    found = true;
                    break;
                }
            }
            if (!found)
            {
                AutoSnapshotConfig cfg = new AutoSnapshotConfig(rscNameRef, runEveryInMinRef * MIN_TO_MS, shippingRef);
                ret = getFlux(cfg);
                configSet.add(cfg);
            }
        }
        return ret;
    }

    private void synchronizedRemove(String rscNameRef, boolean shippingRef)
    {
        synchronized (configSet)
        {
            AutoSnapshotConfig found = null;
            for (AutoSnapshotConfig cfg : configSet)
            {
                if (cfg.rscName.equalsIgnoreCase(rscNameRef) && cfg.shipping == shippingRef)
                {
                    found = cfg;
                    break;
                }
            }
            if (found != null)
            {
                configSet.remove(found);
            }
        }
    }

    public void shippingFinished(String rscNameRef)
    {
        AutoSnapshotConfig cfg = null;
        boolean runNow = false;
        synchronized (configSet)
        {
            for (AutoSnapshotConfig cfgTmp : configSet)
            {
                if (cfgTmp.rscName.equalsIgnoreCase(rscNameRef))
                {
                    cfg = cfgTmp;
                    if (cfg.nextRerunAt < System.currentTimeMillis())
                    {
                        runNow = true;
                        cfg.nextRerunAt = Long.MAX_VALUE; // prevent concurrent reRun from the actual run() method
                        // however, DO NOT call run(cfg) within this synchronized block, as that could
                        // cause a deadlock
                    }
                    break;
                }
            }
        }
        if (runNow)
        {
            run(cfg);
        }
    }

    @Override
    public long run()
    {
        TreeSet<AutoSnapshotConfig> cfgSet = new TreeSet<>();
        long curTime = System.currentTimeMillis();
        synchronized (configSet)
        {
            for (AutoSnapshotConfig cfg : configSet)
            {
                if (cfg.nextRerunAt <= curTime)
                {
                    cfgSet.add(cfg);
                }
                else
                {
                    break;
                }
            }
        }
        for (AutoSnapshotConfig cfg : cfgSet)
        {
            run(cfg);
        }
        return TASK_TIMEOUT;
    }

    private void run(AutoSnapshotConfig cfgRef)
    {
        getFlux(cfgRef)
            .subscriberContext(
                Context.of(
                    ApiModule.API_CALL_NAME,
                    cfgRef.shipping ? AUTO_SHIPPING_API_NAME : AUTO_SNAPSHOT_API_NAME,
                    AccessContext.class, sysCtx,
                    Peer.class, new PeerController("autoSnapshot", null, true)
                )
            )
            .subscribe();
        cfgRef.nextRerunAt += cfgRef.runEveryInMs;
    }

    private Flux<ApiCallRc> getFlux(AutoSnapshotConfig cfgRef)
    {
        Flux<ApiCallRc> flux;
        if (cfgRef.shipping)
        {
            flux = ctrlSnapshotShippingApiCallHandler.autoShipSnapshot(cfgRef.rscName);
        }
        else
        {
            flux = ctrlSnapshotCrtApiCallHandler.createAutoSnapshot(Collections.emptyList(), cfgRef.rscName);
        }
        return flux;
    }

    private class AutoSnapshotConfig implements Comparable<AutoSnapshotConfig>
    {
        private final String rscName;
        private final boolean shipping;

        private long runEveryInMs;
        private long nextRerunAt;

        private AutoSnapshotConfig(String rscNameRef, long runEveryInMsRef, boolean shippingRef)
        {
            rscName = rscNameRef;
            shipping = shippingRef;
            setRunEvery(runEveryInMsRef);
        }

        public void setRunEvery(long runEveryInMsRef)
        {
            runEveryInMs = runEveryInMsRef;
            nextRerunAt = System.currentTimeMillis() + runEveryInMsRef;
        }

        @Override
        public int compareTo(AutoSnapshotConfig oRef)
        {
            int cmp = Long.compare(nextRerunAt, oRef.nextRerunAt);
            if (cmp == 0)
            {
                cmp = rscName.compareTo(oRef.rscName);
                if (cmp == 0)
                {
                    cmp = Long.compare(runEveryInMs, oRef.runEveryInMs);
                }
            }
            return cmp;
        }
    }
}
