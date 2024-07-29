package com.linbit.linstor.tasks;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.BackgroundRunner;
import com.linbit.linstor.core.BackgroundRunner.RunConfig;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotCrtApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotShippingApiCallHandler;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;
import com.linbit.utils.Pair;

import com.linbit.linstor.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

import reactor.core.publisher.Flux;

@Singleton
public class AutoSnapshotTask implements TaskScheduleService.Task
{
    private static final long TASK_TIMEOUT = 10_000;
    private static final int MIN_TO_MS = 60_000;
    private static final String AUTO_SNAPSHOT_API_NAME = "AutoSnapshot";
    private static final String AUTO_SHIPPING_API_NAME = "AutoSnapshotShipping";

    private final CtrlSnapshotCrtApiCallHandler ctrlSnapshotCrtApiCallHandler;
    private final CtrlSnapshotShippingApiCallHandler ctrlSnapshotShippingApiCallHandler;
    private final BackgroundRunner backgroundRunner;
    private final LockGuardFactory lockGuardFactory;

    /*
     * Only used to populate configSet when initializing
     */
    private final ResourceDefinitionRepository rscDfnRepo;
    private final SystemConfRepository sysConfRepo;
    private final AccessContext sysCtx;

    private final SortedSet<AutoSnapshotConfig> configSet = new TreeSet<>();

    @Inject
    public AutoSnapshotTask(
        CtrlSnapshotCrtApiCallHandler ctrlSnapshotCrtApiCallHandlerRef,
        CtrlSnapshotShippingApiCallHandler ctrlSnapshotShippingApiCallHandlerRef,
        ResourceDefinitionRepository rscDfnRepoRef,
        SystemConfRepository sysConfRepoRef,
        @SystemContext AccessContext sysCtxRef,
        BackgroundRunner backgroundRunnerRef,
        LockGuardFactory lockGuardFactoryRef
    )
    {
        ctrlSnapshotCrtApiCallHandler = ctrlSnapshotCrtApiCallHandlerRef;
        ctrlSnapshotShippingApiCallHandler = ctrlSnapshotShippingApiCallHandlerRef;
        rscDfnRepo = rscDfnRepoRef;
        sysConfRepo = sysConfRepoRef;
        sysCtx = sysCtxRef;
        backgroundRunner = backgroundRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;

    }

    @Override
    public void initialize()
    {
        try
        {
            final long now = System.currentTimeMillis();
            for (ResourceDefinition rscDfn : rscDfnRepo.getMapForView(sysCtx).values())
            {
                PriorityProps prioProps = new PriorityProps(
                    rscDfn.getProps(sysCtx),
                    rscDfn.getResourceGroup().getProps(sysCtx),
                    sysConfRepo.getStltConfForView(sysCtx)
                );
                String autoShipping = prioProps.getProp(
                    ApiConsts.KEY_RUN_EVERY,
                    ApiConsts.NAMESPC_SNAPSHOT_SHIPPING
                );
                if (autoShipping != null)
                {
                    long runEveryInMs = Long.parseLong(autoShipping) * MIN_TO_MS;
                    configSet.add(new AutoSnapshotConfig(
                        rscDfn.getName().displayValue,
                        runEveryInMs,
                        now + runEveryInMs + TASK_TIMEOUT, // delay the first run to give the nodes some time to connect
                        true
                    ));
                }

                String autoSnapshot = prioProps.getProp(
                    ApiConsts.KEY_RUN_EVERY,
                    ApiConsts.NAMESPC_AUTO_SNAPSHOT
                );
                if (autoSnapshot != null)
                {
                    long runEveryInMs = Long.parseLong(autoSnapshot) * MIN_TO_MS;
                    AutoSnapshotConfig cfg = new AutoSnapshotConfig(
                        rscDfn.getName().displayValue,
                        runEveryInMs,
                        now + runEveryInMs + TASK_TIMEOUT, // delay the first run to give the nodes some time to connect
                        false
                    );
                    configSet.add(cfg);
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
        if (runEveryInMinRef <= 0)
        {
            synchronizedRemove(rscNameRef, shippingRef);
        }
        else
        {
            synchronized (configSet)
            {
                @Nullable Pair<AutoSnapshotConfig, AutoSnapshotConfig> cfgToReplace = null;
                for (AutoSnapshotConfig cfg : configSet)
                {
                    if (cfg.rscName.equalsIgnoreCase(rscNameRef) && cfg.shipping == shippingRef)
                    {
                        cfgToReplace = new Pair<>(
                            cfg,
                            new AutoSnapshotConfig(rscNameRef, runEveryInMinRef * MIN_TO_MS, shippingRef)
                        );
                        break;
                    }
                }
                if (cfgToReplace == null)
                {
                    AutoSnapshotConfig cfg = new AutoSnapshotConfig(
                        rscNameRef,
                        runEveryInMinRef * MIN_TO_MS,
                        shippingRef
                    );
                    ret = getFlux(cfg);
                    configSet.add(cfg);
                }
                else
                {
                    configSet.remove(cfgToReplace.objA);
                    configSet.add(cfgToReplace.objB);
                }
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
        AutoSnapshotConfig ret = null;
        boolean runNow = false;
        synchronized (configSet)
        {
            @Nullable Pair<AutoSnapshotConfig, AutoSnapshotConfig> cfgToReplace = null;
            for (AutoSnapshotConfig cfg : configSet)
            {
                if (cfg.rscName.equalsIgnoreCase(rscNameRef))
                {
                    ret = cfg;
                    if (cfg.nextRerunAt < System.currentTimeMillis())
                    {
                        cfgToReplace = new Pair<>(
                            cfg,
                            new AutoSnapshotConfig(
                                rscNameRef,
                                cfg.runEveryInMs,
                                Long.MAX_VALUE, // prevent concurrent reRun from the actual run() method
                                true
                            )
                        );
                        // however, DO NOT call run(cfg) within this synchronized block, as that could
                        // cause a deadlock
                        runNow = true;
                    }
                    break;
                }
            }
            if (cfgToReplace != null)
            {
                configSet.remove(cfgToReplace.objA);
                configSet.add(cfgToReplace.objB);
            }
        }
        if (runNow)
        {
            run(ret);
        }
    }

    @Override
    public long run(long scheduleAt)
    {
        TreeSet<AutoSnapshotConfig> cfgSet = new TreeSet<>();
        long curTime = System.currentTimeMillis();
        synchronized (configSet)
        {
            for (AutoSnapshotConfig cfg : configSet)
            {
                if (cfg.nextRerunAt <= curTime && !cfg.shippingInProgress)
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
        return getNextFutureReschedule(scheduleAt, TASK_TIMEOUT);
    }

    private void run(AutoSnapshotConfig cfgRef)
    {
        backgroundRunner.runInBackground(
            new RunConfig<>(
                (cfgRef.shipping ? AUTO_SHIPPING_API_NAME : AUTO_SNAPSHOT_API_NAME) + " of resource " + cfgRef.rscName,
                getFlux(cfgRef),
                sysCtx,
                getNodeNamessByRscName(cfgRef.rscName),
                true
            )
        );
        if (cfgRef.shipping)
        {
            cfgRef.shippingInProgress = true;
        }
        synchronized (configSet)
        {
            // since we will change the ordering of the configSet, we must remove this entry from the
            // TreeSet and re-add it.
            configSet.remove(cfgRef);
            configSet.add(cfgRef.next());
        }
    }

    private List<NodeName> getNodeNamessByRscName(String rscNameRef)
    {
        // TODO: this method is very similar to BackupShippingTask#getNodesToLock, so we should probably combine
        // those sometime...
        List<NodeName> ret = new ArrayList<>();
        try (LockGuard lg = lockGuardFactory.build(LockType.READ, LockObj.NODES_MAP, LockObj.RSC_DFN_MAP))
        {
            ResourceDefinition rscDfn = rscDfnRepo.get(sysCtx, new ResourceName(rscNameRef));
            Iterator<Resource> rscIt = rscDfn.iterateResource(sysCtx);
            while (rscIt.hasNext())
            {
                Resource rsc = rscIt.next();
                if (!rsc.isDeleted() && !rsc.getNode().isDeleted())
                {
                    ret.add(rsc.getNode().getName());
                }
            }
        }
        catch (AccessDeniedException | InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }
        return ret;
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

        /*
         * We MUST NOT change runEveryInMs and nextRerunAt variables, since they are part of the compareTo method.
         * Changing those variable would change the ordering within a sorted collection. However, without telling the
         * sorted collection that this element needs to be reordered, we might run into issues that add/remove or even
         * contains methods stop working properly.
         *
         * If we have to change these properties, simply create a new AutoSnapshotConfig instance, delete the old object
         * from the sorted collection and add the new AutoSnapshotConfig instance.
         */
        private final long runEveryInMs;
        private final long nextRerunAt;

        private boolean shippingInProgress = false;

        private AutoSnapshotConfig(String rscNameRef, long runEveryInMsRef, boolean shippingRef)
        {
            this(rscNameRef, runEveryInMsRef, System.currentTimeMillis() + runEveryInMsRef, shippingRef);
        }

        public AutoSnapshotConfig next()
        {
            return new AutoSnapshotConfig(rscName, runEveryInMs, nextRerunAt + runEveryInMs, shipping);
        }

        private AutoSnapshotConfig(String rscNameRef, long runEveryInMsRef, long nextRerunAtRef, boolean shippingRef)
        {
            rscName = rscNameRef;
            shipping = shippingRef;

            runEveryInMs = runEveryInMsRef;
            nextRerunAt = nextRerunAtRef;
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

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + Objects.hash(nextRerunAt, rscName, shipping, shippingInProgress);
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            boolean eq = obj instanceof AutoSnapshotConfig;
            if (eq)
            {
                AutoSnapshotConfig other = (AutoSnapshotConfig) obj;
                eq = nextRerunAt == other.nextRerunAt && Objects.equals(rscName, other.rscName) &&
                    shipping == other.shipping && shippingInProgress == other.shippingInProgress;
            }
            return eq;
        }
    }
}
