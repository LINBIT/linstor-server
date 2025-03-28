package com.linbit.linstor.tasks;

import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlStorPoolListApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.apicallhandler.controller.VlmAllocatedFetcher;
import com.linbit.linstor.core.apicallhandler.controller.VlmAllocatedResult;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;

import static com.linbit.locks.LockGuardFactory.LockObj.CTRL_CONFIG;
import static com.linbit.locks.LockGuardFactory.LockObj.RSC_DFN_MAP;
import static com.linbit.locks.LockGuardFactory.LockType.READ;
import static com.linbit.locks.LockGuardFactory.LockType.WRITE;

import javax.inject.Inject;

import java.util.Collections;
import java.util.Map;

import reactor.core.publisher.Flux;
import reactor.util.context.Context;

public class UpdateSpaceInfoTask implements TaskScheduleService.Task
{

    private static final String DEFAULT_UPDATE_SLEEP = "180";
    private final ErrorReporter errRep;
    private final ResourceDefinitionRepository rscDefRepo;
    private final VlmAllocatedFetcher vlmAllocatedFetcher;
    private final SystemConfRepository systemConfRepository;
    private final CtrlStorPoolListApiCallHandler ctrlStorPoolListApiCallHandler;
    private final LockGuardFactory lockGuardFactory;
    private final AccessContext sysCtx;
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;

    @Inject
    public UpdateSpaceInfoTask(
        ErrorReporter errorReporterRef,
        LockGuardFactory lockGuardFactoryRef,
        ResourceDefinitionRepository resourceDefinitionRepositoryRef,
        VlmAllocatedFetcher vlmAllocatedFetcherRef,
        ScopeRunner scopeRunnerRef,
        SystemConfRepository systemConfRepositoryRef,
        CtrlStorPoolListApiCallHandler ctrlStorPoolListApiCallHandlerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        @SystemContext AccessContext sysCtxRef)
    {
        errRep = errorReporterRef;
        lockGuardFactory = lockGuardFactoryRef;
        rscDefRepo = resourceDefinitionRepositoryRef;
        vlmAllocatedFetcher = vlmAllocatedFetcherRef;
        systemConfRepository = systemConfRepositoryRef;
        ctrlStorPoolListApiCallHandler = ctrlStorPoolListApiCallHandlerRef;
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        sysCtx = sysCtxRef;
    }

    @Override
    public long run(long scheduleAt)
    {
        // TODO instead of doing 2 independent update calls to the satellites
        // we should merge both requests to a single proto request/response

        final long start = System.currentTimeMillis();
        vlmAllocatedFetcher.fetchVlmAllocated(
                Collections.emptySet(),
                Collections.emptySet(),
                Collections.emptySet())
            .flatMapMany(vlmAllocations ->
                scopeRunner.fluxInTransactionalScope(
                    "Update volume allocations",
                    lockGuardFactory.buildDeferred(WRITE, RSC_DFN_MAP),
                    () ->
                    {
                        for (Map.Entry<Volume.Key, VlmAllocatedResult> entry : vlmAllocations.entrySet())
                        {
                            try
                            {
                                Volume.Key vlmKey = entry.getKey();
                                ResourceDefinition rscDfn = rscDefRepo.get(sysCtx, vlmKey.getResourceName());
                                if (rscDfn != null)
                                {
                                    Resource rsc = rscDfn.getResource(sysCtx, vlmKey.getNodeName());
                                    if (rsc != null)
                                    {
                                        Volume vlm = rsc.getVolume(vlmKey.getVolumeNumber());
                                        if (vlm != null && !entry.getValue().hasErrors())
                                        {
                                            if (vlm.isAllocatedSizeSet(sysCtx)) // avoid unboxing NPE
                                            {
                                                // safe some cpu cycles for noops
                                                if (vlm.getAllocatedSize(sysCtx) != entry.getValue().getAllocatedSize())
                                                {
                                                    vlm.setAllocatedSize(sysCtx, entry.getValue().getAllocatedSize());
                                                }
                                            }
                                            else
                                            {
                                                // set always if currently null
                                                vlm.setAllocatedSize(sysCtx, entry.getValue().getAllocatedSize());
                                            }
                                        }
                                    }
                                }
                            }
                            catch (AccessDeniedException ignored)
                            {
                            }
                        }
                        ctrlTransactionHelper.commit();
                        errRep.logTrace("UpdateVolumeAllocationsTask: Fetched and set volume allocations in %dms",
                            System.currentTimeMillis() - start);
                        return Flux.empty();
                    }
                ))
            .contextWrite(Context.of(
                ApiModule.API_CALL_NAME, "UpdateVolumeAllocations",
                AccessContext.class, sysCtx))
            .subscribe();

        ctrlStorPoolListApiCallHandler.listStorPools(
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            false
        )
            .contextWrite(Context.of(
                ApiModule.API_CALL_NAME, "UpdateFreeSpaceInfo",
                AccessContext.class, sysCtx))
            .subscribe();

        long nextUpdate = Long.parseLong(DEFAULT_UPDATE_SLEEP);
        try (LockGuard ignored = lockGuardFactory.build(READ, CTRL_CONFIG))
        {
            nextUpdate = Long.parseLong(systemConfRepository.getCtrlConfForView(sysCtx)
                .getPropWithDefault(ApiConsts.KEY_UPDATE_CACHE_INTERVAL, DEFAULT_UPDATE_SLEEP));
        }
        catch (AccessDeniedException ignored)
        {
        }
        catch (NumberFormatException nfe)
        {
            errRep.reportError(nfe);
        }
        return getNextFutureReschedule(scheduleAt, (nextUpdate * 1000));
    }
}
