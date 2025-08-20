package com.linbit.linstor.tasks;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.BackgroundRunner.RunConfig;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.remotes.LinstorRemote;
import com.linbit.linstor.core.objects.remotes.S3Remote;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.PeerTask;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.tasks.ScheduleBackupService.BackupShippingtaskConfig;
import com.linbit.linstor.tasks.ScheduleBackupService.ScheduledShippingConfig;
import com.linbit.linstor.tasks.TaskScheduleService.Task;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import reactor.core.publisher.Flux;

public class BackupShippingTask implements TaskScheduleService.Task
{
    private final Object syncObj = new Object();
    private final BackupShippingtaskConfig cfg;

    private boolean incremental;
    private boolean forceRestore;

    public BackupShippingTask(BackupShippingtaskConfig baseTaskObjectsRef)
    {
        cfg = baseTaskObjectsRef;

        incremental = cfg.isIncremental();
        forceRestore = cfg.isForceRestore();
    }

    @Override
    public long run(long scheduledAt)
    {
        boolean inc;
        synchronized (syncObj)
        {
            inc = incremental;
        }
        final Flux<ApiCallRc> flux;

        ScheduleBackupService scheduleBackupService = cfg.getScheduleBackupService();
        ScheduledShippingConfig conf = cfg.getSchedShipCfg();
        AccessContext accCtx = cfg.getAccCtx();

        scheduleBackupService.removeSingleTask(conf, true);
        Peer peer = new PeerTask("BackupShippingTaskPeer", accCtx);

        String rscName = cfg.getRscName();
        String nodeName = cfg.getNodeName();
        ErrorReporter errorReporter = cfg.getErrorReporter();

        if (conf.remote instanceof S3Remote)
        {
            flux = cfg.getBackupCreateApiCallHandler()
                .createBackup(
                    rscName,
                    "",
                    conf.remote.getName().displayValue,
                    nodeName,
                    conf.schedule.getName().displayValue,
                    inc,
                    true
                );
        }
        else if (conf.remote instanceof LinstorRemote)
        {
            flux = cfg.getBackupL2LSrcApiCallHandler()
                .shipBackup(
                    nodeName,
                    rscName,
                    conf.remote.getName().displayValue,
                    conf.dstRscName == null ? rscName : conf.dstRscName,
                    null,
                    null,
                    null,
                    conf.storpoolRenameMap,
                    conf.dstRscGrp,
                    !forceRestore,
                    forceRestore,
                    conf.schedule.getName().displayValue,
                    inc,
                    true,
                    conf.forceRscGrp
                );
        }
        else
        {
            flux = Flux.empty();
            errorReporter.reportError(
                new ImplementationError(
                    "The following remote type can not be used for scheduled shippings: " +
                        conf.remote.getClass().getSimpleName()
                )
            );
        }

        cfg.getBackgroundRunner()
            .runInBackground(
                new RunConfig<>(
                    "scheduled backup shipping of resource: " + rscName + "(" + (inc ? "incremental" : "full") + ")",
                    flux,
                    accCtx,
                    getNodesToLock(rscName),
                    true
                )
                    .putSubscriberContext(Peer.class, peer)
                    .setSubscriptionConsumers(
                        apiCallRc ->
                        {
                            for (ApiCallRc.RcEntry rc : apiCallRc)
                            {
                                if ((ApiConsts.MASK_ERROR & rc.getReturnCode()) == ApiConsts.MASK_ERROR)
                                {
                                    try
                                    {
                                        scheduleBackupService.addTaskAgain(
                                            conf.rscDfn,
                                            conf.schedule,
                                            conf.remote,
                                            scheduledAt,
                                            false,
                                            false,
                                            conf.lastInc,
                                            accCtx
                                        );
                                    }
                                    catch (AccessDeniedException exc)
                                    {
                                        errorReporter.reportError(exc);
                                    }
                                }
                            }
                        },
                        error ->
                        {
                            errorReporter.reportError(error);
                            try
                            {
                                scheduleBackupService.addTaskAgain(
                                    conf.rscDfn,
                                    conf.schedule,
                                    conf.remote,
                                    scheduledAt,
                                    false,
                                    false,
                                    conf.lastInc,
                                    accCtx
                                );
                            }
                            catch (AccessDeniedException exc)
                            {
                                errorReporter.reportError(exc);
                            }
                        }
                    )
            );
        return Task.END_TASK;
    }

    private List<NodeName> getNodesToLock(String rscNameRef)
    {
        // TODO: this method is very similar to AutoSnapshotTask#getNodeNamessByRscName, so we should probably combine
        // those sometime...
        List<NodeName> ret = new ArrayList<>();
        try (LockGuard lg = cfg.getLockGuardFactory().build(LockType.READ, LockObj.NODES_MAP, LockObj.RSC_DFN_MAP))
        {
            AccessContext accCtx = cfg.getAccCtx();
            ResourceDefinition rscDfn = cfg.getRscDfnRepo().get(accCtx, new ResourceName(rscNameRef));
            Iterator<Resource> rscIt = rscDfn.iterateResource(accCtx);
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

    public long getPreviousTaskStartTime()
    {
        return cfg.getLastStartTime();
    }

    public void setIncremental(boolean incRef)
    {
        synchronized (syncObj)
        {
            incremental = incRef;
        }
    }
}
