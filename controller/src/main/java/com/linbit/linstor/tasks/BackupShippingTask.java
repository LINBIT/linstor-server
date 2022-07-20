package com.linbit.linstor.tasks;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupCreateApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupL2LSrcApiCallHandler;
import com.linbit.linstor.core.objects.LinstorRemote;
import com.linbit.linstor.core.objects.S3Remote;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.PeerTask;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.tasks.ScheduleBackupService.ScheduledShippingConfig;
import com.linbit.linstor.tasks.TaskScheduleService.Task;

import reactor.core.publisher.Flux;
import reactor.util.context.Context;

public class BackupShippingTask implements TaskScheduleService.Task
{
    private final AccessContext accCtx;
    private final ErrorReporter errorReporter;
    private final CtrlBackupCreateApiCallHandler backupCrtApiCallHandler;
    private final CtrlBackupL2LSrcApiCallHandler backupL2LSrcApiCallHandler;
    private final ScheduledShippingConfig conf;
    private final ScheduleBackupService scheduleBackupService;
    private final String rscName;
    private final String nodeName;
    private final long previousTaskStartTime;
    private final Object syncObj = new Object();
    private boolean incremental;

    public BackupShippingTask(
        AccessContext accCtxRef,
        ErrorReporter errorReporterRef,
        CtrlBackupCreateApiCallHandler backupCrtApiCallHandlerRef,
        CtrlBackupL2LSrcApiCallHandler backupL2LSrcApiCallHandlerRef,
        ScheduledShippingConfig confRef,
        ScheduleBackupService scheduleBackupServiceRef,
        String rscNameRef,
        String nodeNameRef,
        boolean incrementalRef,
        long previousTaskStartTimeRef
    )
    {
        accCtx = accCtxRef;
        errorReporter = errorReporterRef;
        backupCrtApiCallHandler = backupCrtApiCallHandlerRef;
        backupL2LSrcApiCallHandler = backupL2LSrcApiCallHandlerRef;
        conf = confRef;
        scheduleBackupService = scheduleBackupServiceRef;
        rscName = rscNameRef;
        nodeName = nodeNameRef;
        incremental = incrementalRef;
        previousTaskStartTime = previousTaskStartTimeRef;
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
        scheduleBackupService.removeSingleTask(conf, true);
        Peer peer = new PeerTask("BackupShippingTaskPeer", accCtx);
        if (conf.remote instanceof S3Remote)
        {
            Flux<ApiCallRc> dumbDummyFluxBecauseFinal;
            try
            {
                dumbDummyFluxBecauseFinal = backupCrtApiCallHandler
                    .createBackup(
                        rscName, "", conf.remote.getName().displayValue, nodeName,
                        conf.schedule.getName().displayValue, inc
                    );
            }
            catch (AccessDeniedException exc)
            {
                dumbDummyFluxBecauseFinal = Flux.empty();
                errorReporter.reportError(exc);
            }
            flux = dumbDummyFluxBecauseFinal;
        }
        else if (conf.remote instanceof LinstorRemote)
        {
            flux = backupL2LSrcApiCallHandler
                .shipBackup(
                    nodeName, rscName, conf.remote.getName().displayValue, rscName, null, null, null, null, true,
                    conf.schedule.getName().displayValue, inc
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

        Thread t = new Thread(() ->
        {
            flux.subscriberContext(
                Context.of(
                    AccessContext.class, accCtx, Peer.class, peer, ApiModule.API_CALL_NAME,
                    "scheduled backup shipping"
                )
            )
                .subscribe(
                    apiCallRc ->
                    {
                        for (ApiCallRc.RcEntry rc : apiCallRc.getEntries())
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
                );
        });
        t.start();
        return Task.END_TASK;
    }

    public long getPreviousTaskStartTime()
    {
        return previousTaskStartTime;
    }

    public void setIncremental(boolean incRef)
    {
        synchronized (syncObj)
        {
            incremental = incRef;
        }
    }
}
