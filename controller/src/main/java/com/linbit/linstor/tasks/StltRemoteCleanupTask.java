package com.linbit.linstor.tasks;

import com.linbit.linstor.core.BackgroundRunner;
import com.linbit.linstor.core.BackgroundRunner.RunConfig;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupL2LSrcApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupL2LSrcApiCallHandler.BackupShippingData;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.tasks.TaskScheduleService.Task;

import java.util.Collections;

public class StltRemoteCleanupTask implements TaskScheduleService.Task
{
    private final AccessContext accCtx;
    private final BackupShippingData data;
    private final CtrlBackupL2LSrcApiCallHandler backupL2LSrcApiCallHandler;
    private final BackgroundRunner backgroundRunner;

    public StltRemoteCleanupTask(
        AccessContext accCtxRef,
        BackupShippingData dataRef,
        CtrlBackupL2LSrcApiCallHandler backupL2LSrcApiCallHandlerRef,
        BackgroundRunner backgroundRunnerRef
    )
    {
        accCtx = accCtxRef;
        data = dataRef;
        backupL2LSrcApiCallHandler = backupL2LSrcApiCallHandlerRef;
        backgroundRunner = backgroundRunnerRef;
    }

    @Override
    public long run(long scheduledAtRef)
    {
        Snapshot srcSnapshot = data.getSrcSnapshot();
        backgroundRunner.runInBackground(
            new RunConfig<>(
                "cleanup backup shipping of " + (srcSnapshot.isDeleted() ? " deleted snapshot" : srcSnapshot),
                backupL2LSrcApiCallHandler.startQueueIfReady(data.getStltRemote(), false),
                accCtx,
                Collections.emptyList(),
                true
            )
        );

        return Task.END_TASK;
    }
}
