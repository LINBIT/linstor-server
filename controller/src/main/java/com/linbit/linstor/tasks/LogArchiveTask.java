package com.linbit.linstor.tasks;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;

import static com.linbit.locks.LockGuardFactory.LockObj.NODES_MAP;
import static com.linbit.locks.LockGuardFactory.LockType.READ;

import jakarta.inject.Inject;

public class LogArchiveTask implements TaskScheduleService.Task
{
    private static final long LOGARCHIVE_SLEEP = 24 * 60 * 60 * 1_000;

    private final ErrorReporter errorReporter;
    private final NodeRepository nodeRepository;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlStltSerializer ctrlStltSerializer;
    private final SystemConfRepository systemConfRepository;

    @Inject
    public LogArchiveTask(
        ErrorReporter errorReporterRef,
        NodeRepository nodeRepositoryRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlStltSerializer ctrlClientSerializerRef,
        SystemConfRepository systemConfRepositoryRef
    )
    {
        errorReporter = errorReporterRef;
        nodeRepository = nodeRepositoryRef;
        lockGuardFactory = lockGuardFactoryRef;
        ctrlStltSerializer = ctrlClientSerializerRef;
        systemConfRepository = systemConfRepositoryRef;
    }

    @Override
    public long run(long scheduledAt)
    {
        long ageDays = getArchiveAgeDays();

        errorReporter.archiveLogDirectory(ageDays);

        if (ageDays > 0)
        {
            try (LockGuard lg = lockGuardFactory.build(READ, NODES_MAP))
            {
                for (Node node : nodeRepository.getMapForView().values())
                {
                    Peer nodePeer = node.getPeer();
                    if (nodePeer.isOnline())
                    {
                        nodePeer.sendMessage(
                            ctrlStltSerializer.onewayBuilder(InternalApiConsts.API_ARCHIVE_LOGS)
                                .archiveLogs(ageDays)
                                .build()
                        );
                    }
                }
            }
        }

        // TODO also clean up blacklisted ports for backup shipping

        return getNextFutureReschedule(scheduledAt, LOGARCHIVE_SLEEP);
    }

    private long getArchiveAgeDays()
    {
        long ageDays = ErrorReporter.DFLT_LOG_ARCHIVE_AGE_DAYS;
        try
        {
            @Nullable String ageDaysProp = systemConfRepository.getCtrlConfForView().getProp(
                ApiConsts.KEY_LOG_ARCHIVE_AGE_DAYS,
                ApiConsts.NAMESPC_LOGGING
            );
            if (ageDaysProp != null)
            {
                ageDays = Long.parseLong(ageDaysProp);
            }
        }
        catch (NumberFormatException exc)
        {
            errorReporter.logWarning(
                "LogArchive: unable to read property %s/%s, using default of %d days: %s",
                ApiConsts.NAMESPC_LOGGING,
                ApiConsts.KEY_LOG_ARCHIVE_AGE_DAYS,
                ageDays,
                exc.getMessage()
            );
        }
        return ageDays;
    }
}
