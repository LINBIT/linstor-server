package com.linbit.linstor.tasks;

import com.linbit.linstor.logging.ErrorReporter;

import javax.inject.Inject;

public class LogArchiveTask implements TaskScheduleService.Task
{
    private static final long LOGARCHIVE_SLEEP = 24 * 60 * 60;

    private final ErrorReporter errorReporter;

    @Inject
    public LogArchiveTask(ErrorReporter errorReporterRef)
    {
        errorReporter = errorReporterRef;
    }

    @Override
    public long run()
    {
        errorReporter.archiveLogDirectory();
        return LOGARCHIVE_SLEEP;
    }
}
