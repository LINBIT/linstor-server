package com.linbit.extproc;

import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.timer.CoreTimer;
import com.linbit.timer.Action;
import com.linbit.timer.Timer;

import javax.inject.Inject;

public class ExtCmdFactory
{
    private Timer<String, Action<String>> timer;
    private ErrorReporter errlog;

    @Inject
    public ExtCmdFactory(
        CoreTimer timerRef,
        ErrorReporter errorReporterRef)
    {
        timer = timerRef;
        errlog = errorReporterRef;
    }

    public ExtCmd create()
    {
        return new ExtCmd(timer, errlog);
    }

}
