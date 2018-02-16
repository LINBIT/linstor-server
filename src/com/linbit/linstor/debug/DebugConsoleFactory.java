package com.linbit.linstor.debug;

import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;

import javax.inject.Inject;
import java.util.Map;
import java.util.Set;

public class DebugConsoleFactory
{
    private final ErrorReporter errorReporter;
    private final Set<CommonDebugCmd> debugCommands;

    @Inject
    public DebugConsoleFactory(
        ErrorReporter errorReporterRef,
        Set<CommonDebugCmd> debugCommandsRef
    )
    {
        errorReporter = errorReporterRef;
        debugCommands = debugCommandsRef;
    }

    public DebugConsole create(AccessContext debugCtxRef)
    {
        return new DebugConsoleImpl(debugCtxRef, errorReporter, debugCommands);
    }
}
