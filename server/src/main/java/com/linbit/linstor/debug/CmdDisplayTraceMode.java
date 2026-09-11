package com.linbit.linstor.debug;

import jakarta.inject.Inject;
import com.linbit.linstor.logging.ErrorReporter;
import java.io.PrintStream;
import java.util.Map;

import org.slf4j.event.Level;

public class CmdDisplayTraceMode extends BaseDebugCmd
{
    private final ErrorReporter errorReporter;

    @Inject
    public CmdDisplayTraceMode(ErrorReporter errorReporterRef)
    {
        super(
            new String[]
            {
                "DspTrcMode"
            },
            "Display TRACE level logging mode",
            "Displays the current TRACE level logging mode",
            null,
            null
        );

        errorReporter = errorReporterRef;
    }

    @Override
    public void execute(
        PrintStream debugOut,
        PrintStream debugErr,
        Map<String, String> parameters
    )
        throws Exception
    {
        String modeText = errorReporter.hasAtLeastLogLevel(Level.TRACE) ? "ENABLED" : "DISABLED";
        debugOut.println("Current TRACE mode: " + modeText);
    }
}
