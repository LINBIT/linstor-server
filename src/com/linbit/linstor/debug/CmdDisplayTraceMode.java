package com.linbit.linstor.debug;

import com.linbit.linstor.security.AccessContext;
import java.io.PrintStream;
import java.util.Map;

public class CmdDisplayTraceMode extends BaseDebugCmd
{
    public CmdDisplayTraceMode()
    {
        super(
            new String[]
            {
                "DspTrcMode"
            },
            "Display TRACE level logging mode",
            "Displays the current TRACE level logging mode",
            null,
            null,
            false
        );
    }

    @Override
    public void execute(
        PrintStream debugOut,
        PrintStream debugErr,
        AccessContext accCtx,
        Map<String, String> parameters
    )
        throws Exception
    {
        String modeText = coreSvcs.getErrorReporter().isTraceEnabled() ? "ENABLED" : "DISABLED";
        debugOut.println("Current TRACE mode: " + modeText);
    }
}
