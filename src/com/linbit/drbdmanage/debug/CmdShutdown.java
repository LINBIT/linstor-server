package com.linbit.drbdmanage.debug;

import com.linbit.drbdmanage.security.AccessContext;
import java.io.PrintStream;
import java.util.Map;

/**
 * Displays information about the Controller's threads
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class CmdShutdown extends BaseControllerDebugCmd
{
    private static final int OVERFLOW_SPACE = 20;

    public CmdShutdown()
    {
        super(
            new String[]
            {
                "ShtDwn",
                "Shutdown"
            },
            "Shutdown",
            "Shuts down the local drbdmanage controller instance",
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
    ) throws Exception
    {
        debugCtl.shutdown(accCtx);
        debugCon.exitConsole();
    }
}
