package com.linbit.drbdmanage.debug;

import com.linbit.drbdmanage.security.AccessContext;
import java.io.PrintStream;
import java.util.Map;

/**
 * Shuts down the Controller or Satellite
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class CmdShutdown extends BaseDebugCmd
{
    public CmdShutdown()
    {
        super(
            new String[]
            {
                "ShtDwn",
                "Shutdown"
            },
            "Shutdown",
            "Shuts down the module instance",
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
        cmnDebugCtl.shutdown(accCtx);
        debugCon.exitConsole();
    }
}
