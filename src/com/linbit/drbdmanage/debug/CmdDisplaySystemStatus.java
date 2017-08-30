package com.linbit.drbdmanage.debug;

import com.linbit.drbdmanage.security.AccessContext;
import java.io.PrintStream;
import java.util.Map;

/**
 * Displays information about the system status, e.g. resource usage, etc.
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class CmdDisplaySystemStatus extends BaseDebugCmd
{
    public CmdDisplaySystemStatus()
    {
        super(
            new String[]
            {
                "DspSysSts"
            },
            "Display system status",
            "Displays information about system resource utilization by this application",
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
        cmnDebugCtl.getInstance().printRunTimeInfo(debugOut);
    }
}
