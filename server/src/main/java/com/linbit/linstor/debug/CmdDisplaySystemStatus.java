package com.linbit.linstor.debug;

import jakarta.inject.Inject;
import com.linbit.linstor.core.LinStor;

import java.io.PrintStream;
import java.util.Map;

/**
 * Displays information about the system status, e.g. resource usage, etc.
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class CmdDisplaySystemStatus extends BaseDebugCmd
{
    @Inject
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
            null
        );
    }

    @Override
    public void execute(
        PrintStream debugOut,
        PrintStream debugErr,
        Map<String, String> parameters
    )
        throws Exception
    {
        LinStor.printRunTimeInfo(debugOut);
    }
}
