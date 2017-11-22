package com.linbit.linstor.debug;

import java.io.PrintStream;
import java.util.Map;

import com.linbit.linstor.security.AccessContext;

/**
 * Displays the version of the Controller or Satellite
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class CmdDisplayVersion extends BaseDebugCmd
{
    public CmdDisplayVersion()
    {
        super(
            new String[]
            {
                "DspVsn",
                "Version"
            },
            "Display version",
            "Displays the version of the program module that is being debugged",
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
        debugOut.printf(
            "VERSION:   %s\n",
            cmnDebugCtl.getVersion()
        );
    }
}
