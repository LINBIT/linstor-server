package com.linbit.drbdmanage.debug;

import com.linbit.drbdmanage.security.AccessContext;
import java.io.PrintStream;
import java.util.Map;

/**
 * Displays information about the Controller's threads
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class CmdDisplayModuleInfo extends BaseDebugCmd
{
    private enum DetailLevel
    {
        DEFAULT,
        FULL,
        INVALID
    }

    public CmdDisplayModuleInfo()
    {
        super(
            new String[]
            {
                "DspModInf"
            },
            "Display module information",
            "Displays information about the program module that is being debugged",
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
            "PROGRAM:   %s\n" +
            "MODULE:    %s\n" +
            "VERSION:   %s\n",
            cmnDebugCtl.getProgramName(), cmnDebugCtl.getModuleType(), cmnDebugCtl.getVersion()
        );
    }
}
