package com.linbit.drbdmanage.debug;

import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.SecurityLevel;

import java.io.PrintStream;
import java.util.Map;

/**
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class CmdDisplaySecLevel extends BaseControllerDebugCmd
{
    public CmdDisplaySecLevel()
    {
        super(
            new String[]
            {
                "DspSecLvl"
            },
            "Display security level",
            "Displays the currently active global security level",
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
        debugOut.println("Current global security level: " + SecurityLevel.get().name());
    }
}
