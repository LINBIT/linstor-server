package com.linbit.linstor.debug;

import javax.inject.Inject;
import java.io.PrintStream;
import java.util.Map;

import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.SecurityLevel;

/**
 * Displays the currently active security level
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class CmdDisplaySecLevel extends BaseDebugCmd
{
    @Inject
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
            null
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
