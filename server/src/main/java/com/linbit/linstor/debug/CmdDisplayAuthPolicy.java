package com.linbit.linstor.debug;

import java.io.PrintStream;
import java.util.Map;

import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.Authentication;
import javax.inject.Inject;

/**
 * Displays the currently active security level
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class CmdDisplayAuthPolicy extends BaseDebugCmd
{
    @Inject
    public CmdDisplayAuthPolicy()
    {
        super(
            new String[]
            {
                "DspAutPlc"
            },
            "Display authentication policy",
            "Displays the current policy for client authentication:\n" +
            "    ENABLED  - Interacting with the system requires a client to sign in first\n" +
            "    DISABLED - Requests received from anonymous/unauthenticated clients are\n" +
            "               accepted and executed under the PUBLIC access context",
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
        debugOut.println("Mandatory authentication is " + (Authentication.isRequired() ? "ENABLED" : "DISABLED"));
    }
}

