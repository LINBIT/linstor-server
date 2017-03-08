package com.linbit.drbdmanage.debug;

import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.Privilege;
import java.io.PrintStream;
import java.util.Map;

/**
 * Displays information about the Controller's threads
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class CmdDisplayContextInfo extends BaseControllerDebugCmd
{
    public CmdDisplayContextInfo()
    {
        super(
            new String[]
            {
                "DspCtxInf"
            },
            "Display context information",
            "Displays information about the current security context",
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
        debugOut.printf("%-24s: %s\n", "Identity", accCtx.getIdentity().name.displayValue);
        debugOut.printf("%-24s: %s\n", "Role", accCtx.getRole().name.displayValue);
        debugOut.printf("%-24s: %s\n", "Domain", accCtx.getDomain().name.displayValue);

        String privSeparator = String.format("\n%-24s  ", "");
        String limitPrivs;
        {
            StringBuilder limitPrivsList = new StringBuilder();
            for (Privilege priv : accCtx.getLimitPrivs().getEnabledPrivileges())
            {
                if (limitPrivsList.length() > 0)
                {
                    limitPrivsList.append(privSeparator);
                }
                limitPrivsList.append(priv.name);
            }
            if (limitPrivsList.length() <= 0)
            {
                limitPrivsList.append("None");
            }
            limitPrivs = limitPrivsList.toString();
        }
        debugOut.printf("%-24s: %s\n", "Limit privileges", limitPrivs);

        String effPrivs;
        {
            StringBuilder effPrivsList = new StringBuilder();
            for (Privilege priv : accCtx.getEffectivePrivs().getEnabledPrivileges())
            {
                if (effPrivsList.length() > 0)
                {
                    effPrivsList.append(privSeparator);
                }
                effPrivsList.append(priv.name);
            }
            if (effPrivsList.length() <= 0)
            {
                effPrivsList.append("None");
            }
            effPrivs = effPrivsList.toString();
        }
        debugOut.printf("%-24s: %s\n", "Effective privileges", effPrivs);
    }
}
