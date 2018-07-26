package com.linbit.linstor.debug;

import javax.inject.Inject;
import java.io.PrintStream;
import java.util.Map;
import java.util.TreeMap;

import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.Privilege;

/**
 * Displays information about the current security context
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class CmdDisplayContextInfo extends BaseDebugCmd
{
    private static final String PRM_DETAIL_NAME = "DETAIL";
    private static final String PRM_DETAIL_DFLT = "DEFAULT";
    private static final String PRM_DETAIL_FULL = "FULL";

    private static final Map<String, String> PARAMETER_DESCRIPTIONS = new TreeMap<>();

    private enum DetailLevel
    {
        DEFAULT,
        FULL,
        INVALID
    }

    static
    {
        PARAMETER_DESCRIPTIONS.put(
            PRM_DETAIL_NAME,
            "The level of detail to display; '" + PRM_DETAIL_DFLT +
            "' or '" + PRM_DETAIL_FULL + "'"
        );
    }

    @Inject
    public CmdDisplayContextInfo()
    {
        super(
            new String[]
            {
                "DspCtxInf"
            },
            "Display context information",
            "Displays information about the current security context",
            PARAMETER_DESCRIPTIONS,
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
        DetailLevel detail = DetailLevel.DEFAULT;

        String prmDetail = parameters.get(PRM_DETAIL_NAME);
        if (prmDetail != null)
        {
            prmDetail = prmDetail.toUpperCase();
            if (prmDetail.equals(PRM_DETAIL_FULL))
            {
                detail = DetailLevel.FULL;
            }
            else
            if (prmDetail.equals(PRM_DETAIL_DFLT))
            {
                detail = DetailLevel.DEFAULT;
            }
            else
            {
                printError(
                    debugErr,
                    String.format(
                        "The value '%s' is not valid for the parameter '%s'.",
                        prmDetail, PRM_DETAIL_NAME
                    ),
                    null,
                    "Enter a valid value for the parameter.",
                    String.format(
                        "Valid values are '%s' and '%s'.",
                        PRM_DETAIL_DFLT, PRM_DETAIL_FULL
                    )
                );
                detail = DetailLevel.INVALID;
            }
        }

        if (detail == DetailLevel.DEFAULT || detail == DetailLevel.FULL)
        {
            debugOut.printf(
                "Identity:        %s\n" +
                "Role:            %s\n" +
                "Security domain: %s\n",
                accCtx.subjectId.name.displayValue,
                accCtx.subjectRole.name.displayValue,
                accCtx.subjectDomain.name.displayValue
            );
        }
        if (detail == DetailLevel.FULL)
        {
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
}
