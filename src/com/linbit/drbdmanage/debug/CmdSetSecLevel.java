package com.linbit.drbdmanage.debug;

import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.SecurityLevel;
import java.io.PrintStream;
import java.util.Map;
import java.util.TreeMap;


public class CmdSetSecLevel extends BaseDebugCmd
{
    public static final Map<String, String> PARAMETER_DESCRIPTIONS = new TreeMap<>();

    public static final String PRM_SECLEVEL_NAME = "SECLEVEL";

    public static final String PRM_SECLEVEL_NO_SECURITY = "NO_SECURITY";
    public static final String PRM_SECLEVEL_RBAC        = "RBAC";
    public static final String PRM_SECLEVEL_MAC         = "MAC";

    public static final String LEVEL_SET_FORMAT = "Global security level set to \u001b[1;37m%s\u001b[0m\n";

    static
    {
        PARAMETER_DESCRIPTIONS.put(
            PRM_SECLEVEL_NAME,
            "The security level to set as the global security level\n" +
            "    NO_SECURITY\n" +
            "        No security, public access to all objects.\n" +
            "    RBAC\n" +
            "        Role based access controls.\n" +
            "        Objects are protected by access control lists that a certain type of access\n" +
            "        to specific roles. Sign-in is required to assume any other role than the\n" +
            "        PUBLIC role.\n" +
            "    MAC\n" +
            "        Mandatory access controls.\n" +
            "        In addition to RBAC security, objects and roles are compartmentalized, and\n" +
            "        access across compartment boundaries is regulated by mandatory access control\n" +
            "        rules established by the system administrator.\n"
            );
    }

    public CmdSetSecLevel()
    {
        super(
            new String[]
            {
                "SetSecLvl"
            },
            "Set security level",
            "Sets the global security level",
            PARAMETER_DESCRIPTIONS,
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
        String secLevelText = parameters.get(PRM_SECLEVEL_NAME);
        if (secLevelText != null)
        {
            try
            {
                secLevelText = secLevelText.toUpperCase();
                switch (secLevelText)
                {
                    case PRM_SECLEVEL_NO_SECURITY:
                        SecurityLevel.set(accCtx, SecurityLevel.NO_SECURITY);
                        debugOut.printf(LEVEL_SET_FORMAT, PRM_SECLEVEL_NO_SECURITY);
                        break;
                    case PRM_SECLEVEL_RBAC:
                        SecurityLevel.set(accCtx, SecurityLevel.RBAC);
                        debugOut.printf(LEVEL_SET_FORMAT, PRM_SECLEVEL_NO_SECURITY);
                        break;
                    case PRM_SECLEVEL_MAC:
                        SecurityLevel.set(accCtx, SecurityLevel.MAC);
                        debugOut.printf(LEVEL_SET_FORMAT, PRM_SECLEVEL_NO_SECURITY);
                        break;
                    default:
                        printError(
                            debugErr,
                            "The specified security level is not valid.",
                            String.format(
                                "The value '%s' specified for the parameter %s is not a valid security level name",
                                secLevelText, PRM_SECLEVEL_NAME
                            ),
                            String.format(
                                "Specify a valid security level.\n" +
                                "Valid security levels are:\n" +
                                "    %s\n" +
                                "    %s\n" +
                                "    %s\n",
                                PRM_SECLEVEL_NO_SECURITY, PRM_SECLEVEL_RBAC, PRM_SECLEVEL_MAC
                            ),
                            null
                        );
                        break;
                }
            }
            catch (AccessDeniedException accExc)
            {
                printDmException(debugErr, accExc);
            }
        }
        else
        {
            this.printMissingParamError(debugErr, PRM_SECLEVEL_NAME);
        }
    }
}
