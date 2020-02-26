package com.linbit.linstor.debug;

import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.Privilege;

import javax.inject.Inject;

import java.io.PrintStream;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.event.Level;

public class CmdSetTraceMode extends BaseDebugCmd
{
    private static final String PRM_MODE_NAME = "MODE";
    private static final String PRM_ENABLED = "ENABLED";
    private static final String PRM_DISABLED = "DISABLED";

    private static final Map<String, String> PARAMETER_DESCRIPTIONS = new TreeMap<>();
    static
    {
        PARAMETER_DESCRIPTIONS.put(
            PRM_MODE_NAME,
            "Specifies the TRACE level logging mode to set\n" +
            "    ENABLED\n" +
            "        Enables logging at the TRACE level\n" +
            "    DISABLED\n" +
            "        Disables logging at the TRACE level"
        );
    }

    private final ErrorReporter errorReporter;

    @Inject
    public CmdSetTraceMode(ErrorReporter errorReporterRef)
    {
        super(
            new String[]
            {
                "SetTrcMode"
            },
            "Set TRACE level logging mode",
            "Sets the TRACE level logging mode",
            PARAMETER_DESCRIPTIONS,
            null
        );

        errorReporter = errorReporterRef;
    }

    @Override
    public void execute(
        PrintStream debugOut,
        PrintStream debugErr,
        AccessContext accCtx,
        Map<String, String> parameters
    ) throws Exception
    {
        String prmMode = parameters.get(PRM_MODE_NAME);
        if (prmMode != null)
        {
            AccessContext privCtx = accCtx.clone();
            privCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);
            if (prmMode.equalsIgnoreCase(PRM_ENABLED))
            {
                errorReporter.setLogLevel(privCtx, null, Level.TRACE);
                debugOut.println("New TRACE level logging mode: ENABLED");
            }
            else
            if (prmMode.equalsIgnoreCase(PRM_DISABLED))
            {
                errorReporter.setLogLevel(privCtx, null, Level.DEBUG);
                debugOut.println("New TRACE level logging mode: DISABLED");
            }
            else
            {
                printError(
                    debugErr,
                    "The specified TRACE level logging mode can not be set",
                    "The value specified for the " + PRM_MODE_NAME + " parameter is invalid",
                    "Specify a valid value for the " + PRM_MODE_NAME + " parameter.\n" +
                    "Valid values are:\n" +
                    "    " + PRM_ENABLED + "\n" +
                    "    " + PRM_DISABLED,
                    "The specified value was '" + prmMode + "'"
                );
            }
        }
        else
        {
            printMissingParamError(debugErr, PRM_MODE_NAME);
        }
    }
}
