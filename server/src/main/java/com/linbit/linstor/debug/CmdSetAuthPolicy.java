package com.linbit.linstor.debug;

import com.linbit.linstor.LinStorException;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.MandatoryAuthSetter;
import java.io.PrintStream;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import javax.inject.Inject;
import javax.inject.Named;
import org.slf4j.event.Level;

public class CmdSetAuthPolicy extends BaseDebugCmd
{
    private static final String PRM_REQAUTH_NAME    = "REQUIREAUTH";
    private static final String PRM_ENABLED         = "ENABLED";
    private static final String PRM_DISABLED        = "DISABLED";

    private static final Map<String, String> PARAMETER_DESCRIPTIONS = new TreeMap<>();
    static
    {
        PARAMETER_DESCRIPTIONS.put(
            PRM_REQAUTH_NAME,
            "Whether or not to require authentication for API calls\n" +
            "    ENABLED\n" +
            "        Authentication is required.\n" +
            "        Clients must sign in before interacting with the system.\n" +
            "    DISABLED\n" +
            "        Authentication is optional.\n" +
            "        Requests received from anonymous/unauthenticated clients are\n" +
            "        accepted and executed under the PUBLIC access context."
        );
    }

    private final ErrorReporter errorReporter;
    private final ReadWriteLock reconfigurationLock;
    private final MandatoryAuthSetter authSetter;

    @Inject
    public CmdSetAuthPolicy(
        ErrorReporter errorReporterRef,
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        MandatoryAuthSetter authSetterRef
    )
    {
        super(
            new String[]
            {
                "SetAutPlc"
            },
            "Set the authentication policy",
            "Sets the policy for client authentication:\n" +
            "    ENABLED  - Interacting with the system requires a client to sign in first\n" +
            "    DISABLED - Requests received from anonymous/unauthenticated clients are\n" +
            "               accepted and executed under the PUBLIC access context",
            PARAMETER_DESCRIPTIONS,
            null
        );

        errorReporter = errorReporterRef;
        reconfigurationLock = reconfigurationLockRef;
        authSetter = authSetterRef;
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
        Lock rcfgWrLock = reconfigurationLock.readLock();

        String reqAuthText = parameters.get(PRM_REQAUTH_NAME);
        if (reqAuthText != null)
        {
            try
            {
                rcfgWrLock.lock();
                if (PRM_DISABLED.equalsIgnoreCase(reqAuthText))
                {
                    authSetter.setAuthRequired(accCtx, false);
                    debugOut.println("Mandatory authentication DISABLED");
                }
                else
                if (PRM_ENABLED.equalsIgnoreCase(reqAuthText))
                {
                    authSetter.setAuthRequired(accCtx, true);
                    debugOut.println("Mandatory authentication ENABLED");
                }
                else
                {
                    // Value does not match PRM_ENABLED and does not match PRM_DISABLED,
                    // invalid value on the command line
                    printError(
                        debugErr,
                        "The value specified for the parameter " + PRM_REQAUTH_NAME + " is not valid",
                        "An incorrect value was specified",
                        "Valid values are:\n" +
                        "    " + PRM_ENABLED + "\n" +
                        "    " + PRM_DISABLED + "\n",
                        "The specified value was '" + reqAuthText + "'"
                    );
                }
            }
            catch (AccessDeniedException accExc)
            {
                printLsException(debugErr, accExc);
            }
            catch (DatabaseException dbExc)
            {
                String detailsText = "Review the database error to determine the cause of the problem.";
                String dbErrorMsg = dbExc.getMessage();
                if (dbErrorMsg != null)
                {
                    detailsText += "\nThe error description provided by the database subsystem is:\n" +
                    dbExc.getMessage();
                }
                String reportId = errorReporter.reportError(Level.ERROR, dbExc);
                if (reportId != null)
                {
                    detailsText += "\nAn error report was filed under report ID " + reportId + ".";
                }
                printLsException(
                    debugErr,
                    new LinStorException(
                        "The authentication policy was not changed due to a database error",
                        "The authentication policy was not changed",
                        "A database error was encountered while attempting to change the authentication policy",
                        detailsText,
                        null,
                        dbExc
                    )
                );
            }
            finally
            {
                rcfgWrLock.unlock();
            }
        }
        else
        {
            printMissingParamError(debugErr, PRM_REQAUTH_NAME);
        }
    }
}
