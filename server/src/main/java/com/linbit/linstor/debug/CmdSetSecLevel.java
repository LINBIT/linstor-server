package com.linbit.linstor.debug;

import javax.inject.Inject;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.SecurityLevel;
import com.linbit.linstor.security.SecurityLevelSetter;
import org.slf4j.event.Level;

import javax.inject.Named;
import java.io.PrintStream;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;


public class CmdSetSecLevel extends BaseDebugCmd
{
    public static final Map<String, String> PARAMETER_DESCRIPTIONS = new TreeMap<>();

    public static final String PRM_SECLVL_NAME = "SECLVL";

    public static final String PRM_SECLVL_NO_SECURITY   = "NO_SECURITY";
    public static final String PRM_SECLVL_RBAC          = "RBAC";
    public static final String PRM_SECLVL_MAC           = "MAC";

    public static final String LEVEL_SET_FORMAT = "Global security level set to \u001b[1;37m%s\u001b[0m\n";

    static
    {
        PARAMETER_DESCRIPTIONS.put(PRM_SECLVL_NAME,
            "The security level to set as the global security level\n" +
            "    NO_SECURITY\n" +
            "        No object protection, public access to all objects.\n" +
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

    private final ErrorReporter errorReporter;
    private final ReadWriteLock reconfigurationLock;
    private final SecurityLevelSetter securityLevelSetter;

    @Inject
    public CmdSetSecLevel(
        ErrorReporter errorReporterRef,
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        SecurityLevelSetter securityLevelSetterRef
    )
    {
        super(
            new String[]
            {
                "SetSecLvl"
            },
            "Set security level",
            "Sets the global security level",
            PARAMETER_DESCRIPTIONS,
            null
        );

        errorReporter = errorReporterRef;
        reconfigurationLock = reconfigurationLockRef;
        securityLevelSetter = securityLevelSetterRef;
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
        try
        {
            reconfigurationLock.writeLock().lock();

            String secLevelText = parameters.get(PRM_SECLVL_NAME);
            if (secLevelText != null)
            {
                try
                {
                    secLevelText = secLevelText.toUpperCase();
                    switch (secLevelText)
                    {
                        case PRM_SECLVL_NO_SECURITY:
                            securityLevelSetter.setSecurityLevel(accCtx, SecurityLevel.NO_SECURITY);
                            debugOut.printf(LEVEL_SET_FORMAT, PRM_SECLVL_NO_SECURITY);
                            break;
                        case PRM_SECLVL_RBAC:
                            securityLevelSetter.setSecurityLevel(accCtx, SecurityLevel.RBAC);
                            debugOut.printf(LEVEL_SET_FORMAT, PRM_SECLVL_RBAC);
                            break;
                        case PRM_SECLVL_MAC:
                            securityLevelSetter.setSecurityLevel(accCtx, SecurityLevel.MAC);
                            debugOut.printf(LEVEL_SET_FORMAT, PRM_SECLVL_MAC);
                            break;
                        default:
                            printError(debugErr,
                                "The specified security level is not valid.",
                                String.format(
                                    "The value '%s' specified for the parameter %s is not a valid security level name",
                                    secLevelText, PRM_SECLVL_NAME
                                ),
                                String.format("Specify a valid security level.\n" +
                                    "Valid security levels are:\n" +
                                    "    %s\n" +
                                    "    %s\n" +
                                    "    %s\n",
                                    PRM_SECLVL_NO_SECURITY, PRM_SECLVL_RBAC, PRM_SECLVL_MAC
                                ),
                                null
                            );
                            break;
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
                            "The security level was not changed due to a database error",
                            "The security level was not changed",
                            "A database error was encountered while attempting to change the security level",
                            detailsText,
                            null,
                            dbExc
                        )
                    );
                }
            }
            else
            {
                this.printMissingParamError(debugErr, PRM_SECLVL_NAME);
            }
        }
        finally
        {
            reconfigurationLock.writeLock().unlock();
        }
    }
}
