package com.linbit.drbdmanage.debug;

import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.drbdmanage.security.AccessContext;
import java.io.PrintStream;
import java.util.Map;
import java.util.TreeMap;

/**
 * Ends execution of a Controller service
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class CmdEndService extends BaseControllerDebugCmd
{
    private static final String PRM_SVC_NAME = "SVC";

    private static final Map<String, String> PARAMETER_DESCRIPTIONS = new TreeMap<>();

    static
    {
        PARAMETER_DESCRIPTIONS.put(
            PRM_SVC_NAME,
            "The name of the service instance to end"
        );
    }

    public CmdEndService()
    {
        super(
            new String[]
            {
                "EndSvc"
            },
            "End service",
            "Ends execution of a service",
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
    ) throws Exception
    {
        Map<ServiceName, SystemService> services = debugCtl.getSystemServiceMap();
        String serviceNamePrm = parameters.get(PRM_SVC_NAME);
        if (serviceNamePrm != null)
        {
            try
            {
                ServiceName svcName = new ServiceName(serviceNamePrm);
                SystemService sysSvc = services.get(svcName);
                if (sysSvc != null)
                {
                    sysSvc.shutdown();
                    debugOut.printf(
                        "End service '%s' initiated.\n",
                        sysSvc.getInstanceName().getDisplayName()
                    );
                }
                else
                {
                    printError(
                        debugErr,
                        String.format(
                            "No service with an instance name of '%s' was found",
                            svcName.getDisplayName()
                        ),
                        null,
                        "Enter a valid service instance name.",
                        null
                    );
                }
            }
            catch (InvalidNameException nameExc)
            {
                printError(
                    debugErr,
                    String.format(
                        "The service name '%s' specified in parameter '%s' is " +
                        "not a valid name for a service.",
                        serviceNamePrm, PRM_SVC_NAME
                    ),
                    null,
                    "Enter a valid service name.",
                    null
                );
            }
        }
        else
        {
            printMissingParamError(debugErr, PRM_SVC_NAME);
        }
    }
}
