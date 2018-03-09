package com.linbit.linstor.debug;

import javax.inject.Inject;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.linstor.security.AccessContext;

import java.io.PrintStream;
import java.util.Map;
import java.util.TreeMap;

/**
 * Starts execution of a Controller or Satellite service
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class CmdStartService extends BaseDebugCmd
{
    private static final String PRM_SVC_NAME = "SERVICE";

    private static final Map<String, String> PARAMETER_DESCRIPTIONS = new TreeMap<>();

    static
    {
        PARAMETER_DESCRIPTIONS.put(
            PRM_SVC_NAME,
            "The name of the service instance to start"
        );
    }

    private final Map<ServiceName, SystemService> systemServicesMap;

    @Inject
    public CmdStartService(Map<ServiceName, SystemService> systemServicesMapRef)
    {
        super(
            new String[]
            {
                "StrSvc"
            },
            "Start service",
            "Starts execution of a service instance",
            PARAMETER_DESCRIPTIONS,
            null
        );

        systemServicesMap = systemServicesMapRef;
    }

    @Override
    public void execute(
        PrintStream debugOut,
        PrintStream debugErr,
        AccessContext accCtx,
        Map<String, String> parameters
    ) throws Exception
    {
        String serviceNamePrm = parameters.get(PRM_SVC_NAME);
        if (serviceNamePrm != null)
        {
            try
            {
                ServiceName svcName = new ServiceName(serviceNamePrm);
                SystemService sysSvc = systemServicesMap.get(svcName);
                if (sysSvc != null)
                {
                    sysSvc.start();
                    debugOut.printf(
                        "Start service '%s' initiated.\n",
                        sysSvc.getInstanceName().getDisplayName()
                    );
                }
                else
                {
                    printError(
                        debugErr,
                        String.format(
                            "No service with an instance name of '%s' was found.",
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
