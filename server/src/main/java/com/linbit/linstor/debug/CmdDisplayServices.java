package com.linbit.linstor.debug;

import javax.inject.Inject;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.linstor.security.AccessContext;

import java.io.PrintStream;
import java.util.Map;

/**
 * Displays information about the module's system services (Controller or Satellite)
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class CmdDisplayServices extends BaseDebugCmd
{
    private final Map<ServiceName, SystemService> systemServicesMap;

    @Inject
    public CmdDisplayServices(Map<ServiceName, SystemService> systemServicesMapRef)
    {
        super(
            new String[]
            {
                "DspSvc"
            },
            "Display services",
            "Displays information about the state of currently configured service instances",
            null,
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
        if (systemServicesMap.size() > 0)
        {
            debugOut.printf(
                "%-32s %-7s %s\n",
                "Service instance name",
                "Started",
                "Service type identifier"
            );
            printSectionSeparator(debugOut);
            int startedCtr = 0;
            for (Map.Entry<ServiceName, SystemService> sysSvcEntry : systemServicesMap.entrySet())
            {
                ServiceName svcName = sysSvcEntry.getKey();
                SystemService sysSvc = sysSvcEntry.getValue();
                boolean started = sysSvc.isStarted();
                if (started)
                {
                    ++startedCtr;
                }
                debugOut.printf(
                    "%-32s %-7s %s\n",
                    svcName.getDisplayName(),
                    started ? "Y" : "N",
                    sysSvc.getServiceName().getDisplayName()
                );
            }
            printSectionSeparator(debugOut);
            debugOut.printf("%d services, %d started\n", systemServicesMap.size(), startedCtr);
        }
        else
        {
            debugOut.println(
                "No services are registered at this time."
            );
        }
    }
}
