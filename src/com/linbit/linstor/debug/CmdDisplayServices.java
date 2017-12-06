package com.linbit.linstor.debug;

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
    public CmdDisplayServices()
    {
        super(
            new String[]
            {
                "DspSvc"
            },
            "Display services",
            "Displays information about the state of currently configured service instances",
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
        Map<ServiceName, SystemService> services = cmnDebugCtl.getSystemServiceMap();

        if (services.size() > 0)
        {
            debugOut.printf(
                "%-32s %-7s %s\n",
                "Service instance name",
                "Started",
                "Service type identifier"
            );
            printSectionSeparator(debugOut);
            int startedCtr = 0;
            for (Map.Entry<ServiceName, SystemService> sysSvcEntry : services.entrySet())
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
            debugOut.printf("%d services, %d started\n", services.size(), startedCtr);
        }
        else
        {
            debugOut.println(
                "No services are registered at this time."
            );
        }
    }
}
