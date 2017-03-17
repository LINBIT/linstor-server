package com.linbit.drbdmanage.debug;

import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.drbdmanage.security.AccessContext;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Map;

/**
 * Displays information about the Controller's system services
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class CmdDisplayServices extends BaseControllerDebugCmd
{
    public CmdDisplayServices()
    {
        super(
            new String[]
            {
                "DspSvc"
            },
            "Display services",
            "Displays a table with information about the Controller's system services",
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
        Map<ServiceName, SystemService> services = debugCtl.getSystemServiceMap();

        if (services.size() > 0)
        {
            char[] rulerData = new char[78];
            Arrays.fill(rulerData, '-');
            String ruler = new String(rulerData);

            debugOut.printf(
                "%-32s %-7s %s\n",
                "Service instance name",
                "Started",
                "Service type identifier"
            );
            debugOut.println(ruler);
            int startedCtr = 0;
            for (SystemService sysSvc : services.values())
            {
                boolean started = sysSvc.isStarted();
                if (started)
                {
                    ++startedCtr;
                }
                debugOut.printf(
                    "%-32s %-7s %s\n",
                    sysSvc.getInstanceName().getDisplayName(),
                    started ? "Y" : "N",
                    sysSvc.getServiceName().getDisplayName()
                );
            }
            debugOut.println(ruler);
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
