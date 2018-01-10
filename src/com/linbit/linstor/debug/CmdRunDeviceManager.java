package com.linbit.linstor.debug;

import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.security.AccessContext;
import java.io.PrintStream;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.locks.Lock;

/**
 * Notifies the device manager to run resource operations (create/delete/...) on all resources
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class CmdRunDeviceManager extends BaseSatelliteDebugCmd
{
    public CmdRunDeviceManager()
    {
        super(
            new String[]
            {
                "RunDevMgr"
            },
            "Runs device manager operations on all resources",
            "The device manager is notified to run operations on all resources, thereby attempting" +
            "to adjust the state of any resources known to the satellite to their respective\n" +
            "target states.\n\n" +
            "If the device manager service is stopped, operations will commonly run when the\n" +
            "device manager service is restarted.",
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
    )
        throws Exception
    {
        Map<ResourceName, ResourceDefinition> rscDfnMap = debugCtl.getRscDfnMap();
        Map<ResourceName, UUID> allRsc = new TreeMap<>();
        Lock rscDfnRdLock = satellite.rscDfnMapLock.readLock();
        try
        {
            rscDfnRdLock.lock();
            for (ResourceDefinition curRscDfn : rscDfnMap.values())
            {
                allRsc.put(curRscDfn.getName(), curRscDfn.getUuid());
            }
        }
        finally
        {
            rscDfnRdLock.unlock();
        }

        DeviceManager devMgr = satellite.getDeviceManager();
        if (devMgr != null)
        {
            devMgr.getUpdateTracker().checkMultipleResources(allRsc);
            debugOut.println("Device manager notified to adjust all resources.");
        }
        else
        {
            printError(
                debugErr,
                "Notification of the device manager failed",
                "No instance of the device manager exists",
                "- Check whether the device manager has been created and initialized\n" +
                "- If the satellite is still running the startup procedure, retry the command\n" +
                "  after the startup procedure is complete",
                null
            );
        }
    }
}
