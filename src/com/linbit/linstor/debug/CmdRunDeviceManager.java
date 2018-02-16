package com.linbit.linstor.debug;

import com.google.inject.Inject;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.security.AccessContext;

import javax.inject.Named;
import java.io.PrintStream;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * Notifies the device manager to run resource operations (create/delete/...) on all resources
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class CmdRunDeviceManager extends BaseDebugCmd
{
    private final DeviceManager deviceManager;
    private final ReadWriteLock rscDfnMapLock;
    private final CoreModule.ResourceDefinitionMap rscDfnMap;

    @Inject
    public CmdRunDeviceManager(
        DeviceManager deviceManagerRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef
    )
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
            null
        );

        deviceManager = deviceManagerRef;
        rscDfnMapLock = rscDfnMapLockRef;
        rscDfnMap = rscDfnMapRef;
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
        Map<ResourceName, UUID> allRsc = new TreeMap<>();
        Lock rscDfnRdLock = rscDfnMapLock.readLock();
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

        deviceManager.getUpdateTracker().checkMultipleResources(allRsc);
        debugOut.println("Device manager notified to adjust all resources.");
    }
}
