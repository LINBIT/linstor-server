package com.linbit.linstor.debug;

import com.google.inject.Inject;
import com.linbit.InvalidNameException;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Notifies the device manager to run resource operations (create/delete/...) on all resources
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class CmdRunDeviceManager extends BaseDebugCmd
{
    private static final Map<String, String> PARAMETER_DESCRIPTIONS = new TreeMap<>();

    private static final String PRM_RSC = "RSC";
    private static final String PRM_FILTER_RSC = "MATCHRSC";

    static
    {
        PARAMETER_DESCRIPTIONS.put(
            PRM_RSC,
            "Name of the resource to readjust"
        );
        PARAMETER_DESCRIPTIONS.put(
            PRM_FILTER_RSC,
            "Filter pattern to apply to the resource name for selecting resources\n" +
            "to readjust"
        );
    }

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
            "Run device manager operations on selected resources",
            "The device manager is notified to run operations on selected resources, thereby attempting" +
            "to adjust the state of those resources to their respective target states.\n\n" +
            "If the device manager service is stopped, operations will commonly run when the\n" +
            "device manager service is restarted.",
            PARAMETER_DESCRIPTIONS,
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
        String prmRsc = parameters.get(PRM_RSC);
        String prmFilter = parameters.get(PRM_FILTER_RSC);
        Lock rscDfnRdLock = rscDfnMapLock.readLock();

        if (prmRsc == null)
        {
            Matcher nameMatcher = null;
            if (prmFilter != null)
            {
                Pattern namePattern = Pattern.compile(prmFilter, Pattern.CASE_INSENSITIVE);
                nameMatcher = namePattern.matcher("");
            }

            Map<ResourceName, UUID> slctRsc = new TreeMap<>();
            try
            {
                rscDfnRdLock.lock();
                if (nameMatcher == null)
                {
                    // Select all resources
                    for (ResourceDefinition curRscDfn : rscDfnMap.values())
                    {
                        slctRsc.put(curRscDfn.getName(), curRscDfn.getUuid());
                    }
                    deviceManager.getUpdateTracker().checkMultipleResources(slctRsc);
                    debugOut.println("Device manager notified to adjust all resources.");
                }
                else
                {
                    // Select resources by filter match
                    for (ResourceDefinition curRscDfn : rscDfnMap.values())
                    {
                        ResourceName rscName = curRscDfn.getName();
                        nameMatcher.reset(rscName.value);
                        if (nameMatcher.find())
                        {
                            slctRsc.put(rscName, curRscDfn.getUuid());
                        }
                    }
                    deviceManager.getUpdateTracker().checkMultipleResources(slctRsc);
                    debugOut.println("Device manager notified to adjust " + slctRsc.size() + " selected resources.");
                }
            }
            finally
            {
                rscDfnRdLock.unlock();
            }
        }
        else
        {
            if (prmFilter == null)
            {
                // Schedule a single resource
                ResourceDefinition rscDfn = null;
                try
                {
                    rscDfnRdLock.lock();
                    rscDfn = rscDfnMap.get(new ResourceName(prmRsc));
                }
                catch (InvalidNameException nameExc)
                {
                    debugPrintHelper.printError(
                        debugErr,
                        "The value specified for the parameter " + PRM_RSC + " is not a valid " +
                        "resource name",
                        null,
                        "Specify a valid resource name to select a single resource, or set a filter pattern " +
                        "using the " + PRM_FILTER_RSC + " parameter to select resources that have a name\n" +
                        "that matches the pattern.",
                        String.format(
                            "The specified value was '%s'.", prmRsc
                        )
                    );
                }
                finally
                {
                    rscDfnRdLock.unlock();
                }

                if (rscDfn != null)
                {
                    ResourceName rscName = rscDfn.getName();
                    deviceManager.getUpdateTracker().checkResource(rscDfn.getUuid(), rscName);
                    debugOut.println("Device manager notified to adjust the resource '" + rscName.displayValue + "'");
                }
                else
                {
                    debugPrintHelper.printError(
                        debugErr,
                        "The resource named '" + prmRsc + "' does not exist",
                        null,
                        "- Check whether the resource name is spelled correctly\n" +
                        "- Check whether the satellite is connected to a controller and has received\n" +
                        "  information about the cluster configuration",
                        null
                    );
                }
            }
            else
            {
                debugPrintHelper.printError(
                    debugErr,
                    "The command line contains conflicting parameters",
                    "The parameters " + PRM_RSC + " and " + PRM_FILTER_RSC + " were combined " +
                    "in the command line.\n" +
                    "Combining the two parameters is not supported.",
                    "Specify either the " + PRM_RSC + " parameter to adjust a single resource,\n" +
                    " or specify the " + PRM_FILTER_RSC + " parameter to adjust all resources\n" +
                    "that have a name matching the specified filter.",
                    null
                );
            }
        }
    }
}
