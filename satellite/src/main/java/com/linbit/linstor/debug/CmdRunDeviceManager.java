package com.linbit.linstor.debug;

import com.linbit.InvalidNameException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.core.StltUpdateTracker;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.security.AccessContext;

import javax.inject.Inject;
import javax.inject.Named;

import java.io.PrintStream;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import reactor.core.publisher.Flux;

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
    private final ControllerPeerConnector ctrlPeerConn;

    @Inject
    public CmdRunDeviceManager(
        DeviceManager deviceManagerRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        ControllerPeerConnector ctrlPeerConnRef
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
        ctrlPeerConn = ctrlPeerConnRef;
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

        try
        {
            NodeName localNodeName = ctrlPeerConn.getLocalNodeName();
            if (localNodeName == null)
            {
                throw new LinStorException(
                    getClass().getSimpleName() + ": Unable to select the local node, command aborted",
                    "The command cannot be executed in the device manager's current state",
                    "The satellite cannot select the local node." +
                    "Possible causes include:\n" +
                    "    - The satellite may not be connected to a controller\n" +
                    "    - Initialization data may not yet have been sent by the controller\n" +
                    "    - The satellite may not be registered in the controller's database",
                    "- Make sure the satellite is connected to a controller\n" +
                    "- Check whether the satellite has received data from the controller\n" +
                    "- Check the satellite's node name\n" +
                    "- Check whether a node entry for the satellite's node name appears in the\n" +
                    "  satellite's node list",
                    null
                );
            }

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
                            Resource curRsc = curRscDfn.getResource(accCtx, localNodeName);
                            if (curRsc != null)
                            {
                                slctRsc.put(curRscDfn.getName(), curRsc.getUuid());
                            }
                        }
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
                                Resource curRsc = curRscDfn.getResource(accCtx, localNodeName);
                                if (curRsc != null)
                                {
                                    slctRsc.put(rscName, curRsc.getUuid());
                                }
                            }
                        }
                    }
                }
                finally
                {
                    rscDfnRdLock.unlock();
                }

                StltUpdateTracker updTracker = deviceManager.getUpdateTracker();
                for (Map.Entry<ResourceName, UUID> entry : slctRsc.entrySet())
                {
                    Flux<ApiCallRc> fluxObj = updTracker.updateResource(
                        entry.getValue(), entry.getKey(), localNodeName
                    );
                    // Subscribe a noop consumer
                    fluxObj.subscribe(
                        rc ->
                        {
                        }
                    );
                }

                if (nameMatcher == null)
                {
                    debugOut.println("Device manager notified to adjust all resources.");
                }
                else
                {
                    debugOut.println("Device manager notified to adjust " + slctRsc.size() + " selected resources.");
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
                        Resource rsc = rscDfn.getResource(accCtx, localNodeName);
                        if (rsc != null)
                        {
                            Flux<ApiCallRc> fluxObj = deviceManager.getUpdateTracker().updateResource(
                                rsc.getUuid(), rscName, localNodeName
                            );
                            // Subscribe a noop consumer
                            fluxObj.subscribe(
                                rc ->
                                {
                                }
                            );
                            debugOut.println(
                                "Device manager notified to adjust the resource '" +
                                rscName.displayValue + "'"
                            );
                        }
                        else
                        {
                            debugPrintHelper.printError(
                                debugErr,
                                "Cannot run device manager actions on resource '" + rscName + "'",
                                "The resource is not assigned to the local node",
                                null,
                                null
                            );
                        }
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
        catch (LinStorException exc)
        {
            debugPrintHelper.printLsException(debugErr, exc);
        }
    }
}
