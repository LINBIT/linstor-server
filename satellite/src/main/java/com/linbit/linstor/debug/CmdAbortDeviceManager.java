package com.linbit.linstor.debug;

import com.linbit.linstor.core.DeviceManager;

import jakarta.inject.Inject;

import java.io.PrintStream;
import java.util.Map;

/**
 * Notifies the device manager to run resource operations (create/delete/...) on all resources
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class CmdAbortDeviceManager extends BaseDebugCmd
{
    private final DeviceManager deviceManager;

    @Inject
    public CmdAbortDeviceManager(
        DeviceManager deviceManagerRef
    )
    {
        super(
            new String[]
            {
                "AbrtDevMgr"
            },
            "Abort dispatching resource handlers",
            """
            The device manager is notified to abort dispatching resource handlers, so that
            a currently proceeding run of the device manager ends early and potentially
            without checking and readjusting all resources.""",
            null,
            null
        );

        deviceManager = deviceManagerRef;
    }

    @Override
    public void execute(
        PrintStream debugOut,
        PrintStream debugErr,
        Map<String, String> parameters
    )
        throws Exception
    {
        deviceManager.abortDeviceHandlers();
        debugOut.println("Device manager notified to abort dispatching resource handlers.");
    }
}
