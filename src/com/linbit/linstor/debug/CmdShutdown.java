package com.linbit.linstor.debug;

import javax.inject.Inject;
import java.io.PrintStream;
import java.util.Map;

import com.linbit.linstor.core.ApplicationLifecycleManager;
import com.linbit.linstor.security.AccessContext;

/**
 * Shuts down the Controller or Satellite
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class CmdShutdown extends BaseDebugCmd
{
    private final ApplicationLifecycleManager applicationLifecycleManager;

    @Inject
    public CmdShutdown(ApplicationLifecycleManager applicationLifecycleManagerRef)
    {
        super(
            new String[]
            {
                "ShtDwn",
                "Shutdown"
            },
            "Shutdown",
            "Shuts down the module instance",
            null,
            null
        );

        applicationLifecycleManager = applicationLifecycleManagerRef;
    }

    @Override
    public void execute(
        PrintStream debugOut,
        PrintStream debugErr,
        AccessContext accCtx,
        Map<String, String> parameters
    ) throws Exception
    {
        applicationLifecycleManager.shutdown(accCtx);
        debugCon.exitConsole();
    }
}
