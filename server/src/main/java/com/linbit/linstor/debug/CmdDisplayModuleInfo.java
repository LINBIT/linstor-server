package com.linbit.linstor.debug;

import jakarta.inject.Inject;
import java.io.PrintStream;
import java.util.Map;

import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.VersionInfoProvider;
import com.linbit.linstor.logging.ErrorReporter;

import jakarta.inject.Named;

/**
 * Displays information about the program module (Controller or Satellite)
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class CmdDisplayModuleInfo extends BaseDebugCmd
{
    private final ErrorReporter errorReporter;
    private final String moduleName;

    @Inject
    public CmdDisplayModuleInfo(
        ErrorReporter errorReporterRef,
        @Named(CoreModule.MODULE_NAME) String moduleNameRef
    )
    {
        super(
            new String[]
            {
                "DspModInf"
            },
            "Display module information",
            "Displays information about the program module that is being debugged",
            null,
            null
        );

        errorReporter = errorReporterRef;
        moduleName = moduleNameRef;
    }

    @Override
    public void execute(
        PrintStream debugOut,
        PrintStream debugErr,
        Map<String, String> parameters
    ) throws Exception
    {
        VersionInfoProvider versionInfoProvider = LinStor.VERSION_INFO_PROVIDER;

        debugOut.printf(
            """
            PROGRAM:        %s
            MODULE:         %s
            VERSION:        %s (%s)
            BUILD TIME:     %s
            INSTANCE ID:    %s
            """,
            LinStor.PROGRAM, moduleName,
            versionInfoProvider.getVersion(), versionInfoProvider.getGitCommitId(),
            versionInfoProvider.getBuildTime(),
            errorReporter.getInstanceId()
        );
    }
}
