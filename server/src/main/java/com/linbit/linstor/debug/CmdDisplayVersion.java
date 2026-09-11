package com.linbit.linstor.debug;

import jakarta.inject.Inject;
import java.io.PrintStream;
import java.util.Map;

import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.VersionInfoProvider;

/**
 * Displays the version of the Controller or Satellite
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class CmdDisplayVersion extends BaseDebugCmd
{
    @Inject
    public CmdDisplayVersion()
    {
        super(
            new String[]
            {
                "DspVsn",
                "Version"
            },
            "Display version",
            "Displays the version of the program module that is being debugged",
            null,
            null
        );
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
            VERSION:        %s (%s)
            BUILD TIME:     %s
            """,
            versionInfoProvider.getVersion(), versionInfoProvider.getGitCommitId(),
            versionInfoProvider.getBuildTime()
        );
    }
}
