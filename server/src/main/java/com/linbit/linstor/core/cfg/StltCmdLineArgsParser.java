package com.linbit.linstor.core.cfg;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.LinStor;

import java.io.File;
import java.util.Arrays;
import java.util.stream.Collectors;

import picocli.CommandLine;

class StltCmdLineArgsParser
{
    @CommandLine.Option(names = {"-c", "--config-directory"},
        description = "Configuration directory for the controller"
    )
    private @Nullable String configurationDirectory;

    @CommandLine.Option(names = {"-d", "--debug-console"}, description = "")
    private @Nullable Boolean debugConsole;

    @CommandLine.Option(
        names = {"-p", "--stack-traces"},
        description = "print error stack traces on standard error"
    )
    private @Nullable Boolean printStackTrace;

    @CommandLine.Option(names = {"-l", "--logs"}, description = "Path to the log directory")
    private @Nullable String logDirectory;

    @CommandLine.Option(names = {"--log-level"},
        description = "The desired log level. Options: ERROR, WARN, INFO, DEBUG, TRACE")
    private @Nullable String logLevel;

    @CommandLine.Option(names = {"--log-level-linstor"},
        description = "The desired log level. Options: ERROR, WARN, INFO, DEBUG, TRACE")
    private @Nullable String logLevelLinstor;


    @CommandLine.Option(names = {"-v", "--version"}, versionHelp = true, description = "Show the version number")
    private @Nullable Boolean versionInfoRequested;

    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "display this help message")
    private @Nullable Boolean usageHelpRequested;

    @CommandLine.Option(
        names = {"-k", "--keep-res"},
        description =
            "if this regex matches a file of a drbd resource file created by linstor, the " +
            "matched file will NOT be deleted"
    )
    private @Nullable String keepResourceRegex;

    @CommandLine.Option(names = {"--port"}, description = "overrides the plain port")
    private @Nullable Integer plainPort;

    @CommandLine.Option(names = {"-s", "--skip-hostname-check"},
        description = "deprecated. this argument will be silently ignored")
    @Deprecated
    private @Nullable Boolean skipHostNameCheck;

    @CommandLine.Option(names = {"--skip-drbd-check"},
                        description = "deprecated. this argument will be silently ignored.")
    @Deprecated
    private boolean skipDrbdCheck;

    @CommandLine.Option(names = {"--bind-address"}, description = "overrides the bind address")
    private @Nullable String bindAddress;

    @CommandLine.Option(names = {"--override-node-name"}, description = "Overrides node name used in error reports.")
    private @Nullable String nodeName;

    @CommandLine.Option(names = { "--remote-spdk" }, hidden = true)
    private boolean remoteSpdk;

    @CommandLine.Option(names = {"--ebs" }, hidden = true)
    private boolean ebs;

    @CommandLine.Option(names = "--allow-ext-files", split = ",", description = "Whitelist paths for external files")
    private @Nullable String[] extFilesWhitelist;

    static void parseCommandLine(String[] args, StltConfig stltCfg)
    {
        StltCmdLineArgsParser linArgParser = new StltCmdLineArgsParser();
        CommandLine cmd = new CommandLine(linArgParser);
        cmd.setCommandName("Satellite");
        cmd.setOverwrittenOptionsAllowed(true);

        try
        {
            cmd.parse(args);
        }
        catch (Exception exc)
        {
            System.err.println(exc.getMessage());
            cmd.usage(System.err);
            System.exit(InternalApiConsts.EXIT_CODE_CMDLINE_ERROR);
        }

        if (cmd.isVersionHelpRequested())
        {
            System.out.println("LINSTOR Satellite " + LinStor.VERSION_INFO_PROVIDER.getVersion());
            System.exit(InternalApiConsts.EXIT_CODE_SHUTDOWN);
        }

        if (cmd.isUsageHelpRequested())
        {
            cmd.usage(System.out);
            System.exit(InternalApiConsts.EXIT_CODE_SHUTDOWN);
        }

        if (linArgParser.configurationDirectory != null)
        {
            stltCfg.setConfigDir(linArgParser.configurationDirectory + "/");
            File workingDir = stltCfg.getConfigPath().toFile();
            if (workingDir.exists() && !workingDir.isDirectory())
            {
                System.err.println("Error: Given configuration directory is no directory");
                System.exit(InternalApiConsts.EXIT_CODE_CMDLINE_ERROR);
            }
        }

        stltCfg.setRemoteSpdk(linArgParser.remoteSpdk);
        stltCfg.setEbs(linArgParser.ebs);

        stltCfg.setLogDirectory(linArgParser.logDirectory);

        stltCfg.setLogPrintStackTrace(linArgParser.printStackTrace);
        stltCfg.setDebugConsoleEnable(linArgParser.debugConsole);
        stltCfg.setDrbdKeepResPattern(linArgParser.keepResourceRegex);
        stltCfg.setNetPort(linArgParser.plainPort);
        stltCfg.setNetBindAddress(linArgParser.bindAddress);
        stltCfg.setStltOverrideNodeName(linArgParser.nodeName);

        stltCfg.setLogLevel(linArgParser.logLevel);
        stltCfg.setLogLevelLinstor(linArgParser.logLevelLinstor);

        if (linArgParser.extFilesWhitelist != null)
        {
            stltCfg.setExternalFilesWhitelist(
                Arrays.stream(linArgParser.extFilesWhitelist).collect(Collectors.toSet())
            );
        }
    }

    private StltCmdLineArgsParser()
    {
    }
}
