package com.linbit.linstor.core;

import com.linbit.linstor.InternalApiConsts;
import java.io.File;
import picocli.CommandLine;

class SatelliteArgumentParser
{
    @CommandLine.Option(names = {"-c", "--config-directory"},
        description = "Configuration directory for the controller"
    )
    private String configurationDirectory = "./";
    @CommandLine.Option(names = {"-d", "--debug-console"}, description = "")
    private boolean debugConsole = false;

    @CommandLine.Option(
        names = {"-p", "--stack-traces"},
        description = "print error stack traces on standard error"
    )
    private boolean printStackTrace = false;

    @CommandLine.Option(names = {"-l", "--logs"}, description = "Path to the log directory")
    private String logDirectory = "./logs";

    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "display this help message")
    private boolean usageHelpRequested;

    @CommandLine.Option(
        names = {"-k", "--keep-res"},
        description =
            "if this regex matches a file of a drbd resource file created by linstor, the " +
            "matched file will NOT be deleted"
    )
    private String keepResourceRegex;

    @CommandLine.Option(names = {"--port"}, description = "overrides the plain port")
    private Integer plainPort = null;

    @CommandLine.Option(names = {"-s", "--skip-hostname-check"}, description = "skips the hostname check on the " +
        "satellite when a controller assigns the nodename")
    private boolean skipHostNameCheck = false;

    @CommandLine.Option(names = {"--skip-drbd-check"},
                        description = "skips the check for a supported DRBD installation")
    private boolean skipDrbdCheck = true;

    @CommandLine.Option(names = {"--bind-address"}, description = "overrides the bind address")
    private String bindAddress = null;

    @CommandLine.Option(names = {"--override-node-name"}, description = "Overrides node name used in error reports.")
    private String nodeName = null;

    static SatelliteCmdlArguments parseCommandLine(String[] args)
    {
        SatelliteArgumentParser linArgParser = new SatelliteArgumentParser();
        CommandLine cmd = new CommandLine(linArgParser);
        cmd.setCommandName("Satellite");

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

        if (cmd.isUsageHelpRequested())
        {
            cmd.usage(System.out);
            System.exit(InternalApiConsts.EXIT_CODE_SHUTDOWN);
        }

        SatelliteCmdlArguments cArgs = new SatelliteCmdlArguments();
        if (linArgParser.configurationDirectory != null)
        {
            cArgs.setConfigurationDirectory(linArgParser.configurationDirectory + "/");
            File workingDir = new File(cArgs.getConfigurationDirectory());
            if (workingDir.exists() && !workingDir.isDirectory())
            {
                System.err.println("Error: Given configuration directory is no directory");
                System.exit(InternalApiConsts.EXIT_CODE_CMDLINE_ERROR);
            }
        }

        if (linArgParser.logDirectory != null)
        {
            cArgs.setLogDirectory(linArgParser.logDirectory);
        }

        cArgs.setPrintStacktraces(linArgParser.printStackTrace);
        cArgs.setStartDebugConsole(linArgParser.debugConsole);
        if (linArgParser.keepResourceRegex != null)
        {
            cArgs.setKeepResRegex(linArgParser.keepResourceRegex);
        }
        cArgs.setOverridePlainPort(linArgParser.plainPort);
        cArgs.setSkipHostnameCheck(linArgParser.skipHostNameCheck);
        cArgs.setSkipDrbdCheck(linArgParser.skipDrbdCheck);
        cArgs.setBindAddress(linArgParser.bindAddress);
        if (linArgParser.nodeName != null)
        {
            cArgs.setOverrideNodeName(linArgParser.nodeName);
        }

        return cArgs;
    }

    private SatelliteArgumentParser()
    {
    }
}
