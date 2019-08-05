package com.linbit.linstor.core;

import com.linbit.linstor.InternalApiConsts;
import java.io.File;

import picocli.CommandLine;

class ControllerArgumentParser
{
    @CommandLine.Option(names = {"-c", "--config-directory"},
        description = "Configuration directory for the controller"
    )
    private String configurationDirectory = "./";
    @CommandLine.Option(names = {"-d", "--debug-console"}, description = "")
    private boolean debugConsole = false;
    @CommandLine.Option(
        names = {"--memory-database"},
        description = "Use a in memory database for testing. [format=dbtype;port;listenaddr]"
    )
    private String memoryDB = null;

    @CommandLine.Option(
        names = {"-p", "--stack-traces"},
        description = "print error stack traces on standard error"
    )
    private boolean printStackTrace = false;

    @CommandLine.Option(names = {"-l", "--logs"}, description = "Path to the log directory")
    private String logDirectory = "./logs";

    @CommandLine.Option(names = {"--log-level"},
        description = "The desired log level. Options: ERROR, WARN, INFO, DEBUG, TRACE")
    private String logLevel = null;

    @CommandLine.Option(
        names = {"--rest-bind"},
        description = "Bind address for the REST HTTP server. e.g. 0.0.0.0:3370"
    )
    private String restBindAddress = null;

    @CommandLine.Option(
        names = {"--rest-bind-secure"},
        description = "Bind address for the REST HTTPS server. e.g. 0.0.0.0:3371"
    )
    private String restBindAddressSecure = null;

    @CommandLine.Option(names = {"-v", "--version"}, versionHelp = true, description = "Show the version number")
    private boolean versionInfoRequested;

    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "display this help message")
    private boolean usageHelpRequested;

    static ControllerCmdlArguments parseCommandLine(String[] args)
    {
        ControllerArgumentParser linArgParser = new ControllerArgumentParser();
        CommandLine cmd = new CommandLine(linArgParser);
        cmd.setCommandName("Controller");

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
            System.out.println("LINSTOR Controller " + LinStor.VERSION_INFO_PROVIDER.getVersion());
            System.exit(InternalApiConsts.EXIT_CODE_SHUTDOWN);
        }

        if (cmd.isUsageHelpRequested())
        {
            cmd.usage(System.out);
            System.exit(InternalApiConsts.EXIT_CODE_SHUTDOWN);
        }

        ControllerCmdlArguments cArgs = new ControllerCmdlArguments();
        if (linArgParser.configurationDirectory != null)
        {
            cArgs.setConfigurationDirectory(linArgParser.configurationDirectory + "/");
            File workingDir = cArgs.getConfigurationDirectory().toFile();
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

        if (linArgParser.memoryDB != null)
        {
            String[] memopts = linArgParser.memoryDB.split(";");
            if (memopts.length > 0)
            {
                cArgs.setInMemoryDbType(memopts[0]);
            }
            if (memopts.length > 1)
            {
                cArgs.setInMemoryDbPort(Integer.parseInt(memopts[1]));
            }
            if (memopts.length > 2)
            {
                cArgs.setInMemoryDbAddress(memopts[2]);
            }
        }

        if (linArgParser.restBindAddress != null)
        {
            cArgs.setRESTBindAddress(linArgParser.restBindAddress);
        }

        if (linArgParser.restBindAddressSecure != null)
        {
            cArgs.setRESTBindAddressSecure(linArgParser.restBindAddressSecure);
        }

        cArgs.setPrintStacktraces(linArgParser.printStackTrace);
        cArgs.setStartDebugConsole(linArgParser.debugConsole);

        if (linArgParser.logLevel != null)
        {
            cArgs.setLogLevel(linArgParser.logLevel);
        }

        return cArgs;
    }

    private ControllerArgumentParser()
    {
    }
}
