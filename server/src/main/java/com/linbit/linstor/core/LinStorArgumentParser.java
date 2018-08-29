package com.linbit.linstor.core;

import java.io.File;
import picocli.CommandLine;

public class LinStorArgumentParser
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

    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "display this help message")
    private boolean usageHelpRequested;

    @CommandLine.Option(
        names = {"-k", "--keep-res"},
        description =
            "if this regex matches a file of a drbd resource file created by linstor, the " +
            "matched file will NOT be deleted"
    )
    private String keepResourceRegex;

    static LinStorArguments parseCommandLine(String[] args)
    {
        LinStorArgumentParser linArgParser = new LinStorArgumentParser();
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
            System.exit(2);
        }

        if (cmd.isUsageHelpRequested())
        {
            cmd.usage(System.out);
            System.exit(0);
        }

        LinStorArguments cArgs = new LinStorArguments();
        if (linArgParser.configurationDirectory != null)
        {
                cArgs.setConfigurationDirectory(linArgParser.configurationDirectory + "/");
                File workingDir = new File(cArgs.getConfigurationDirectory());
                if (workingDir.exists() && !workingDir.isDirectory())
                {
                    System.err.println("Error: Given configuration directory is no directory");
                    System.exit(2);
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

        cArgs.setPrintStacktraces(linArgParser.printStackTrace);
        cArgs.setStartDebugConsole(linArgParser.debugConsole);
        if (linArgParser.keepResourceRegex != null)
        {
            cArgs.setKeepResRegex(linArgParser.keepResourceRegex);
        }

        return cArgs;
    }

    private LinStorArgumentParser()
    {
    }
}
