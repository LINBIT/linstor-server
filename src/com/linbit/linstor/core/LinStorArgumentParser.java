package com.linbit.linstor.core;

import java.io.File;

import picocli.CommandLine;

public class LinStorArgumentParser
{
    @CommandLine.Option(names = {"-c", "--controller-directory"}, description = "Working directory for the controller")
    private String controllerDirectory = null;
    @CommandLine.Option(names = {"-d", "--debug-console"}, description = "")
    private boolean debugConsole = false;
    @CommandLine.Option(
        names = {"--memory-database"},
        description = "Use a in memory database for testing. [format=dbtype;port;listenaddr]"
    )
    private String memoryDB = null;

    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "display this help message")
    private boolean usageHelpRequested;

    static LinStorArguments parseCommandLine(String[] args)
    {
        LinStorArgumentParser linArgParser = new LinStorArgumentParser();
        CommandLine cmd = new CommandLine(linArgParser);
        cmd.setCommandName("Controller");

        try
        {
            cmd.parse(args);
        }
        catch (Exception e)
        {
            System.err.println(e.getMessage());
            cmd.usage(System.err);
            System.exit(2);
        }

        if (cmd.isUsageHelpRequested())
        {
            cmd.usage(System.out);
            System.exit(0);
        }

        LinStorArguments cArgs = new LinStorArguments();
        if (linArgParser.controllerDirectory != null)
        {
                cArgs.setWorkingDirectory(linArgParser.controllerDirectory + "/");
                File workingDir = new File(cArgs.getWorkingDirectory());
                if (!workingDir.exists() || !workingDir.isDirectory())
                {
                    System.err.println("Error: Given controller runtime directory does not exist or is no directory");
                    System.exit(2);
                }
        }

        if (linArgParser.memoryDB != null)
        {
            cArgs.setMemoryDatabaseInitScript(linArgParser.memoryDB);
        }

        if (linArgParser.debugConsole)
        {
            cArgs.setStartDebugConsole(true);
        }

        return cArgs;
    }

    private LinStorArgumentParser()
    {
    }
}
