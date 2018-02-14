package com.linbit.linstor.core;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.File;

public class LinStorArgumentParser
{
    private static final String CONTROLLER_DIRECTORY = "controller_directory";
    private static final String DEBUG_CONSOLE = "debug_console";
    private static final String MEMORY_DB = "memory_database";

    static LinStorArguments parseCommandLine(String[] args)
    {
        Options opts = new Options();
        opts.addOption(Option.builder("h").longOpt("help").required(false).build());
        opts.addOption(Option.builder("c").longOpt(CONTROLLER_DIRECTORY).hasArg().required(false).build());
        opts.addOption(Option.builder("d").longOpt(DEBUG_CONSOLE).required(false).build());
        opts.addOption(Option.builder().longOpt(MEMORY_DB).hasArg().required(false).build());

        CommandLineParser parser = new DefaultParser();
        LinStorArguments cArgs = new LinStorArguments();
        try
        {
            CommandLine cmd = parser.parse(opts, args);

            if (cmd.hasOption("help"))
            {
                HelpFormatter helpFrmt = new HelpFormatter();
                helpFrmt.printHelp("Controller", opts);
                System.exit(0);
            }

            if (cmd.hasOption(CONTROLLER_DIRECTORY))
            {
                cArgs.setWorkingDirectory(cmd.getOptionValue(CONTROLLER_DIRECTORY) + "/");
                File workingDir = new File(cArgs.getWorkingDirectory());
                if (!workingDir.exists() || !workingDir.isDirectory())
                {
                    System.err.println("Error: Given controller runtime directory does not exist or is no directory");
                    System.exit(2);
                }
            }

            if (cmd.hasOption(MEMORY_DB))
            {
                cArgs.setMemoryDatabaseInitScript(cmd.getOptionValue(MEMORY_DB));
            }

            if (cmd.hasOption(DEBUG_CONSOLE))
            {
                cArgs.setStartDebugConsole(true);
            }
        }
        catch (ParseException pExc)
        {
            System.err.println("Command line parse error: " + pExc.getMessage());
            System.exit(1);
        }

        return cArgs;
    }

    private LinStorArgumentParser()
    {
    }
}
