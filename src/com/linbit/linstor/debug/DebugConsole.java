package com.linbit.linstor.debug;

import java.io.InputStream;
import java.io.PrintStream;
import java.util.Map;

/**
 * Interface for the Controller's and Satellite's debug consoles
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface DebugConsole
{
    void stdStreamsConsole(
        String consolePrompt
    );

    void streamsConsole(
        String consolePrompt,
        InputStream debugIn,
        PrintStream debugOut,
        PrintStream debugErr,
        boolean prompt
    );

    void processCommandLine(
        PrintStream debugOut,
        PrintStream debugErr,
        String commandLine
    );

    void processCommand(
        PrintStream debugOut,
        PrintStream debugErr,
        String commandUpperCase
    );

    void loadCommand(
        PrintStream debugOut,
        PrintStream debugErr,
        String cmdClassName
    );

    void exitConsole();
}
