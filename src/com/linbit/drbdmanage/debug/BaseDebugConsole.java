package com.linbit.drbdmanage.debug;

import com.linbit.ImplementationError;
import com.linbit.drbdmanage.CoreServices;
import com.linbit.drbdmanage.security.AccessContext;

import java.io.*;
import java.text.ParseException;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Base methods for the Controller's and Satellite's debug consoles
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public abstract class BaseDebugConsole implements DebugConsole
{
    private boolean exitFlag = false;

    protected AccessContext debugCtx;
    protected CoreServices dbgCoreSvcs;
    protected Map<String, String> parameters;
    protected Map<String, CommonDebugCmd> commandMap;

    enum ParamParserState
    {
        SKIP_COMMAND,
        SPACE,
        OPTIONAL_SPACE,
        READ_KEY,
        READ_VALUE,
        ESCAPE
    };

    protected BaseDebugConsole(AccessContext debugCtxRef, CoreServices coreSvcsRef)
    {
        debugCtx = debugCtxRef;
        dbgCoreSvcs = coreSvcsRef;
        parameters = new TreeMap<>();
        commandMap = new TreeMap<>();
    }

    @Override
    public void stdStreamsConsole(
        String consolePrompt
    )
    {
        streamsConsole(consolePrompt, System.in, System.out, System.err, true);
    }

    @Override
    public void streamsConsole(
        String consolePrompt,
        InputStream debugIn,
        PrintStream debugOut,
        PrintStream debugErr,
        boolean prompt
    )
    {
        try
        {
            BufferedReader cmdIn = new BufferedReader(
                new InputStreamReader(debugIn)
            );

            String inputLine;
            commandLineLoop:
            while (!exitFlag)
            {
                // Print the console prompt if required
                if (prompt)
                {
                    debugOut.print("\n");
                    debugOut.print(consolePrompt);
                    debugOut.flush();
                }

                inputLine = cmdIn.readLine();
                if (inputLine != null)
                {
                    processCommandLine(debugOut, debugErr, inputLine);
                }
                else
                {
                    // End of command input stream, exit console
                    break;
                }
            }
        }
        catch (IOException ioExc)
        {
            dbgCoreSvcs.getErrorReporter().reportError(ioExc);
        }
        catch (Throwable error)
        {
            dbgCoreSvcs.getErrorReporter().reportError(error);
        }
    }

    @Override
    public void processCommandLine(
        PrintStream debugOut,
        PrintStream debugErr,
        String inputLine
    )
    {
        String commandLine = inputLine.trim();
        if (!inputLine.isEmpty())
        {
            if (inputLine.startsWith("?"))
            {
                findCommands(
                    commandMap,
                    debugOut,
                    debugErr,
                    inputLine.substring(1, inputLine.length()).trim()
                );
            }
            else
            {
                // Parse the debug command line
                char[] commandChars = commandLine.toCharArray();
                String command = parseCommandName(commandChars);

                try
                {
                    parseCommandParameters(commandChars);
                    processCommand(debugOut, debugErr, command);
                }
                catch (ParseException parseExc)
                {
                    debugErr.println(parseExc.getMessage());
                }
                finally
                {
                    parameters.clear();
                }
            }
        }
    }

    @Override
    public void processCommand(
        PrintStream debugOut,
        PrintStream debugErr,
        String command
    )
    {
        String cmdName = command.toUpperCase();
        CommonDebugCmd debugCmd = commandMap.get(cmdName);
        if (debugCmd != null)
        {
            String displayCmdName = debugCmd.getDisplayName(cmdName);
            if (displayCmdName == null)
            {
                displayCmdName = "<unknown command>";
            }

            String cmdInfo = debugCmd.getCmdInfo();
            if (cmdInfo == null)
            {
                debugOut.printf("\u001b[1;37m%-20s\u001b[0m\n", displayCmdName);
            }
            else
            {
                debugOut.printf("\u001b[1;37m%-20s\u001b[0m %s\n", displayCmdName, cmdInfo);
            }

            try
            {
                debugCmd.execute(debugOut, debugErr, debugCtx, parameters);
            }
            catch (Exception exc)
            {
                dbgCoreSvcs.getErrorReporter().reportError(exc);
            }
        }
        else
        {
            debugErr.printf(
                "Error:\n" +
                    "    The statement '%s' is not a valid debug console command.\n" +
                    "Correction:\n" +
                    "    Enter a valid debug console command.\n" +
                    "    Use the \"?\" builtin query (without quotes) to display a list\n" +
                    "    of available commands.\n",
                command
            );
        }
    }

    public void findCommands(
        Map<String, CommonDebugCmd> commandMap,
        PrintStream debugOut,
        PrintStream debugErr,
        String cmdPattern
    )
    {
        try
        {
            debugOut.printf("\u001b[1;37m%-20s\u001b[0m %s\n", "?", "Find commands");
            Map<String, CommonDebugCmd> selectedCmds;

            if (cmdPattern.isEmpty())
            {
                // Select all commands
                selectedCmds = commandMap;
            }
            else
            {
                // Filter commands using the regular expression pattern
                selectedCmds = new TreeMap<>();
                Pattern cmdRegEx = Pattern.compile(cmdPattern, Pattern.CASE_INSENSITIVE);
                for (Map.Entry<String, CommonDebugCmd> cmdEntry : commandMap.entrySet())
                {
                    String cmdName = cmdEntry.getKey();
                    Matcher cmdMatcher = cmdRegEx.matcher(cmdName);
                    if (cmdMatcher.matches())
                    {
                        selectedCmds.put(cmdEntry.getKey(), cmdEntry.getValue());
                    }
                }
            }

            // Generate the output list
            StringBuilder cmdNameList = new StringBuilder();
            for (Map.Entry<String, CommonDebugCmd> cmdEntry : selectedCmds.entrySet())
            {
                String cmdName = cmdEntry.getKey();
                CommonDebugCmd debugCmd = cmdEntry.getValue();

                String cmdInfo = debugCmd.getCmdInfo();
                if (cmdInfo == null)
                {
                    cmdNameList.append(
                        String.format(
                            "    %-20s\n",
                            debugCmd.getDisplayName(cmdName)
                        )
                    );
                }
                else
                {
                    cmdNameList.append(
                        String.format(
                            "    %-20s %s\n",
                            debugCmd.getDisplayName(cmdName),
                            cmdInfo
                        )
                    );
                }
            }

            // Write results
            if (cmdNameList.length() > 0)
            {
                debugOut.println("Matching commands:");
                debugOut.print(cmdNameList.toString());
                debugOut.flush();
            }
            else
            {
                if (cmdPattern.isEmpty())
                {
                    debugOut.println("No commands are available.");
                }
                else
                {
                    debugOut.println("No commands matching the pattern were found.");
                }
            }
        }
        catch (PatternSyntaxException patternExc)
        {
            debugErr.println(
                "Error:\n" +
                    "    The specified regular expression pattern is not valid.\n" +
                    "    (See error details section for a more detailed description of the error)\n" +
                    "Correction:\n" +
                    "    If any text is entered following the ? sign, the text must form a valid\n" +
                    "    regular expression pattern.\n" +
                    "Error details:\n"
            );
            debugErr.println(patternExc.getMessage());
        }
    }

    public void exitConsole()
    {
        exitFlag = true;
    }

    private String parseCommandName(char[] commandChars)
    {
        int commandLength = commandChars.length;
        for (int idx = 0; idx < commandChars.length; ++idx)
        {
            if (commandChars[idx] == ' ' || commandChars[idx] == '\t')
            {
                commandLength = idx;
                break;
            }
        }
        String command = new String(commandChars, 0, commandLength);
        return command;
    }

    private void parseCommandParameters(char[] commandChars)
        throws ParseException
    {
        int keyOffset = 0;
        int valueOffset = 0;
        int length = 0;

        ParamParserState state = ParamParserState.SKIP_COMMAND;
        String key = null;
        String value = null;
        for (int idx = 0; idx < commandChars.length; ++idx)
        {
            char cc = commandChars[idx];
            switch (state)
            {
                case SKIP_COMMAND:
                    if (cc == ' ' || cc == '\t')
                    {
                        state = ParamParserState.OPTIONAL_SPACE;
                    }
                    break;
                case SPACE:
                    if (cc != ' ' && cc != '\t')
                    {
                        errorParser(idx);
                    }
                    state = ParamParserState.OPTIONAL_SPACE;
                    break;
                case OPTIONAL_SPACE:
                    if (cc == ' ' || cc == '\t')
                    {
                        break;
                    }
                    keyOffset = idx;
                    state = ParamParserState.READ_KEY;
                    // fall-through
                case READ_KEY:
                    if (cc == '(')
                    {
                        length = idx - keyOffset;
                        if (length < 1)
                        {
                            errorInvalidKey(keyOffset);
                        }
                        key = new String(commandChars, keyOffset, length).toUpperCase();
                        valueOffset = idx + 1;
                        state = ParamParserState.READ_VALUE;
                    }
                    else
                    if (!((cc >= 'a' && cc <= 'z') || (cc >= 'A' && cc <= 'Z')))
                    {
                        if (!(cc >= '0' && cc <= '9' && idx > keyOffset))
                        {
                            errorInvalidKey(keyOffset);
                        }
                    }
                    break;
                case READ_VALUE:
                    if (cc == ')')
                    {
                        length = idx - valueOffset;
                        value = new String(commandChars, valueOffset, length);
                        if (key != null)
                        {
                            parameters.put(key, value);
                        }
                        else
                        {
                            errorInvalidKey(keyOffset);
                        }
                        state = ParamParserState.OPTIONAL_SPACE;
                    }
                    else
                    if (cc == '\\')
                    {
                        // Ignore the next character's special meaning
                        state = ParamParserState.ESCAPE;
                    }
                    break;
                case ESCAPE:
                    // Only values can be escaped, so the next state is always READ_VALUE
                    state = ParamParserState.READ_VALUE;
                    break;
                default:
                    throw new ImplementationError(
                        String.format(
                            "Missing case label for enum member '%s'",
                            state.name()
                        ),
                        null
                    );
            }
        }
        if (state != ParamParserState.SKIP_COMMAND && state != ParamParserState.OPTIONAL_SPACE)
        {
            errorIncompleteLine(commandChars.length);
        }
    }

    private void errorInvalidKey(int pos) throws ParseException
    {
        throw new ParseException(
            String.format(
                "The command line is not valid. " +
                    "An invalid parameter key was encountered at position %d.",
                pos
            ),
            pos
        );
    }

    private void errorParser(int pos) throws ParseException
    {
        throw new ParseException(
            String.format(
                "The command line is not valid. " +
                    "The parser encountered an error at position %d.",
                pos
            ),
            pos
        );
    }

    private void errorIncompleteLine(int pos) throws ParseException
    {
        throw new ParseException(
            "The command line is not valid. The input line appears to have been truncated.",
            pos
        );
    }
}
