package com.linbit.linstor.debug;

import com.linbit.AutoIndent;
import com.linbit.ImplementationError;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.transaction.manager.TransactionMgrGenerator;
import com.linbit.linstor.transaction.manager.TransactionMgrUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.text.ParseException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class DebugConsoleImpl implements DebugConsole
{
    public static final String CONSOLE_PROMPT = "Command ==> ";

    private static final String HELP_COMMAND = "Help";

    private boolean exitFlag = false;

    private final AccessContext debugCtx;
    private final ErrorReporter errorReporter;
    private final Map<String, CommonDebugCmd> commandMap;
    private final LinStorScope debugScope;
    private final TransactionMgrGenerator transactionMgrGenerator;

    private Map<String, String> parameters;
    private Set<String> unknownParameters;

    enum ParamParserState
    {
        SKIP_COMMAND,
        SPACE,
        OPTIONAL_SPACE,
        READ_KEY,
        READ_VALUE,
        ESCAPE
    }

    public DebugConsoleImpl(
        AccessContext debugCtxRef,
        ErrorReporter errorReporterRef,
        LinStorScope debugScopeRef,
        TransactionMgrGenerator transactionMgrGeneratorRef,
        Set<CommonDebugCmd> debugCommands
    )
    {
        debugCtx = debugCtxRef;
        errorReporter = errorReporterRef;
        debugScope = debugScopeRef;
        transactionMgrGenerator = transactionMgrGeneratorRef;
        commandMap = new TreeMap<>();

        for (CommonDebugCmd command : debugCommands)
        {
            for (String name : command.getCmdNames())
            {
                commandMap.put(name.toUpperCase(), command);
            }
        }

        parameters = new TreeMap<>();
        unknownParameters = new TreeSet<>();
    }

    @Override
    public void stdStreamsConsole(
        final String consolePrompt
    )
    {
        streamsConsole(consolePrompt, System.in, System.out, System.err, true, true);
    }

    @Override
    public void streamsConsole(
        final String consolePrompt,
        final InputStream debugIn,
        final PrintStream debugOut,
        final PrintStream debugErr,
        final boolean prompt,
        final boolean setupScope
    )
    {
        try
        {

            BufferedReader cmdIn = new BufferedReader(
                new InputStreamReader(debugIn)
            );

            String inputLine;
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
                    processCommandLine(debugOut, debugErr, inputLine, setupScope);
                }
                else
                {
                    // End of command input stream, exit console
                    break;
                }
            }
            debugOut.flush();
            debugErr.flush();
        }
        catch (IOException ioExc)
        {
            String reportId = errorReporter.reportError(ioExc);
            if (reportId != null)
            {
                debugErr.printf(
                    "DebugConsole: An unhandled I/O exception was encountered while processing the debug command.\n" +
                    "The report ID of the error report is %s\n",
                    reportId
                );
                String excMsg = ioExc.getMessage();
                if (excMsg != null)
                {
                    debugErr.printf(
                        "The error description returned by the runtime environment or operating system is:\n" +
                        "%s\n",
                        excMsg
                    );
                }
                debugErr.flush();
            }
        }
        catch (Throwable error)
        {
            String reportId = errorReporter.reportError(error);
            if (reportId != null)
            {
                debugErr.printf(
                    "DebugConsole: An unhandled error was encountered while processing the debug command:\n" +
                    "The error type is: %s\n" +
                    "The report ID of the error report is: %s\n",
                    error.getClass().getCanonicalName(), reportId
                );
                String excMsg = error.getMessage();
                if (excMsg != null)
                {
                    debugErr.printf(
                        "The error description returned by the runtime environment or operating system is:\n" +
                        "%s\n",
                        excMsg
                    );
                }
                debugErr.flush();
            }
        }
    }

    @Override
    public void processCommandLine(
        final PrintStream debugOut,
        final PrintStream debugErr,
        final String inputLine,
        final boolean setupScope
    )
    {
        String commandLine = inputLine.trim();
        if (!inputLine.isEmpty())
        {
            if (inputLine.startsWith("?"))
            {
                findCommands(
                    debugOut,
                    debugErr,
                    inputLine.substring(1, inputLine.length()).trim()
                );
            }
            else
            if (isHelpCommand(inputLine))
            {
                helpCommand(
                    debugOut,
                    debugErr,
                    inputLine.substring(HELP_COMMAND.length(), inputLine.length()).trim()
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

                    processCommand(debugOut, debugErr, command, setupScope);
                }
                catch (ParseException parseExc)
                {
                    debugErr.println(parseExc.getMessage());
                }
                finally
                {
                    parameters.clear();
                    unknownParameters.clear();
                }
            }
        }
    }

    private void processCommand(
        final PrintStream debugOut,
        final PrintStream debugErr,
        final String command,
        final boolean setupScope
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

            if (parameters.size() > 0 && !debugCmd.acceptsUndeclaredParameters())
            {
                Map<String, String> paramDescr = debugCmd.getParametersDescription();
                if (paramDescr != null)
                {
                    for (String paramKey : parameters.keySet())
                    {
                        if (!paramDescr.containsKey(paramKey))
                        {
                            unknownParameters.add(paramKey);
                        }
                    }
                }
                else
                {
                    unknownParameters.addAll(parameters.keySet());
                }
            }

            if (unknownParameters.isEmpty())
            {
                String cmdInfo = debugCmd.getCmdInfo();
                debugOut.printf("\u001b[1;37m%-20s\u001b[0m %s%n", displayCmdName, cmdInfo);

                try
                {
                    boolean doScope = setupScope && debugCmd.requiresScope();
                    // java will skip this autocloseable if it is null, no NPE
                    try (LinStorScope.ScopeAutoCloseable close = doScope ? debugScope.enter() : null)
                    {
                        if (doScope)
                        {
                            try
                            {
                                TransactionMgr transMgr = transactionMgrGenerator.startTransaction();
                                TransactionMgrUtil.seedTransactionMgr(debugScope, transMgr);
                            }
                            catch (Exception exc)
                            {
                                String reportId = errorReporter.reportError(exc);
                                debugErr.printf(
                                    "DebugConsole: An unhandled exception was encountered when attempting to start " +
                                        "the transaction.\n" +
                                        "Some commands may not work as a result of this problem.\n" +
                                    "The report ID of the error report is %s\n",
                                    reportId
                                );
                            }
                        }
                        debugCmd.execute(debugOut, debugErr, debugCtx, parameters);
                    }
                    catch (Exception exc)
                    {
                        String reportId = errorReporter.reportError(exc);
                        debugErr.printf(
                            "DebugConsole: An unhandled exception was encountered when attempting to enter or exit " +
                                "the injector scope (" + LinStorScope.class.getSimpleName() + ").\n" +
                                "Some commands may not work as a result of this problem.\n" +
                                "The report ID of the error report is %s\n",
                            reportId
                        );
                    }
                }
                catch (Exception exc)
                {
                    String reportId = errorReporter.reportError(exc);
                    if (reportId != null)
                    {
                        debugErr.printf(
                            "DebugConsole: An unhandled exception was encountered while processing " +
                            "the debug command:\n" +
                            "The exception type is: %s\n" +
                            "The report ID of the error report is: %s\n",
                            exc.getClass().getCanonicalName(), reportId
                        );
                        String excMsg = exc.getMessage();
                        if (excMsg != null)
                        {
                            debugErr.printf(
                                "The error description returned by the runtime environment or operating system is:\n" +
                                "%s\n",
                                excMsg
                            );
                        }
                        debugErr.flush();
                    }
                }
            }
            else
            {
                StringBuilder errorMsg = new StringBuilder();
                errorMsg.append(
                    String.format(
                        "Error:\n" +
                        "    The '%s' command does not support the following parameters:\n",
                        displayCmdName
                    )
                );
                for (String paramKey : unknownParameters)
                {
                    errorMsg.append("        ");
                    errorMsg.append(paramKey);
                    errorMsg.append("\n");
                }
                errorMsg.append(
                    "Correction:\n" +
                    "    Remove all unsupported parameters from the command line\n"
                );
                debugErr.print(errorMsg.toString());
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

    public void helpCommand(
        final PrintStream debugOut,
        final PrintStream debugErr,
        final String cmdName
    )
    {
        debugOut.printf("\u001b[1;37m%-20s\u001b[0m %s\n", "Help", "Help for command usage");
        debugOut.println();

        // TODO: Help for 'Find commands', '?' command
        if (cmdName.isEmpty() || cmdName.equalsIgnoreCase(HELP_COMMAND))
        {
            helpForHelp(debugOut, debugErr);
        }
        else
        {
            String uCaseCmdName = cmdName.toUpperCase();
            CommonDebugCmd debugCmd = commandMap.get(uCaseCmdName);
            if (debugCmd != null)
            {
                helpForCommand(debugCmd, debugOut, debugErr, cmdName, uCaseCmdName);
            }
            else
            {
                // Check for an attempt to enter multiple command names at a time
                if (cmdName.indexOf(' ') != -1 || cmdName.indexOf('\t') != -1)
                {
                    debugErr.print(
                        "Error:\n" +
                        "    No command that matches the command name specified in the command line was found.\n" +
                        "Cause:\n" +
                        "    The command line appears to contain multiple parameters.\n" +
                        "Correction:\n" +
                        "    Reenter the HELP command line in the correct format.\n" +
                        "    The following format is expected:\n" +
                        "        HELP commandname\n"
                    );
                }
                else
                {
                    debugErr.print(
                        "Error:\n" +
                        "    No command that matches the command name specified in the command line was found.\n" +
                        "Cause:\n" +
                        "    Common causes for this error are:\n" +
                        "        * The command name was mistyped\n" +
                        "        * A command with the specified name does not exist\n" +
                        "        * The command with the specified name in currently unavailable, because it\n" +
                        "          was not loaded by the system\n" +
                        "Correction:\n" +
                        "    Check whether the command name specified on the command line is correct.\n"
                    );
                }
            }
        }
    }

    private void helpForHelp(
        final PrintStream debugOut,
        final PrintStream debugErr
    )
    {
        debugOut.println(
            "To request help for a debug console command, enter the HELP command in\n" +
            "the following format:\n\n" +
            "    HELP commandname\n\n" +
            "Substitute the word 'commandname' with the name of the command for which\n" +
            "help information should be displayed.\n\n" +
            "Enter ? to display a list of all available commands.\n" +
            "Enter ? followed by a regular expression to apply as a filter to display a\n" +
            "list of those commands that have a name that matches the filter.\n\n"
        );
    }

    private void helpForCommand(
        final CommonDebugCmd debugCmd,
        final PrintStream debugOut,
        final PrintStream debugErr,
        final String cmdName,
        final String uCaseCmdName
    )
    {
        // Output the name of the command for which help was requested
        debugOut.println("\u001b[1;37mHelp for command:\u001b[0m");
        String cmdInfo = debugCmd.getCmdInfo();
        debugOut.printf("    %-20s %s%n", debugCmd.getDisplayName(uCaseCmdName), cmdInfo);
        debugOut.println();

        // Output any additional alias names that invoke the same command
        {
            Set<String> cmdNameList = debugCmd.getCmdNames();
            StringBuilder aliases = new StringBuilder();
            for (String name : cmdNameList)
            {
                if (!uCaseCmdName.equalsIgnoreCase(name))
                {
                    aliases.append(String.format("    %s\n", name));
                }
            }
            if (aliases.length() > 0)
            {
                debugOut.println("\u001b[1;37mCommand alias names:\u001b[0m");
                debugOut.print(aliases.toString());
                debugOut.println();
            }
        }

        // Output the command's description
        String cmdDescr = debugCmd.getCmdDescription();
        debugOut.println("\u001b[1;37mDescription:\u001b[0m");
        AutoIndent.printWithIndent(debugOut, AutoIndent.DEFAULT_INDENTATION, cmdDescr);
        debugOut.println();

        // Output command parameter descriptions
        {
            Map<String, String> paramsDescr = debugCmd.getParametersDescription();
            if (paramsDescr != null)
            {
                if (paramsDescr.size() > 0)
                {
                    debugOut.println("\u001b[1;37mSupported parameters:\u001b[0m");
                    for (Map.Entry<String, String> paramEntry : paramsDescr.entrySet())
                    {
                        debugOut.printf("    %s\n", paramEntry.getKey());
                        AutoIndent.printWithIndent(
                            debugOut,
                            2 * AutoIndent.DEFAULT_INDENTATION,
                            paramEntry.getValue()
                        );
                    }
                    debugOut.println();
                }
            }
        }

        // Output help for any additional, undeclared parameters
        {
            if (debugCmd.acceptsUndeclaredParameters())
            {
                String undeclParamsDescr = debugCmd.getUndeclaredParametersDescription();
                if (undeclParamsDescr == null)
                {
                    debugOut.print(
                        "This command accepts additional parameters that are not declared in the\n" +
                        "list of supported parameters. The command does not provide additional\n" +
                        "information about those parameters.\n"
                    );
                    debugOut.println();
                }
                else
                {
                    debugOut.print(
                        "This command accepts additional parameters that are not declared in the\n" +
                        "list of supported parameters. The following description is provided by\n" +
                        "the command for the use of such additional parameters:\n"
                    );
                    AutoIndent.printWithIndent(debugOut, AutoIndent.DEFAULT_INDENTATION, undeclParamsDescr);
                    debugOut.println();
                }
            }
        }
    }

    public void findCommands(
        final PrintStream debugOut,
        final PrintStream debugErr,
        final String cmdPattern
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
                    if (cmdMatcher.find())
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
                cmdNameList.append(
                    String.format(
                        "    %-20s %s%n",
                        debugCmd.getDisplayName(cmdName),
                        cmdInfo
                    )
                );
            }

            // Write results
            if (cmdNameList.length() > 0)
            {
                if (cmdPattern.isEmpty())
                {
                    debugOut.println("Available commands:");
                }
                else
                {
                    debugOut.println("Filter:");
                    debugOut.printf("    %s\n", cmdPattern);
                    debugOut.println("Matching commands:");
                }
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

    @Override
    public void exitConsole()
    {
        exitFlag = true;
    }

    private String parseCommandName(final char[] commandChars)
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

    private void parseCommandParameters(final char[] commandChars)
        throws ParseException
    {
        int keyOffset = 0;
        int valueOffset = 0;
        int length = 0;

        ParamParserState state = ParamParserState.SKIP_COMMAND;
        String key = null;
        String value = null;
        StringBuilder valueCache = new StringBuilder();
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
                        valueCache.append(commandChars, valueOffset, length);
                        value = valueCache.toString();
                        valueCache.setLength(0);
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
                        // Cache the value up to before the backslash
                        length = idx - valueOffset;
                        valueCache.append(commandChars, valueOffset, length);
                        valueOffset = idx + 1;
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

    private boolean isHelpCommand(final String inputLine)
    {
        boolean result = false;
        int matchLength = HELP_COMMAND.length();
        if (inputLine.length() >= matchLength)
        {
            String matchSection = inputLine.substring(0, matchLength).toUpperCase();
            String matchCmd = HELP_COMMAND.toUpperCase();
            if (matchCmd.equals(matchSection))
            {
                if (inputLine.length() > matchLength)
                {
                    char expectedSpace = inputLine.charAt(matchLength);
                    if (expectedSpace == ' ' || expectedSpace == '\t')
                    {
                        result = true;
                    }
                }
                else
                {
                    result = true;
                }
            }
        }
        return result;
    }

    private void errorInvalidKey(int pos) throws ParseException
    {
        throw new ParseException(
            String.format(
                "Error:\n" +
                "    The command line is not valid. An invalid parameter key was encountered at position %d.\n" +
                "Correction:\n" +
                "    Check whether the format of the command parameter key at the\n" +
                "    specified position is correct.\n" +
                "    The general format of a command parameter is:\n" +
                "        PARAMETERKEY(parametervalue)\n",
                pos
            ),
            pos
        );
    }

    private void errorParser(int pos) throws ParseException
    {
        throw new ParseException(
            String.format(
                "Error:\n" +
                "    The command line is not valid. The parser encountered an error at position %d.\n" +
                "Cause:\n" +
                "    This error is commonly cause by entering an invalid character or unbalanced parenthesis.\n" +
                "Correction:\n" +
                "    Make sure that values for command parameters are correctly enclosed in parenthesis\n" +
                "    and that no invalid characters are present in the command line.",
                pos
            ),
            pos
        );
    }

    private void errorIncompleteLine(int pos) throws ParseException
    {
        throw new ParseException(
            "Error:\n" +
            "    The command line is not valid. The input line appears to have been truncated.\n" +
            "Cause:\n" +
            "    This error is commonly caused by entering a command parameter without a value.\n" +
            "Correction:\n" +
            "    Reenter the command using the correct format for command parameters.\n" +
            "    The general format of a command parameter is:\n" +
            "        PARAMETERKEY(parametervalue)\n" +
            "Error details:\n" +
            "    The parameter key is not case sensitive.",
            pos
        );
    }
}
