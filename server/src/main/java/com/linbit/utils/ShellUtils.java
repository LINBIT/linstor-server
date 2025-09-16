package com.linbit.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ShellUtils
{
    /**
     * This pattern matches any characters that need escaping in Unix shells.
     * This is taken from Pythons shlex.quote.
     */
    private static Pattern unsafeForShell = Pattern.compile("[^\\w@%+=:,./-]");

    private ShellUtils()
    {
    }

    public static List<String> shellSplit(final CharSequence string)
    {
        List<String> tokens = new ArrayList<>();
        boolean escaping = false;
        char quoteChar = ' ';
        boolean quoting = false;
        StringBuilder current = new StringBuilder();
        for (int index = 0; index < string.length(); index++)
        {
            char chr = string.charAt(index);
            if (escaping)
            {
                current.append(chr);
                escaping = false;
            }
            else
            if (chr == '\\' && !(quoting && quoteChar == '\''))
            {
                escaping = true;
            }
            else
            if (quoting && chr == quoteChar)
            {
                quoting = false;
            }
            else
            if (!quoting && (chr == '\'' || chr == '"'))
            {
                quoting = true;
                quoteChar = chr;
            }
            else
            if (!quoting && Character.isWhitespace(chr))
            {
                if (current.length() > 0)
                {
                    tokens.add(current.toString());
                    current = new StringBuilder();
                }
            }
            else
            {
                current.append(chr);
            }
        }
        if (current.length() > 0)
        {
            tokens.add(current.toString());
        }
        return tokens;
    }

    /**
     * Return a shell-escaped version of the string.
     *
     * @param string The string to escape.
     * @return The escaped string.
     */
    public static String shellQuote(String string)
    {
        String result;
        Matcher matcher = unsafeForShell.matcher(string);
        if (string.isEmpty())
        {
            result = "''";
        }
        else if (matcher.find())
        {
            result = "'" + string.replace("'", "'\"'\"'") + "'";
        }
        else
        {
            result = string;
        }
        return result;
    }

    /**
     * Return a shell-ready version of the string array.
     *
     * @param strings The strings to escape.
     * @return The escaped string, joined together.
     */
    public static String joinShellQuote(String... strings)
    {
        assert Arrays.stream(strings).noneMatch(Objects::isNull) :
            "joinShellQuote.strings contains null: " + StringUtils.join(Arrays.asList(strings));

        return StringUtils.join(" ", Arrays.stream(strings).map(ShellUtils::shellQuote).toArray());
    }

    /**
     * Return an argument list excluding specified arguments
     *
     * @param commandArgs List of command line arguments
     * @param excludeArgs Arguments to remove from the command line arguments
     * @return List of command line arguments excluding the specified arguments
     */
    public static List<String> excludeArguments(List<String> commandArgs, ExcludeArgsEntry... excludeArgs)
    {
        Map<String, ExcludeArgsEntry> excludeArgsMap = new TreeMap<>();
        for (ExcludeArgsEntry entry : excludeArgs)
        {
            excludeArgsMap.put(entry.name, entry);
        }

        List<String> argList = null;

        int pos = 0;
        final Iterator<String> cmdArgIter = commandArgs.iterator();
        while (cmdArgIter.hasNext())
        {
            final String cmdArg = cmdArgIter.next();
            final ExcludeArgsEntry entry = excludeArgsMap.get(cmdArg);
            if (entry != null)
            {
                // Exclude the current argument and any following arguments
                // according to the entry's skipCount

                // If no modified output list exists, construct it now
                if (argList == null)
                {
                    argList = new LinkedList<>();
                    int idx = 0;
                    for (String copyArg : commandArgs)
                    {
                        if (idx < pos)
                        {
                            argList.add(copyArg);
                        }
                        else
                        {
                            break;
                        }
                        ++idx;
                    }
                }

                for (int ctr = 0; ctr < entry.skipCount && cmdArgIter.hasNext(); ++ctr)
                {
                    cmdArgIter.next(); // Discard element
                }
            }
            else
            {
                if (argList != null)
                {
                    argList.add(cmdArg);
                }
            }

            ++pos;
        }

        // Return a modified argument list if one exists, otherwise return the original argument list
        return argList != null ? argList : commandArgs;
    }

    public static class ExcludeArgsEntry
    {
        public final String     name;
        public final int        skipCount;

        public ExcludeArgsEntry(final String argName, final int argSkipCount)
        {
            if (argName == null)
            {
                throw new IllegalArgumentException();
            }

            name = argName;
            skipCount = argSkipCount;
        }
    }
}
