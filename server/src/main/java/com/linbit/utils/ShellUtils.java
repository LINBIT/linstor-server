package com.linbit.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
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
}
