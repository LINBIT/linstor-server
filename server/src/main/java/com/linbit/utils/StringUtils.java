package com.linbit.utils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author rpeinthor
 */
public class StringUtils
{
    /**
     * This pattern matches any characters that need escaping in Unix shells.
     * This is taken from Pythons shlex.quote.
     */
    private static Pattern unsafeForShell = Pattern.compile("[^\\w@%+=:,./-]");

    /**
     * Joins a collection into a string with the given delimeter.
     * @param col collection to join.
     * @param delim delimeter to use or separation.
     * @return A string concatenated with the specified delim.
     */
    public static String join(Collection<?> col, String delim)
    {
        StringBuilder sb = new StringBuilder();
        Iterator<?> iter = col.iterator();
        if (iter.hasNext())
        {
            sb.append(Objects.toString(iter.next()));
        }
        while (iter.hasNext())
        {
            sb.append(delim);
            sb.append(Objects.toString(iter.next()));
        }
        return sb.toString();
    }

    /**
     * Shorthand version to join a collection, separater is ",".
     * @param col
     * @return
     */
    public static String join(Collection<?> col)
    {
        return join(col, ",");
    }

    /**
     * Convenience method delegating the call to {@link #join(String, Object...)} in a
     * typesafe way
     *
     * @param delimiter
     * @param array
     * @return
     */
    public static String join(String delimiter, String... array)
    {
        return join(delimiter, (Object[]) array);
    }

    public static String join(String delimiter, Object... array)
    {
        StringBuilder sb = new StringBuilder();
        for (Object element : array)
        {
            sb.append(element).append(delimiter);
        }
        sb.setLength(sb.length() - delimiter.length());
        return sb.toString();
    }

    /**
     * Return a shell-escaped version of the string.
     *
     * @param string The string to escape.
     * @return The escaped string.
     */
    public static String shellQuote(@Nonnull String string)
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
    public static String joinShellQuote(@Nonnull String... strings)
    {
        return join(" ", Arrays.stream(strings).map(StringUtils::shellQuote).toArray());
    }

    public static String firstLetterCaps(String string)
    {
        return string.substring(0, 1).toUpperCase() + string.substring(1);
    }

    public static boolean isEmpty(String string)
    {
        return string == null || string.isEmpty();
    }

    public static String[] concat(String[] array, Collection<String> list)
    {
        List<String> result = new ArrayList<>(Arrays.asList(array));
        result.addAll(list);
        return result.toArray(new String[result.size()]);
    }

    public static String[] concat(String[] array1, String... array2)
    {
        String[] result = new String[array1.length + array2.length];
        System.arraycopy(array1, 0, result, 0, array1.length);
        System.arraycopy(array2, 0, result, array1.length, array2.length);
        return result;
    }

    public static String repeat(String repeatedElement, String glue, int times)
    {
        StringBuilder sb = new StringBuilder();
        if (times > 0)
        {
            sb.append(repeatedElement);
        }
        for (int idx = 1; idx < times; ++idx)
        {
            sb.append(glue).append(repeatedElement);
        }
        return sb.toString();
    }

    public static List<String> asStrList(Collection<?> collection)
    {
        List<String> ret = new ArrayList<>();
        if (collection != null)
        {
            for (Object obj : collection)
            {
                ret.add(Objects.toString(obj));
            }
        }
        return ret;
    }

    /**
     * Check if the given propValue is "true" or "yes".
     * @param propValue Linstor property string
     * @return true if value is "Yes" or "True" case-insensitive
     */
    public static boolean propTrueOrYes(@Nullable String propValue)
    {
        return propValue != null && (propValue.equalsIgnoreCase("true") || propValue.equalsIgnoreCase("yes"));
    }

    /**
     * Check if the given propValue is null or "false"
     * @param propValue Linstor property string
     * @return true if value is null or "FaLsE" case-insensitive
     */
    public static boolean propFalseOrNull(@Nullable String propValue)
    {
        return propValue == null || "false".equalsIgnoreCase(propValue);
    }

    /**
     * sorts in this order:
     * "a", "b", null
     */
    public static int compareToNullable(@Nullable String a, @Nullable String b)
    {
        int cmp;
        if (a != null)
        {
            if (b != null)
            {
                cmp = a.compareTo(b);
            }
            else
            {
                cmp = -1;
            }
        }
        else
        {
            if (b != null)
            {
                cmp = 1;
            }
            else
            {
                cmp = 0;
            }
        }
        return cmp;
    }

    public static class ConditionalStringJoiner
    {
        private final StringJoiner stringJoiner;

        public ConditionalStringJoiner(CharSequence delimiter)
        {
            stringJoiner = new StringJoiner(delimiter);
        }

        public ConditionalStringJoiner addIf(boolean condition, CharSequence charSequence)
        {
            if (condition)
            {
                stringJoiner.add(charSequence);
            }
            return this;
        }

        @Override
        public String toString()
        {
            return stringJoiner.toString();
        }
    }

    private StringUtils()
    {
    }
}
