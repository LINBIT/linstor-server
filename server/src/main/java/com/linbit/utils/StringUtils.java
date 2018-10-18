package com.linbit.utils;

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.StringJoiner;

/**
 *
 * @author rpeinthor
 */
public class StringUtils
{
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

    public static String join(String delimiter, String... array)
    {
        StringBuilder sb = new StringBuilder();
        for (String element : array)
        {
            sb.append(element).append(delimiter);
        }
        sb.setLength(sb.length() - delimiter.length());
        return sb.toString();
    }

    public static String firstLetterCaps(String string)
    {
        return string.substring(0, 1).toUpperCase() + string.substring(1);
    }

    public static boolean isEmpty(String string)
    {
        return string == null || string.isEmpty();
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
}
