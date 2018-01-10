package com.linbit.utils;

import java.util.Collection;
import java.util.Iterator;

/**
 *
 * @author rpeinthor
 */
public class StringUtils {
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
            sb.append(iter.next().toString());
        while (iter.hasNext()) {
            sb.append(delim);
            sb.append(iter.next().toString());
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
}
