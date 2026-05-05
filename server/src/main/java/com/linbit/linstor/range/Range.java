package com.linbit.linstor.range;

import com.linbit.linstor.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

public record Range(int from, int to) implements Comparable<Range>
{
    public Range
    {
        if (from > to)
        {
            int tmp = from;
            from = to;
            to = tmp;
        }
    }

    public static List<Range> parseList(@Nullable String strRef)
    {
        return parseList(strRef, true);
    }

    public static List<Range> parseList(@Nullable String strRef, boolean mergeAfterParseRef)
    {
        List<Range> ret = new ArrayList<>();
        if (strRef != null)
        {
            for (final String part : strRef.split(",", -1))
            {
                String fromStr;
                @Nullable String toStr = null;
                final String trimmedPart = part.trim();
                int idx = trimmedPart.indexOf("-");
                switch (idx)
                {
                    case -1 -> { // no "-" at all. Just a single (positive) number
                        fromStr = trimmedPart;
                    }
                    case 0 -> { // "-" at the very beginning, i.e. the first number is negative
                        int idx2 = trimmedPart.indexOf("-", 1);
                        if (idx2 != -1)
                        {
                            fromStr = trimmedPart.substring(0, idx2);
                            toStr = trimmedPart.substring(idx2 + 1);
                        }
                        else
                        {
                            fromStr = trimmedPart;
                        }
                    }
                    default -> { // "-" exists but not in the beginning. normal range
                        fromStr = trimmedPart.substring(0, idx);
                        toStr = trimmedPart.substring(idx + 1);
                    }
                }
                if (!fromStr.isBlank())
                {
                    try
                    {
                        int fromInt = Integer.parseInt(fromStr.trim());
                        int toInt;
                        if (toStr != null)
                        {
                            toInt = Integer.parseInt(toStr.trim());
                        }
                        else
                        {
                            toInt = fromInt;
                        }
                        ret.add(new Range(fromInt, toInt));
                    }
                    catch (NumberFormatException nfe)
                    {
                        throw new IllegalArgumentException(
                            "could not parse from '" + fromStr + "'" + (toStr == null ? "" : ", to: '" + toStr + "'")
                        );
                    }
                }
            }
            if (mergeAfterParseRef)
            {
                ret = RangeUtils.merge(ret);
            }
        }
        return ret;
    }

    /**
     * <p>Returns true if {@code this} and {@code other} either intersect (see {@link #intersects(Range)} )
     * or if the two ranges at least touch</p>
     * <p>Examples:
     * <ul>
     *  <li>{@code <1,10>.touches(<10,20>) == true} since the ranges <i>intersect</i>.</li>
     *  <li>{@code <1,10>.touches(<11,20>) == true} although the ranges do not <i>intersect</i>, the end of
     *  {@code this} (10) "touches" the beginning of {@code other} (11).</li>
     *  <li>{@code <1,10>.touches(<12,20>) == false} since there is a gap between the two ranges</li>
     * </ul>
     */
    public boolean touches(Range other)
    {
        // cast to long to prevent issues with (Integer.MAX_VALUE + 1)
        return from <= (long) other.to + 1 && other.from <= (long) to + 1;
    }

    /**
     * <p>Returns true iff {@code this} and {@code other} both cover at least one number.</p>
     * <p>Examples:
     * <ul>
     *  <li>{@code <1,10>.intersects(<10,20>) == true} since both cover number 10.</li>
     *  <li>{@code <1,10>.intersects(<11,20>) == false} since no number is covered by both</li>
     * </ul>
     */
    public boolean intersects(Range other)
    {
        return from <= other.to && other.from <= to;
    }

    @Override
    public int compareTo(Range oRef)
    {
        int cmp = Integer.compare(from, oRef.from);
        if (cmp == 0)
        {
            cmp = Integer.compare(to, oRef.to);
        }
        return cmp;
    }

    public Range createMerged(Range otherRef)
    {
        return new Range(Math.min(from, otherRef.from), Math.max(to, otherRef.to));
    }

    public boolean contains(int nrRef)
    {
        return from <= nrRef && nrRef <= to;
    }
}
