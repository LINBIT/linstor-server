package com.linbit.linstor.range;

import com.linbit.linstor.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

public class RangeUtils
{
    private RangeUtils()
    {
    }

    /**
     * <p>Subtracts the {@code rangesToSubtract} list from {@code rangesList}. This is not a simple
     * {@code list.removeAll(otherList)} operation, but a range-aware one.</p>
     * <p>Example: <br/>
     * {@code rangesList}: {@code [(10,100), (200,300)]}<br/>
     * {@code rangesToSubtract}: {@code [(80,90), (290,400)]}<br/>
     * result: {@code [(10,79),(91,100),(200,289)]}
     */
    public static List<Range> subtract(List<Range> rangesList, List<Range> rangesToSubtract)
    {
        List<Range> ret = new ArrayList<>();

        if (!rangesList.isEmpty())
        {
            if (rangesToSubtract.isEmpty())
            { // nothing to subtract. Copy the list and return it
                ret.addAll(rangesList);
            }
            else
            {
                final ArrayDeque<Range> rangesToProcress = new ArrayDeque<>(rangesList);
                final int lenSubtractList = rangesToSubtract.size();
                int idxSubtr = 0;
                Range curSubtrRange = rangesToSubtract.get(0);
                while (!rangesToProcress.isEmpty())
                {
                    boolean nextSubtr = false;
                    Range curRange = rangesToProcress.removeFirst();
                    if (curRange.to() < curSubtrRange.from())
                    {
                        ret.add(curRange);
                    }
                    else if (curRange.from() > curSubtrRange.to())
                    {
                        rangesToProcress.addFirst(curRange);
                        nextSubtr = true;
                    }
                    else
                    { // overlapping
                        @Nullable Range subtrBeginning = getFrontRangeAfterSubstraction(curRange, curSubtrRange);
                        if (subtrBeginning != null)
                        {
                            // we know that curSubtrRange is strictly after subtrBeiginning, hence we can
                            // just add subtrBeginning to the result
                            ret.add(subtrBeginning);
                        }
                        @Nullable Range subtrEnd = getEndRangeAfterSubtraction(curRange, curSubtrRange);
                        if (subtrEnd != null)
                        {
                            // subtrEnd does not intersect with curSubtrRange but might still intersect with the next
                            // Range to subtract with. so we need to process it again.
                            rangesToProcress.addFirst(subtrEnd);
                            nextSubtr = true;
                        }
                    }
                    if (nextSubtr)
                    {
                        idxSubtr++;
                        if (idxSubtr < lenSubtractList)
                        {
                            curSubtrRange = rangesToSubtract.get(idxSubtr);
                        }
                        else
                        { // copy current + remaining
                            ret.addAll(rangesToProcress);
                            rangesToProcress.clear();
                        }
                    }
                }
            }
        } // else: rangesList is empty - nothing to remove from. just return the initialized empty list
        return ret;
    }

    /**
     * <p>Returns a range with the same {@link Range#from()} value as the input {@code rangeRef} iff the beginning of
     * {@code rangeRef} is not cut off.</p>
     * <p>Examples:
     * <ul>
     *  <li><pre>
     *   aaaaaa
     * -    bbbbb
     * = aaa
     * </pre></li>
     *  <li><pre>
     *   aaaaaa
     * -    bb
     * = aaa
     *  </pre>
     *  Note that the last "a" is missing in the returned {@link Range}. To get that part, use
     *  {@link #getEndRangeAfterSubtraction(Range, Range)}</li>
       <li><pre>
     *     aaaaaa
     * - bbbb
     * =           // null
     *  </pre>
     *  This example returns {@code null} since there is no "front range" left from {@code rangeRef} after the
     *  subtraction</li>
     * </ul>
     * </p>
     */
    private static @Nullable Range getFrontRangeAfterSubstraction(Range rangeRef, Range subtrRangeRef)
    {
        @Nullable Range ret = null;
        if (rangeRef.contains(subtrRangeRef.from() - 1))
        {
            ret = new Range(rangeRef.from(), subtrRangeRef.from() - 1);
        }
        return ret;
    }

    /**
     * <p>Returns a range with the same {@link Range#to()} value as the input {@code rangeRef} iff the end of
     * {@code rangeRef} is not cut off.</p>
     * <p>Examples:
     * <ul>
     *  <li><pre>
     *   aaaaaa
     * -    bbbbb
     * =            // null
     *  </pre>
     *  This example returns {@code null} since there is no "end range" left from {@code rangeRef} after the
     *  subtraction</li>
     *  <li><pre>
     *   aaaaaa
     * -    bb
     * =      a
     *  </pre>
     *  Note that the first "aaa" is missing in the returned {@link Range}. To get that part, use
     *  {@link #getFrontRangeAfterSubstraction(Range, Range)}</li>
       <li><pre>
     *     aaaaaa
     * - bbbb
     * =     aaaa
     *  </pre></li>
     * </ul>
     * </p>
     */
    private static @Nullable Range getEndRangeAfterSubtraction(Range rangeRef, Range subtrRangeRef)
    {
        @Nullable Range ret = null;
        if (rangeRef.contains(subtrRangeRef.to() + 1))
        {
            ret = new Range(subtrRangeRef.to() + 1, rangeRef.to());
        }
        return ret;
    }

    /**
     * Merges a given list of ranges. Example: [(10-15), (16-20)] will be merged into [(10-20)].
     * This method does not make any assumptions of the ordering of the list-parameter
     */
    public static List<Range> merge(@Nullable List<@Nullable Range> ranges)
    {
        List<Range> result = new ArrayList<>();
        if (ranges != null)
        {
            for (@Nullable Range range : ranges)
            {
                if (range != null)
                {
                    result = merge(result, range);
                }
            }
        }
        return result;
    }

    /**
     * <p>Creates a new ordered list of ranges including the {@code rangeToMerge}. This method
     * will also collapse / merge Ranges.</p>
     * <p>The {@code orderedRanges} are expected to be in ascending order and non-touching (see
     * {@link Range#touches(Range)}. If you  are not sure this is the case, rather use {@link #merge(List)}.</p>
     */
    public static List<Range> merge(List<Range> orderedRanges, Range rangeToMerge)
    {
        @Nullable List<Range> result = simpleMerge(orderedRanges, rangeToMerge);
        if (result == null)
        {
            result = mergeImpl(orderedRanges, rangeToMerge);
        }
        return result;
    }

    private static List<Range> mergeImpl(List<Range> orderedRanges, Range rangeToMerge)
    {
        List<Range> result = new ArrayList<>();
        @Nullable Range mergedRange = null;
        boolean copyRemaining = false;
        for (int idx = 0; idx < orderedRanges.size(); idx++)
        {
            Range range = orderedRanges.get(idx);
            if (range.touches(rangeToMerge))
            {
                if (mergedRange == null)
                {
                    mergedRange = range;
                }
                else
                {
                    mergedRange = mergedRange.createMerged(range);
                }
                mergedRange = mergedRange.createMerged(rangeToMerge);
            }
            else
            {
                if (mergedRange != null)
                {
                    result.add(mergedRange);
                    mergedRange = null;
                    copyRemaining = true;
                }
                else if (isInsertable(orderedRanges, rangeToMerge, idx))
                {
                    result.add(rangeToMerge);
                    copyRemaining = true;
                }

                result.add(range);

                if (copyRemaining && orderedRanges.size() > idx + 1)
                {
                    result.addAll(orderedRanges.subList(idx + 1, orderedRanges.size()));
                    break;
                }
            }
        }
        if (mergedRange != null)
        {
            result.add(mergedRange);
        }
        return result;
    }

    /**
     * Checks if {@code rangeToMerge} is simply insertable at the current position. This method
     * assumes that the next range does not touch {@code rangeToMerge}.
     */
    private static boolean isInsertable(List<Range> orderedRanges, Range rangeToMerge, int idx)
    {
        boolean ret;
        if (idx == 0)
        {
            ret = false;
        }
        else
        {
            Range prev = orderedRanges.get(idx - 1);
            ret = prev.to() < rangeToMerge.from();
        }
        return ret;
    }

    /**
     * Merges {@code rangeToMergeRef} with {@code orderedRangesRef} into {@code resultRef} if one of the following
     * conditions is met:
     * <ul>
     *  <li>{@code orderedRangesRef} is empty</li>
     *  <li>{@code rangeToMergeRef} is strictly before or touches (see {@link Range#touches(Range)} the first
     *      element of {@code orderedRangesRef}</li>
     *  <li>{@code rangeToMergeRef} is strictly after or touches the last element of {@code orderedRangesRef}</li>
     * </ul>
     * @return {@code null} if the merge is more complex than these simple cases, or a fully merged list of ranges
     * otherwise.
     */
    private static @Nullable List<Range> simpleMerge(List<Range> orderedRangesRef, Range rangeToMergeRef)
    {
        @Nullable List<Range> ret = null;
        if (orderedRangesRef.isEmpty())
        {
            ret = new ArrayList<>();
            ret.add(rangeToMergeRef);
        }
        else
        {
            Range first = orderedRangesRef.get(0);
            if (rangeToMergeRef.to() <= first.from())
            {
                ret = simpleMergeFirst(orderedRangesRef, rangeToMergeRef, first);
            }
            else
            {
                Range last = orderedRangesRef.get(orderedRangesRef.size() - 1);
                if (last.to() <= rangeToMergeRef.from())
                {
                    ret = simpleMergeLast(orderedRangesRef, rangeToMergeRef, last);
                }
            }
        }
        return ret;
    }

    private static List<Range> simpleMergeFirst(List<Range> orderedRangesRef, Range rangeToMergeRef, Range first)
    {
        List<Range> ret = new ArrayList<>();
        if (rangeToMergeRef.touches(first))
        {
            ret.add(rangeToMergeRef.createMerged(first));
            ret.addAll(orderedRangesRef.subList(1, orderedRangesRef.size()));
        }
        else
        {
            ret.add(rangeToMergeRef);
            ret.addAll(orderedRangesRef);
        }
        return ret;
    }

    private static List<Range> simpleMergeLast(List<Range> orderedRangesRef, Range rangeToMergeRef, Range last)
    {
        List<Range> ret = new ArrayList<>();
        if (orderedRangesRef.size() > 1)
        {
            // exclude last for now
            ret.addAll(orderedRangesRef.subList(0, orderedRangesRef.size() - 1));
        }
        if (last.touches(rangeToMergeRef))
        {
            ret.add(last.createMerged(rangeToMergeRef));
        }
        else
        {
            ret.add(last);
            ret.add(rangeToMergeRef);
        }
        return ret;
    }

    /**
     * Returns a string that can again be parsed using {@link Range#parseList(String)}. The given list of
     * {@link Range}s is <b>not</b> modified (i.e. simplified) but rendered as given, including duplicates
     * and overlapping ranges if necessary. This is where this method differs from {@link Range#parseList(String)}
     * which does simplify before returning.
     *
     * <p>In other words, the result of <code>Range.parseList(render(input)).equals(input)</code> might be false but
     * the returned {@link Range}s will cover the same numbers as the input.</p>
     * <p>Examples:
     * <pre>
     *   render([(1-3),(3-5)]) => "1-3,3-5"
     *   Range.parseList("1-3,3-5") => [(1-5)]
     * </pre></p>
     */
    public static String render(@Nullable List<Range> ranges)
    {
        String ret;
        if (ranges == null || ranges.isEmpty())
        {
            ret = "";
        }
        else
        {
            StringBuilder sb = new StringBuilder();
            for (Range range : ranges)
            {
                int from = range.from();
                int to = range.to();
                if (from == to)
                {
                    sb.append(from);
                }
                else
                {
                    sb.append(from).append("-").append(to);
                }
                sb.append(",");
            }
            sb.setLength(sb.length() - 1);
            ret = sb.toString();
        }
        return ret;
    }
}
