package com.linbit;

/**
 * Allocates unoccupied numbers
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class NumberAlloc
{
    /**
     * Finds the index in the occupied where all numbers at equal or
     * greater indexes have a value equal to or greater than firstNumber
     *
     * @param occupied
     * @param firstNumber The least number allowed at any index equal to or
     *     greater than the requested index
     * @return Index in occupied where all numbers at equal or greater indexes
     *     have a value equal to or greater than firstNumber
     */
    public static int getStartIndex(int[] occupied, int firstNumber)
    {
        int index = findInsertIndex(occupied, firstNumber);
        if (index < 0)
        {
            index = (index + 1) * -1;
        }
        return index;
    }

    /**
     * Finds the index in the occupied where all numbers at lesser
     * indexes have a value equal to or less than lastNumber
     *
     * @param occupied Array of unique numbers sorted in ascending order
     * @param lastNumber The greatest number allowed at any index less than
     *     the requested index
     * @return Index in occupied where all numbers at lesser indexes have a value
     *     equal to or less than lastNumber
     */
    public static int getEndIndex(int[] occupied, int lastNumber)
    {
        int index = findInsertIndex(occupied, lastNumber);
        if (index < 0)
        {
            index = (index + 1) * -1;
        }
        else
        {
            ++index;
        }
        return index;
    }

    /**
     * Searches for the lowest-value unoccupied number in an array of occupied numbers
     *
     * @param occupied Array of unique numbers sorted in ascending order
     * @param firstNumber Begin of number range to search (inclusive).
     *     This is the first number expected at index fromIndex in occupied
     * @param lastNumber End of number range to search (inclusive).
     * @return A free (unoccupied) number within the specified range
     * @throws ExhaustedPoolException If all numbers in the specified range
     *     are occupied and no free number could be allocated
     */
    public static int getFreeNumber(
        int[] occupied,
        int firstNumber,
        int lastNumber
    )
        throws ExhaustedPoolException
    {
        int fromIndex = getStartIndex(occupied, firstNumber);
        int toIndex = getEndIndex(occupied, lastNumber);
        return getFreeNumber(occupied, firstNumber, lastNumber, fromIndex, toIndex);
    }

    /**
     * Searches for the lowest-value unoccupied number within a specified range in
     * an array of occupied numbers
     *
     * @param occupied Array of unique numbers sorted in ascending order
     * @param firstNumber Begin of number range to search (inclusive).
     *     This is the first number expected at index fromIndex in occupied
     * @param lastNumber End of number range to search (inclusive).
     * @param fromIndex Begin of the range to search in occupied
     * @param toIndex End of the range to search in occupied
     * @return A free (unoccupied) number within the specified range
     * @throws ExhaustedPoolException If all numbers in the specified range
     *     are occupied and no free number could be allocated
     */
    public static int getFreeNumber(
        int[] occupied,
        int firstNumber,
        int lastNumber,
        int fromIndex,
        int toIndex
    )
        throws ExhaustedPoolException
    {
        int result = -1;
        if (toIndex - fromIndex == 0)
        {
            // Empty list of occupied numbers, so the first number is free
            result = firstNumber;
        }
        else
        {
            // Find the index of a gap in the occupied numbers
            int gapIdx = getNumberGapIndex(occupied, firstNumber, lastNumber, fromIndex, toIndex);
            if (gapIdx == fromIndex)
            {
                // Gap between the least allowed number (firstNr) and the
                // first element's value
                result = firstNumber;
            }
            else
            if (gapIdx > fromIndex)
            {
                // Gap somewhere between two numbers or between the
                // last element's value and the greatest allowed number (lastNr)
                result = occupied[gapIdx - 1] + 1;
            }
            else
            {
                // No gap anywhere
                throw new ExhaustedPoolException(
                    String.format(
                        "All numbers in the range [%d - %d] are occupied\n",
                        firstNumber, lastNumber
                    )
                );
            }
        }
        return result;
    }

    /**
     * Searches an array of occupied numbers for a gap between two numbers in the
     * number range [firstNumber - lastNumber].
     *
     * @param occupied Array of unique numbers sorted in ascending order
     * @param firstNumber Begin of number range to search (inclusive).
     *     This is the first number expected at index fromIndex in occupied
     * @param lastNumber End of number range to search (inclusive).
     * @return Index of the first gap between two numbers, or -1 if there are no gaps
     */
    public static int getNumberGapIndex(int[] occupied, int firstNumber, int lastNumber)
    {
        return NumberAlloc.getNumberGapIndex(occupied, firstNumber, lastNumber, 0, occupied.length);
    }

    /**
     * Searches the range [fromIndex - (toIndex - 1)] for a gap between two numbers in the
     * number range [firstNumber - lastNumber].
     *
     * @param occupied Array of unique numbers sorted in ascending order
     * @param firstNumber Begin of number range to search (inclusive).
     *     This is the first number expected at index fromIndex in occupied
     * @param lastNumber End of number range to search (inclusive).
     * @param fromIndex Begin of the range to search in occupied
     * @param toIndex End of the range to search in occupied
     * @return Index of the first gap between two numbers, or -1 if there are no gaps
     */
    public static int getNumberGapIndex(
        int[] occupied,
        int firstNumber,
        int lastNumber,
        int fromIndex,
        int toIndex
    )
    {
        if (fromIndex < 0 || toIndex < fromIndex || toIndex > occupied.length)
        {
            throw new IllegalArgumentException();
        }

        int startIndex  = fromIndex;
        int endIndex    = toIndex;
        int resultIndex = -1;

        while (startIndex < endIndex)
        {
            int width = endIndex - startIndex;
            int midIndex = startIndex + (width >>> 1);
            if (occupied[midIndex] == firstNumber + (midIndex - fromIndex))
            {
                // No gap in the lower part of the current region
                // Isolate higher part of the region
                startIndex = midIndex + 1;
            }
            else
            {
                // Gap somewhere in the lower part of the region
                // Isolate lower part of the region
                endIndex = midIndex;
                resultIndex = midIndex;
            }
        }
        // Check for a gap between the greatest occupied number
        // and the greatest allowed number
        if (resultIndex == -1)
        {
            if (toIndex > fromIndex)
            {
                if (occupied[toIndex - 1] < lastNumber)
                {
                    resultIndex = toIndex;
                }
            }
        }
        return resultIndex;
    }

    /**
     * Searches the specified array of ints for the specified value using the
     * binary search algorithm. The array must be sorted (as by the sort(int[]) method)
     * prior to making this call. If it is not sorted, the results are undefined.
     * If the array contains multiple elements with the specified value, there is no
     * guarantee which one will be found.
     *
     * This method is functionally equivalent to Arrays.binarySearch(int[] a, int key)
     * from the Java class library, but is guaranteed to work for arrays up to
     * Integer.MAX_VALUE size (the maximum size of any array in Java).
     * (The Java class library's implementation may fail to work on very large arrays
     *  due to integer overflow)
     *
     * @param numbers An array of numbers sorted in ascending order
     * @param value The value to find, or to find an insertion point for
     * @return index of the search key, if it is contained in the array;
     *     otherwise, (-(insertion point) - 1). The insertion point is defined as the point
     *     at which the key would be inserted into the array: the index of the first element
     *     greater than the key, or a.length if all elements in the array are less than the
     *     specified key. Note that this guarantees that the return value will be &gt;= 0 if
     *     and only if the key is found.
     */
    public static final int findInsertIndex(
        int[] numbers,
        int value
    )
    {
        return findInsertIndex(numbers, value, 0, numbers.length);
    }

    /**
     * Searches the specified array of ints for the specified value using the
     * binary search algorithm. The array must be sorted (as by the sort(int[]) method)
     * prior to making this call. If it is not sorted, the results are undefined.
     * If the array contains multiple elements with the specified value, there is no
     * guarantee which one will be found.
     *
     * This method is functionally equivalent to Arrays.binarySearch(int[] a, int key)
     * from the Java class library, but is guaranteed to work for arrays up to
     * Integer.MAX_VALUE size (the maximum size of any array in Java).
     * (The Java class library's implementation may fail to work on very large arrays
     *  due to integer overflow)
     *
     * @param numbers An array of numbers sorted in ascending order
     * @param value The value to find, or to find an insertion point for
     * @param fromIndex Start index in the array (inclusive)
     * @param toIndex End index in the array (exclusive)
     * @return index of the search key, if it is contained in the array;
     *     otherwise, (-(insertion point) - 1). The insertion point is defined as the point
     *     at which the key would be inserted into the array: the index of the first element
     *     greater than the key, or a.length if all elements in the array are less than the
     *     specified key. Note that this guarantees that the return value will be &gt;= 0 if
     *     and only if the key is found.
     */
    public static final int findInsertIndex(
        int[] numbers,
        int value,
        int fromIndex,
        int toIndex
    )
    {
        if (fromIndex < 0 || toIndex < fromIndex || toIndex > numbers.length)
        {
            throw new IllegalArgumentException();
        }

        int startIndex = fromIndex;
        int endIndex   = toIndex;
        int index      = 0;

        boolean found = false;
        while (startIndex < endIndex)
        {
            int width = endIndex - startIndex;
            index = startIndex + (width >>> 1);
            if (value < numbers[index])
            {
                endIndex = index;
            }
            else
            if (value > numbers[index])
            {
                ++index;
                startIndex = index;
            }
            else
            {
                found = true;
                break;
            }
        }
        if (!found)
        {
            index = -index - 1;
        }
        return index;
    }

    private NumberAlloc()
    {
    }
}
