package com.linbit.linstor.numberpool;

import com.linbit.ExhaustedPoolException;
import com.linbit.linstor.range.Range;

import java.util.List;
import java.util.Map;

public interface NumberPool
{
    /**
     * Determines whether a number is allocated or free for allocation
     *
     * @param nr the number to check for its allocation status
     * @return true if the number is allocated, false otherwise
     */
    boolean isAllocated(int nr);

    /**
     * Allocates a number
     *
     * @param nr the number to allocate
     * @return true if the number was allocated by this call, false if it had already been allocated
     */
    boolean allocate(int nr);

    /**
     * Deallocates a number
     *
     * @param nr the number to deallocate
     */
    void deallocate(int nr);

    /**
     * Allocates all specified numbers
     *
     * @param nrList array of numbers to allocate
     */
    void allocateAll(int[] nrList);

    /**
     * Deallocates all specified numbers
     *
     * @param nrList array of numbers to deallocate
     */
    void deallocateAll(int[] nrList);

    /**
     * Returns the size of this number pool. This is the total count of numbers managed by this number pool.
     *
     * @return Size of the number pool
     */
    int getSize();

    /**
     * Indicates whether the number pool is empty (has no allocated numbers)
     *
     * @return True if none of the numbers managed by the number pool are allocated, false otherwise
     */
    boolean isEmpty();

    /**
     * Returns the count of allocated numbers in this number pool
     *
     * @return Count of currently allocated numbers
     */
    int getAllocatedCount();

    /**
     * Returns the count of available (unallocated) numbers in this number pool
     *
     * @return Count of currently available (unallocated) numbers
     */
    int getAvailableCount();

    /**
     * Allocates multiple numbers and returns the allocation result for each of the numbers
     *
     * For each number in the list, allocation is attempted. The method returns a map, where
     * each number is allocated to a boolean value indicating the of the allocation attempt
     * for that number. True indicates that the number had been unallocated before calling
     * this method, and has been allocated by this call; false indicates that the number
     * had already been allocated before calling this method.
     *
     * @param nrList list of numbers to allocate
     * @return Map of number to allocation status
     */
    Map<Integer, Boolean> multiAllocate(List<Integer> nrList);

    /**
     * Finds the first unallocated number within the range rangeStart - rangeEnd (inclusively)
     *
     * @param rangeStart The lowest-value number in the allocation range
     * @param rangeEnd The highest-value number in the allocation range
     * @return the lowest-value unallocated number in the specified allocation range
     * @throws ExhaustedPoolException if all numbers within the specified allocation range are allocated
     */
    default int findUnallocated(int rangeStart, int rangeEnd) throws ExhaustedPoolException
    {
        return findUnallocated(new Range(rangeStart, rangeEnd));
    }

    /**
     * Finds the first unallocated number within the given range (inclusively)
     *
     * @param range The allocation range
     * @return the lowest-value unallocated number in the specified allocation range
     * @throws ExhaustedPoolException if all numbers within the specified allocation range are allocated
     */
    int findUnallocated(Range range) throws ExhaustedPoolException;

    /**
     * Finds the  first unallocated number within the range rangeStart - rangeEnd, starting at the
     * specified offset. If all numbers greater than offset are allocated, the search continues
     * at rangeStart.
     *
     * Offset is an absolute value (not relative to rangeStart or rangeEnd).
     *
     * @param rangeStart The lowest-value number in the allocation range
     * @param rangeEnd The highest-value number in the allocation range
     * @param offset Start offset for the search for unallocated numbers
     * @return An unallocated number within the specified allocation range,
     *         preferably greater than or equal to offset
     */
    default int findUnallocatedFromOffset(int rangeStart, int rangeEnd, int offset) throws ExhaustedPoolException
    {
        return findUnallocatedFromOffset(new Range(rangeStart, rangeEnd), offset);
    }

    /**
     * Finds the  first unallocated number within the given range, starting at the specified offset.
     * If all numbers greater than offset are allocated, the search continues at rangeStart.
     *
     * Offset is an absolute value (not relative to rangeStart or rangeEnd).
     *
     * @param range The allocation range
     * @param offset Start offset for the search for unallocated numbers
     * @return An unallocated number within the specified allocation range,
     *         preferably greater than or equal to offset
     */
    int findUnallocatedFromOffset(Range range, int offset) throws ExhaustedPoolException;

    /**
     * Allocates the first unallocated number within the range rangeStart - rangeEnd (inclusively)
     *
     * @param rangeStart The lowest-value number in the allocation range
     * @param rangeEnd The highest-value number in the allocation range
     * @return the lowest-value unallocated number in the specified allocation range
     * @throws ExhaustedPoolException if all numbers within the specified allocation range are allocated
     */
    default int autoAllocate(int rangeStart, int rangeEnd) throws ExhaustedPoolException
    {
        return autoAllocate(List.of(new Range(rangeStart, rangeEnd)));
    }

    /**
     * Allocates the first unallocated number within the given range (inclusively)
     *
     * @param range The allocation range
     * @return the lowest-value unallocated number in the specified allocation range
     * @throws ExhaustedPoolException if all numbers within the specified allocation range are allocated
     */
    default int autoAllocate(Range range) throws ExhaustedPoolException
    {
        return autoAllocate(List.of(range));
    }

    /**
     * Allocates the first unallocated number within the given ranges (inclusively)
     *
     * @param ranges List of ranges to choose from
     * @return the lowest-value unallocated number in the specified allocation range
     * @throws ExhaustedPoolException if all numbers within the specified allocation range are allocated
     */
    int autoAllocate(List<Range> ranges) throws ExhaustedPoolException;

    /**
     * Allocates the  first unallocated number within the range rangeStart - rangeEnd, starting at the
     * specified offset. If all numbers greater than offset are allocated, the search continues
     * at rangeStart.
     *
     * Offset is an absolute value (not relative to rangeStart or rangeEnd).
     *
     * @param rangeStart The lowest-value number in the allocation range
     * @param rangeEnd The highest-value number in the allocation range
     * @param offset Start offset for the search for unallocated numbers
     * @return An unallocated number within the specified allocation range,
     *         preferably greater than or equal to offset
     */

    default int autoAllocateFromOffset(int rangeStart, int rangeEnd, int offset) throws ExhaustedPoolException
    {
        return autoAllocateFromOffset(new Range(rangeStart, rangeEnd), offset);
    }

    /**
     * Allocates the  first unallocated number within the given range, starting at the specified offset.
     * If all numbers greater than offset are allocated, the search continues at rangeStart.
     *
     * Offset is an absolute value (not relative to where range starts or ends).
     *
     * @param range The allocation range
     * @param offset Start offset for the search for unallocated numbers
     * @return An unallocated number within the specified allocation range,
     *         preferably greater than or equal to offset
     */
    int autoAllocateFromOffset(Range range, int offset) throws ExhaustedPoolException;

    /**
     * Clears the pool by resetting the state of all numbers to unallocated
     */
    void clear();
}
