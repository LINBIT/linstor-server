package com.linbit.linstor.numberpool;

// TODO: It may make sense to replace IllegalArgumentException with ImplementationError or some kind of
//       RangeException depending on where it occurs (constructor, method, ...)

import com.linbit.ExhaustedPoolException;
import com.linbit.linstor.annotation.Nullable;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Number allocation pool based on a tree of sparse bitmap arrays
 *
 * Roughly based on prior work by Nick Liu
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class BitmapPool implements NumberPool
{
    // BEGIN Bitmap & level shift constants
    // These constants are tightly linked to the fact that the implementation uses values of type long
    // as bitmap elements. Changing any of these values requires changing the implementation as well,
    // for this reason, do not change any of these values.
    private static final int ELEM_BITS      = 64;
    private static final int MASK_ELEM      = 0x3F;
    private static final int BITMAP_BITS    = 4096;
    private static final int MASK_BITMAP    = 0xFFF;
    private static final int SH_LVL         = 6;
    // END Bitmap & level shift constants

    // power(2, 30) numbers
    public static final int MAX_CAPACITY = 0x40000000;

    private static final long SUBELEMS_EMPTY     = 0L;
    private static final long SUBELEMS_EXHAUSTED = -1L;
    private static final int POOL_EXHAUSTED = -1;

    private final int poolSize;

    private final int levels;
    private final int maxUnalignedLevel;
    private final boolean subLevelsAligned;

    private final int[] levelSubElem;
    private final int[] levelDivisor;
    private final int[] levelShift;

    private final boolean[] levelAligned;
    private final boolean   bitmapAligned;

    private final ReadWriteLock poolLock;
    private final Lock          poolRdLock;
    private final Lock          poolWrLock;

    private final @Nullable LevelDataItem[] levelStack;

    private int allocatedCount;

    private @Nullable BitmapBase rootLevel;

    /**
     * Initializes a pool of the specified size
     *
     * @param size The number of items managed by this pool, where one item is one number
     */
    public BitmapPool(final int size)
    {
        if (size >= 1 && size <= MAX_CAPACITY)
        {
            poolLock = new ReentrantReadWriteLock();
            poolRdLock = poolLock.readLock();
            poolWrLock = poolLock.writeLock();

            poolSize = size;
            bitmapAligned = (poolSize & MASK_BITMAP) == 0;

            // Determine the number of required levels
            {
                int lvlCtr = 0;
                for (int levelCap = BITMAP_BITS; levelCap < size; levelCap <<= SH_LVL)
                {
                    ++lvlCtr;
                }
                levels = lvlCtr;
            }

            // Initialize the levelShift table
            {
                int shift = (levels + 1) * SH_LVL;
                levelShift = new int[levels];
                for (int curLevel = 0; curLevel < levels; ++curLevel)
                {
                    levelShift[curLevel] = shift;
                    shift -= SH_LVL;
                }
            }

            // Initialize the levelSubElem and levelAligned tables
            {
                int curUnalignedLevel = 0;
                levelSubElem = new int[levels];
                levelAligned = new boolean[levels];
                for (int curLevel = 0; curLevel < levels; ++curLevel)
                {
                    int alignBase = BITMAP_BITS << ((levels - curLevel - 1) * SH_LVL);
                    levelSubElem[curLevel] = (ceilAlign2(poolSize, alignBase) >>> levelShift[curLevel]) & MASK_ELEM;
                    levelSubElem[curLevel] = levelSubElem[curLevel] > 0 ? levelSubElem[curLevel] : ELEM_BITS;
                    levelAligned[curLevel] = levelSubElem[curLevel] == ELEM_BITS;
                    if (curLevel > 0 && !levelAligned[curLevel])
                    {
                        curUnalignedLevel = curLevel;
                    }
                }
                if (!bitmapAligned)
                {
                    curUnalignedLevel = levels;
                }
                subLevelsAligned = (curUnalignedLevel == 0);
                maxUnalignedLevel = curUnalignedLevel;
            }

            // Initialize the levelDivisor table
            {
                int divisor = 1 << ((levels + 1) * SH_LVL);
                levelDivisor = new int[levels];
                for (int curLevel = 0; curLevel < levels; ++curLevel)
                {
                    levelDivisor[curLevel] = divisor;
                    divisor >>>= SH_LVL;
                }
            }

            // Initialize the root level and any unaligned levels
            poolInit();

            if (levels > 0)
            {
                levelStack = new LevelDataItem[levels];
                for (int idx = 0; idx < levels; ++idx)
                {
                    levelStack[idx] = new LevelDataItem();
                }
            }
            else
            {
                levelStack = null;
            }

            allocatedCount = 0;
        }
        else
        {
            throw new IllegalArgumentException("poolSize = " + size);
        }
    }

    /**
     * Clears the pool by resetting the state of all numbers to unallocated
     */
    @Override
    public void clear()
    {
        poolWrLock.lock();
        try
        {
            poolInit();
            allocatedCount = 0;
        }
        finally
        {
            poolWrLock.unlock();
        }
    }

    /**
     * Indicates whether the number pool is empty (has no allocated numbers)
     *
     * @return True if none of the numbers managed by the number pool are allocated, false otherwise
     */
    @Override
    public boolean isEmpty()
    {
        return allocatedCount == 0;
    }

    /**
     * Returns the size of this number pool. This is the total count of numbers managed by this number pool.
     *
     * @return Size of the number pool
     */
    @Override
    public int getSize()
    {
        return poolSize;
    }

    /**
     * Returns the count of allocated numbers in this number pool
     *
     * @return Count of currently allocated numbers
     */
    @Override
    public int getAllocatedCount()
    {
        return allocatedCount;
    }

    /**
     * Returns the count of available (unallocated) numbers in this number pool
     *
     * @return Count of currently available (unallocated) numbers
     */
    @Override
    public int getAvailableCount()
    {
        return poolSize - allocatedCount;
    }

    /**
     * Determines whether a number is allocated or free for allocation
     *
     * @param nr the number to check for its allocation status
     * @return true if the number is allocated, false otherwise
     */
    @Override
    public boolean isAllocated(final int nr)
    {
        if (!(nr >= 0 && nr < poolSize))
        {
            throw new IllegalArgumentException();
        }

        boolean result = false;
        poolRdLock.lock();
        try
        {
            if (levels > 0)
            {
                // Traverse the tree until there are no more child elements or until the number bitmap
                // is found
                int curLevel = 0;
                IntermediateBitmap curImBm = (IntermediateBitmap) rootLevel;
                do
                {
                    int subIdx = (nr & (int) ((1L << (levelShift[curLevel] + SH_LVL)) - 1)) >>> levelShift[curLevel];
                    long subBitMask = 1L << subIdx;
                    if ((curImBm.subFullBits & subBitMask) != 0)
                    {
                        // The child element's pool is exhausted, there are no unallocated numbers in the
                        // child element's hierarchy, therefore all numbers are allocated
                        result = true;
                    }
                    else
                    {
                        if (curLevel < levels - 1)
                        {
                            // Traverse further by selecting the child element as the current level
                            // for the next iteration
                            curImBm = (IntermediateBitmap) curImBm.subElem[subIdx];
                        }
                        else
                        {
                            // Iteration reached the number bitmap level
                            Bitmap curBm = (Bitmap) curImBm.subElem[subIdx];
                            // If the number bitmap is not present, then it has been deallocated
                            // because there were no allocated numbers in the bitmap
                            if (curBm != null)
                            {
                                subIdx = (nr & MASK_BITMAP) >>> SH_LVL;
                                subBitMask = 1L << subIdx;
                                if ((curBm.subFullBits & subBitMask) != 0)
                                {
                                    // All numbers in the child element are allocated
                                    result = true;
                                }
                                else
                                {
                                    // Determine the number's allocation status from its allocation bit
                                    long nrBitMask = 1L << (nr & MASK_ELEM);
                                    result = (curBm.numbers[subIdx] & nrBitMask) != 0;
                                }
                            }
                        }
                    }
                    ++curLevel;
                }
                while (curLevel < levels && curImBm != null);
            }
            else
            {
                Bitmap curBm = (Bitmap) rootLevel;
                int subIdx = (nr & MASK_BITMAP) >>> SH_LVL;
                long subBitMask = 1L << subIdx;
                if ((curBm.subFullBits & subBitMask) != 0)
                {
                    // All numbers in the child element are allocated
                    result = true;
                }
                else
                {
                    // Determine the number's allocation status from its allocation bit
                    long nrBitMask = 1L << (nr & MASK_ELEM);
                    result = (curBm.numbers[subIdx] & nrBitMask) != 0;
                }
            }
        }
        finally
        {
            poolRdLock.unlock();
        }
        return result;
    }

    /**
     * Allocates a number
     *
     * @param nr the number to allocate
     * @return true if the number was allocated by this call, false if it had already been allocated
     */
    @Override
    public boolean allocate(final int nr)
    {
        boolean allocated;
        if (nr >= 0 && nr < poolSize)
        {
            poolWrLock.lock();
            try
            {
                allocated = uncheckedAllocateImpl(nr);
            }
            finally
            {
                poolWrLock.unlock();
            }
        }
        else
        {
            throw new IllegalArgumentException();
        }
        return allocated;
    }

    /**
     * Deallocates a number
     *
     * @param nr the number to deallocate
     */
    @Override
    public void deallocate(final int nr)
    {
        if (nr >= 0 && nr < poolSize)
        {
            poolWrLock.lock();
            try
            {
                uncheckedDeallocateImpl(nr);
            }
            finally
            {
                poolWrLock.unlock();
            }
        }
        else
        {
            throw new IllegalArgumentException();
        }
    }

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
    @Override
    public Map<Integer, Boolean> multiAllocate(final List<Integer> nrList)
    {
        for (int nr : nrList)
        {
            if (!(nr >= 0 && nr < poolSize))
            {
                throw new IllegalArgumentException();
            }
        }
        Map<Integer, Boolean> resultMap = new TreeMap<>();
        poolWrLock.lock();
        try
        {
            for (int nr : nrList)
            {
                boolean allocated = uncheckedAllocateImpl(nr);
                resultMap.put(nr, allocated);
            }
        }
        finally
        {
            poolWrLock.unlock();
        }
        return resultMap;
    }

    /**
     * Allocates all specified numbers
     *
     * @param nrList array of numbers to allocate
     */
    @Override
    public void allocateAll(final int[] nrList)
    {
        for (int nr : nrList)
        {
            if (!(nr >= 0 && nr < poolSize))
            {
                throw new IllegalArgumentException();
            }
        }
        poolWrLock.lock();
        try
        {
            for (int nr : nrList)
            {
                uncheckedAllocateImpl(nr);
            }
        }
        finally
        {
            poolWrLock.unlock();
        }
    }

    /**
     * Deallocates all specified numbers
     *
     * @param nrList array of numbers to deallocate
     */
    @Override
    public void deallocateAll(final int[] nrList)
    {
        for (int nr : nrList)
        {
            if (!(nr >= 0 && nr < poolSize))
            {
                throw new IllegalArgumentException();
            }
        }
        poolWrLock.lock();
        try
        {
            for (int nr : nrList)
            {
                uncheckedDeallocateImpl(nr);
            }
        }
        finally
        {
            poolWrLock.unlock();
        }
    }

    /**
     * Finds the first unallocated number within the range rangeStart - rangeEnd (inclusively)
     *
     * @param rangeStart The lowest-value number in the allocation range
     * @param rangeEnd The highest-value number in the allocation range
     * @return the lowest-value unallocated number in the specified allocation range
     * @throws ExhaustedPoolException if all numbers within the specified allocation range are allocated
     */
    @Override
    public int findUnallocated(final int rangeStart, final int rangeEnd)
        throws ExhaustedPoolException
    {
        int result;
        if (rangeStart >= 0 && rangeEnd < poolSize && rangeStart <= rangeEnd)
        {
            result = uncheckedFind(rangeStart, rangeEnd, false);
        }
        else
        {
            throw new IllegalArgumentException();
        }
        return result;
    }

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
     * @throws dsaext.numberpool.NumberPool.ExhaustedPoolException
     */
    @Override
    public int findUnallocatedFromOffset(final int rangeStart, final int rangeEnd, final int offset)
        throws ExhaustedPoolException
    {
        int result;
        if (rangeStart >= 0 && rangeEnd < poolSize && rangeStart <= rangeEnd)
        {
            result = uncheckedFindFromOffset(rangeStart, rangeEnd, offset, false);
        }
        else
        {
            throw new IllegalArgumentException();
        }
        return result;
    }

    /**
     * Allocates the first unallocated number within the range rangeStart - rangeEnd (inclusively)
     *
     * @param rangeStart The lowest-value number in the allocation range
     * @param rangeEnd The highest-value number in the allocation range
     * @return the lowest-value unallocated number in the specified allocation range
     * @throws ExhaustedPoolException if all numbers within the specified allocation range are allocated
     */
    @Override
    public int autoAllocate(final int rangeStart, final int rangeEnd)
        throws ExhaustedPoolException
    {
        int result;
        if (rangeStart >= 0 && rangeEnd < poolSize && rangeStart <= rangeEnd)
        {
            result = uncheckedFind(rangeStart, rangeEnd, true);
        }
        else
        {
            throw new IllegalArgumentException();
        }
        return result;
    }

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
     * @throws dsaext.numberpool.NumberPool.ExhaustedPoolException
     */
    @Override
    public int autoAllocateFromOffset(final int rangeStart, final int rangeEnd, final int offset)
        throws ExhaustedPoolException
    {
        int result;
        if (rangeStart >= 0 && rangeEnd < poolSize && rangeStart <= rangeEnd &&
            rangeStart <= offset && offset <= rangeEnd)
        {
            result = uncheckedFindFromOffset(rangeStart, rangeEnd, offset, true);
        }
        else
        {
            throw new IllegalArgumentException();
        }
        return result;
    }

    private int uncheckedFind(int rangeStart, int rangeEnd, boolean allocFlag)
        throws ExhaustedPoolException
    {
        int result;

        poolWrLock.lock();
        try
        {
            result = uncheckedFindImpl(rangeStart, rangeEnd, allocFlag);
        }
        finally
        {
            poolWrLock.unlock();
        }

        if (result == POOL_EXHAUSTED)
        {
            throw new ExhaustedPoolException();
        }
        return result;
    }

    private int uncheckedFindFromOffset(int rangeStart, int rangeEnd, int offset, boolean allocFlag)
        throws ExhaustedPoolException
    {
        int result;

        poolWrLock.lock();
        try
        {
            if (offset > rangeStart)
            {
                result = uncheckedFindImpl(offset, rangeEnd, allocFlag);
                if (result == POOL_EXHAUSTED)
                {
                    result = uncheckedFindImpl(rangeStart, offset - 1, allocFlag);
                }
            }
            else
            {
                result = uncheckedFindImpl(rangeStart, rangeEnd, allocFlag);
            }
        }
        finally
        {
            poolWrLock.unlock();
        }

        if (result == POOL_EXHAUSTED)
        {
            throw new ExhaustedPoolException();
        }
        return result;
    }

    /**
     * Marks the specified number as allocated
     *
     * The number must be within the range covered by the number pool
     * @param nr Number to allocate
     */
    private boolean uncheckedAllocateImpl(final int nr)
    {
        boolean allocated = false;
        if (levels > 0)
        {
            // Traverse the tree until either the number bitmap is found, or until there are no
            // more child elements and a quick allocation of the hierarchy to the number bitmap
            // can be performed
            IntermediateBitmap curImBm = (IntermediateBitmap) rootLevel;
            // Assume that the current element is on the path that selects the last element in
            // each level, and therefore may be unaligned, until an element is selected in some
            // level that is not the last one
            boolean lastElem = true;
            int curLevel = 0;
            do
            {
                int subIdx = (nr & (int) ((1L << (levelShift[curLevel] + SH_LVL)) - 1)) >>> levelShift[curLevel];
                long subBitMask = 1L << subIdx;
                lastElem &= subIdx == curImBm.subElem.length - 1;
                if ((curImBm.subFullBits & subBitMask) == 0L)
                {
                    // Save the path for updating the pool used / pool full bits if they have to
                    // be changed as a result of the number allocation
                    levelStack[curLevel].imBitmap = curImBm;
                    levelStack[curLevel].elemIdx = subIdx;
                    levelStack[curLevel].elemBitMask = subBitMask;
                    if (curLevel < levels - 1)
                    {
                        IntermediateBitmap nextImBm = (IntermediateBitmap) curImBm.subElem[subIdx];
                        // If the child element is not present, then it has been deallocated
                        // because its pool was empty, and the entire hierarchy can be
                        // allocated immediately
                        if (nextImBm == null)
                        {
                            allocated = true;
                            quickAllocEmpty(curLevel, nr, lastElem);
                            ++allocatedCount;
                            break;
                        }
                        // Traverse further by selecting the child element as the current level
                        // for the next iteration
                        curImBm = nextImBm;
                    }
                    else
                    {
                        // Iteration reached the number bitmap level
                        Bitmap curBm = (Bitmap) curImBm.subElem[subIdx];
                        // If the bitmap is not present, then it has been deallocated because there
                        // were no allocated numbers in the bitmap, and the number bitmap can
                        // be allocated immediately
                        if (curBm == null)
                        {
                            allocated = true;
                            quickAllocEmpty(curLevel, nr, lastElem);
                            ++allocatedCount;
                        }
                        else
                        {
                            subIdx = (nr & MASK_BITMAP) >>> SH_LVL;
                            subBitMask = 1L << subIdx;
                            // If the number bitmap is exhausted, then the number is already allocated
                            if ((curBm.subFullBits & subBitMask) == 0L)
                            {
                                int nrIdx = nr & MASK_ELEM;
                                long nrBitMask = 1L << nrIdx;
                                // If the number is not allocated yet, it is being allocated now
                                allocated = (curBm.numbers[subIdx] & nrBitMask) == 0;
                                if (allocated)
                                {
                                    // Allocate the number
                                    curBm.numbers[subIdx] |= nrBitMask;
                                    // Update pool used / pool full bits
                                    curBm.subUsedBits |= subBitMask;
                                    if (curBm.numbers[subIdx] == SUBELEMS_EXHAUSTED)
                                    {
                                        curBm.subFullBits |= subBitMask;
                                    }
                                    // Update the pool used / pool full bits on intermediate elements
                                    // on the path to the number bitmap and deallocate any elements
                                    // that have an exhausted number pool
                                    allocUpdateLevels(curBm);
                                    ++allocatedCount;
                                }
                            }
                        }
                        break;
                    }
                }
                else
                {
                    // Number is already allocated
                    break;
                }
                ++curLevel;
            }
            while (curLevel < levels);
        }
        else
        {
            Bitmap curBm = (Bitmap) rootLevel;
            int subIdx = nr >>> SH_LVL;
            long subBitMask = 1L << subIdx;
            // If the number bitmap is exhausted, then the number is already allocated
            if ((curBm.subFullBits & subBitMask) == 0L)
            {
                int nrIdx = nr & MASK_ELEM;
                long nrBitMask = 1L << nrIdx;
                // If the number is not allocated yet, it is being allocated now
                allocated = (curBm.numbers[subIdx] & nrBitMask) == 0;
                if (allocated)
                {
                    // Allocate the number
                    curBm.numbers[subIdx] |= nrBitMask;
                    // Update pool used / pool full bits
                    curBm.subUsedBits |= subBitMask;
                    if (curBm.numbers[subIdx] == SUBELEMS_EXHAUSTED)
                    {
                        curBm.subFullBits |= subBitMask;
                    }
                    ++allocatedCount;
                }
            }
        }
        // Release the references to IntermediateBitmap objects to enable deallocation
        for (int idx = 0; idx < levels; ++idx)
        {
            levelStack[idx].imBitmap = null;
        }
        return allocated;
    }

    /**
     * Marks the specified number as unallocated
     *
     * The number must be within the range covered by the number pool
     * @param nr Number to deallocate
     */
    private void uncheckedDeallocateImpl(final int nr)
    {
        if (levels > 0)
        {
            // Traverse the tree until either the number bitmap is found, or until there are no
            // more child elements and a quick allocation of the hierarchy to the number bitmap
            // can be performed
            IntermediateBitmap curImBm = (IntermediateBitmap) rootLevel;
            int curLevel = 0;
            // Assume that the current element is on the path that selects the last element in
            // each level, and therefore may be unaligned, until an element is selected in some
            // level that is not the last one
            boolean lastElem = true;
            do
            {
                int subIdx = (nr & (int) ((1L << (levelShift[curLevel] + SH_LVL)) - 1)) >>> levelShift[curLevel];
                long subBitMask = 1L << subIdx;
                lastElem &= subIdx == curImBm.subElem.length - 1;
                // Save the path for updating the pool used / pool full bits in case that they have
                // to be changed as a result of the number allocation
                levelStack[curLevel].imBitmap = curImBm;
                levelStack[curLevel].elemIdx = subIdx;
                levelStack[curLevel].elemBitMask = subBitMask;
                if (curLevel < levels - 1)
                {
                    IntermediateBitmap nextImBm = (IntermediateBitmap) curImBm.subElem[subIdx];
                    // If the child element is not present, then it has been deallocated because its
                    // pool was either empty or exhausted
                    if (nextImBm == null)
                    {
                        if ((curImBm.subFullBits & subBitMask) == 0L)
                        {
                            // Child element has been deallocated because it was empty
                            // Number is not allocated
                            break;
                        }
                        else
                        {
                            // Child element was deallocated because its pool was exhausted, and
                            // the entire hierarchy can be allocated immediately
                            quickAllocFull(curLevel, nr, lastElem);
                            --allocatedCount;
                        }
                        break;
                    }
                    // Traverse further by selecting the child element as the current level
                    // for the next iteration
                    curImBm = nextImBm;
                }
                else
                {
                    // Iteration reached the bitmap level
                    Bitmap curBm = (Bitmap) curImBm.subElem[subIdx];
                    // If the number bitmap is not present, then it has been deallocated because
                    // it was either empty or exhausted
                    if (curBm == null)
                    {
                        if ((curImBm.subFullBits & subBitMask) == 0L)
                        {
                            // Number bitmap has been deallocated because it was empty
                            // Number is not allocated
                            break;
                        }
                        else
                        {
                            // Number bitmap has been deallocated because it was exhausted, and
                            // the number bitmap can be allocated immediately
                            quickAllocFull(curLevel, nr, lastElem);
                            --allocatedCount;
                        }
                    }
                    else
                    {
                        subIdx = (nr & MASK_BITMAP) >>> SH_LVL;
                        subBitMask = 1L << subIdx;
                        int nrIdx = nr & MASK_ELEM;
                        long nrBitMask = 1L << nrIdx;
                        if ((curBm.numbers[subIdx] & nrBitMask) != 0)
                        {
                            // Deallocate the number
                            curBm.numbers[subIdx] &= ~nrBitMask;
                            // Update the pool used / pool full bits
                            curBm.subFullBits  &= ~subBitMask;
                            if (curBm.numbers[subIdx] == SUBELEMS_EMPTY)
                            {
                                curBm.subUsedBits &= ~subBitMask;
                            }
                            // Update the pool used / pool full bits on intermediate elements
                            // on the path to the number bitmap and deallocate any elements
                            // that have an empty number pool
                            deallocUpdateLevels(curBm);
                            --allocatedCount;
                        }
                    }
                }
                ++curLevel;
            }
            while (curLevel < levels);
        }
        else
        {
            Bitmap curBm = (Bitmap) rootLevel;
            int subIdx = nr >>> SH_LVL;
            long subBitMask = 1L << subIdx;
            int nrIdx = nr & MASK_ELEM;
            long nrBitMask = 1L << nrIdx;
            if ((curBm.numbers[subIdx] & nrBitMask) != 0)
            {
                // Deallocate the number
                curBm.numbers[subIdx] &= ~nrBitMask;
                // Update pool used / pool full bits
                curBm.subFullBits &= ~subBitMask;
                if (curBm.numbers[subIdx] == 0L)
                {
                    curBm.subUsedBits &= ~subBitMask;
                }
                --allocatedCount;
            }
        }
        // Release the references to IntermediateBitmap objects to enable deallocation
        for (int idx = 0; idx < levels; ++idx)
        {
            levelStack[idx].imBitmap = null;
        }
    }

    /**
     * Searches for an unallocated number within the specified range
     *
     * @param rangeStart Lower bound for the search (inclusive)
     * @param rangeEnd Upper bound for the search (inclusive)
     * @param allocate if true, and an unallocated number is found, the number is allocated; if false, the
     *     number remains unallocated
     * @return An unallocated number within the specified range, or -1 if no unallocated number is available
     */
    private int uncheckedFindImpl(final int rangeStart, final int rangeEnd, boolean allocate)
    {
        int result = POOL_EXHAUSTED;
        int nr = rangeStart;
        if (levels > 0)
        {
            // Assume rangeStart as the candidate for allocation and traverse the tree until either
            // the number bitmap is found, or until there are no more child elements and a
            // quick allocation of the hierarchy to the number bitmap of the candidate number can
            // be performed.
            // If the current candidate is allocated, or an entire hierarchy containing it is
            // allocated, skip the number or the hierarchy and calculate the new candidate number,
            // then repeat the search process either on the current level of the tree or from the
            // root level of the tree.
            int curLevel = 0;
            IntermediateBitmap curImBm = (IntermediateBitmap) rootLevel;
            if (curImBm.subFullBits != SUBELEMS_EXHAUSTED)
            {
                do
                {
                    int subIdx = (nr & (int) ((1L << (levelShift[curLevel] + SH_LVL)) - 1)) >>> levelShift[curLevel];
                    long subBitMask = 1L << subIdx;
                    if ((curImBm.subFullBits & subBitMask) == 0)
                    {
                        // If the goal of the search is allocating the number rather than just finding
                        // an unallocated number, save the path for updating the pool used / pool full
                        // bits in case that they have to be changed as a result of the number allocation
                        if (allocate)
                        {
                            levelStack[curLevel].imBitmap = curImBm;
                            levelStack[curLevel].elemIdx = subIdx;
                            levelStack[curLevel].elemBitMask = subBitMask;
                        }
                        if (curLevel < levels - 1)
                        {
                            IntermediateBitmap nextImBm = (IntermediateBitmap) curImBm.subElem[subIdx];
                            // If the child element is not present, then it has been deallocated because
                            // its pool was empty
                            if (nextImBm == null)
                            {
                                // The current candidate number is an unallocated number and the
                                // entire hierarchy can be allocated immediately
                                result = nr;
                                if (allocate)
                                {
                                    boolean lastElem = true;
                                    for (int idx = 0; lastElem && idx <= curLevel; ++idx)
                                    {
                                        if (levelStack[idx].elemIdx < levelSubElem[idx] - 1)
                                        {
                                            lastElem = false;
                                        }
                                    }
                                    quickAllocEmpty(curLevel, nr, lastElem);
                                    ++allocatedCount;
                                }
                            }
                            else
                            {
                                // Traverse further by selecting the child element as the current level
                                // for the next iteration
                                curImBm = nextImBm;
                                ++curLevel;
                            }
                        }
                        else
                        {
                            // Iteration reached the bitmap level
                            Bitmap curBm = (Bitmap) curImBm.subElem[subIdx];
                            // If the number bitmap is not present, then it has been deallocated
                            // because its pool was empty
                            if (curBm == null)
                            {
                                // The current candidate number is an unallocated number and the
                                // number bitmap can be allocated immediately
                                result = nr;
                                if (allocate)
                                {
                                    boolean lastElem = true;
                                    for (int idx = 0; lastElem && idx <= curLevel; ++idx)
                                    {
                                        if (levelStack[idx].elemIdx < levelSubElem[idx] - 1)
                                        {
                                            lastElem = false;
                                        }
                                    }
                                    quickAllocEmpty(curLevel, nr, lastElem);
                                    ++allocatedCount;
                                }
                            }
                            else
                            {
                                subIdx = (nr & MASK_BITMAP) >>> SH_LVL;
                                subBitMask = 1L << subIdx;
                                // Iterate over the number bitmap's element to find an element
                                // that has unallocated numbers in its pool
                                do
                                {
                                    if ((curBm.subFullBits & subBitMask) == 0)
                                    {
                                        int nrIdx = nr & MASK_ELEM;
                                        long nrBitMask = 1L << nrIdx;
                                        // Iterate over the number bitmap element's numbers allocation
                                        // bits to find an unallocated number
                                        do
                                        {
                                            if ((curBm.numbers[subIdx] & nrBitMask) == 0)
                                            {
                                                result = nr;
                                                if (allocate)
                                                {
                                                    curBm.numbers[subIdx] |= nrBitMask;
                                                    curBm.subUsedBits |= subBitMask;
                                                    if (curBm.numbers[subIdx] == SUBELEMS_EXHAUSTED)
                                                    {
                                                        // All numbers in the current cell are allocated
                                                        curBm.subFullBits |= subBitMask;
                                                    }
                                                    allocUpdateLevels(curBm);
                                                    ++allocatedCount;
                                                }
                                            }
                                            else
                                            {
                                                nrBitMask <<= 1;
                                                ++nr;
                                            }
                                        }
                                        while (result == POOL_EXHAUSTED && nrBitMask != 0L && nr <= rangeEnd);
                                        if (nrBitMask == 0L)
                                        {
                                            // Iteration left the range of the currently selected
                                            // element, select the next element, the candidate number
                                            // has already been advanced to the next element's first
                                            // number's value
                                            ++subIdx;
                                            subBitMask <<= 1;
                                        }
                                    }
                                    else
                                    {
                                        // The current element's pool is exhausted, advance the
                                        // candidate number to the next element's first number's value
                                        ++subIdx;
                                        subBitMask <<= 1;
                                        nr = (nr + ELEM_BITS) & ~MASK_ELEM;
                                    }
                                }
                                while (result == POOL_EXHAUSTED && subBitMask != 0L && nr <= rangeEnd);
                                if (result == POOL_EXHAUSTED && nr <= rangeEnd)
                                {
                                    // All elements of the currently selected bitmap that are within
                                    // the range for the search had an exhausted pool, and the candidate
                                    // number has already been advanced to the next number bitmap's
                                    // first element's first number's value.
                                    // Restart the search at the root level;
                                    curImBm = (IntermediateBitmap) rootLevel;
                                    curLevel = 0;
                                }
                            }
                        }
                    }
                    else
                    {
                        // The entire currently selected hierarchy contains only exhausted pools in
                        // the range for the search, advance the candidate number to the next
                        // child element of the current level
                        nr = (nr + levelDivisor[curLevel]) & ~(levelDivisor[curLevel] - 1);
                        if (subIdx >= curImBm.subElem.length - 1)
                        {
                            // If the candidate number was advanced beyond the range of child
                            // elements that the element selected at the current level manages,
                            // restart the search at the root level to select the next element
                            // on one of the levels higher up in the hierarchy
                            curImBm = (IntermediateBitmap) rootLevel;
                            curLevel = 0;
                        }
                    }
                }
                while (result == POOL_EXHAUSTED && nr <= rangeEnd);
            }
        }
        else
        {
            Bitmap curBm = (Bitmap) rootLevel;
            if (curBm.subFullBits != SUBELEMS_EXHAUSTED)
            {
                int subIdx = (nr & MASK_BITMAP) >>> SH_LVL;
                long subBitMask = 1L << subIdx;
                // Iterate over the number bitmap element's numbers allocation
                // bits to find an unallocated number
                do
                {
                    if ((curBm.subFullBits & subBitMask) == 0)
                    {
                        int nrIdx = nr & MASK_ELEM;
                        long nrBitMask = 1L << nrIdx;
                        // Iterate over the number bitmap element's numbers allocation
                        // bits to find an unallocated number
                        do
                        {
                            if ((curBm.numbers[subIdx] & nrBitMask) == 0)
                            {
                                result = nr;
                                if (allocate)
                                {
                                    curBm.numbers[subIdx] |= nrBitMask;
                                    curBm.subUsedBits |= subBitMask;
                                    if (curBm.numbers[subIdx] == SUBELEMS_EXHAUSTED)
                                    {
                                        // All numbers in the current cell are allocated
                                        curBm.subFullBits |= subBitMask;
                                    }
                                    ++allocatedCount;
                                }
                            }
                            else
                            {
                                nrBitMask <<= 1;
                                ++nr;
                            }
                        }
                        while (result == POOL_EXHAUSTED && nrBitMask != 0L && nr <= rangeEnd);
                        if (nrBitMask == 0L)
                        {
                            ++subIdx;
                            subBitMask <<= 1;
                            // Iteration left the range of the currently selected
                            // element, select the next element, the candidate number
                            // has already been advanced to the next element's first
                            // number's value
                        }
                    }
                    else
                    {
                        // The current element's pool is exhausted, advance the
                        // candidate number to the next element's first number's value
                        ++subIdx;
                        subBitMask <<= 1;
                        nr = (nr + ELEM_BITS) & ~MASK_ELEM;
                    }
                }
                while (result == POOL_EXHAUSTED && subBitMask != 0L && nr <= rangeEnd);
            }
        }
        // Release the references to IntermediateBitmap objects to enable deallocation
        for (int idx = 0; idx < levels; ++idx)
        {
            levelStack[idx].imBitmap = null;
        }
        return result;
    }

    /**
     * Increases an unaligned value to the next greater multiple of the specified power of 2 base
     *
     * @param value The value to align
     * @param base The base to align the value to; must be a power of 2
     * @return Aligned value
     */
    private static int ceilAlign2(final int value, final int base)
    {
        int result = value & ~(base - 1);
        if (result != value)
        {
            result += base;
        }
        return result;
    }

    /**
     * New empty intermediate bitmaps & new empty bitmap - quick number allocation
     */
    private void quickAllocEmpty(
        final int curLevel,
        final int nr,
        boolean lastElemInit
    )
    {
        boolean lastElem = lastElemInit;
        // Update the pool used bits from the root level to the level where
        // the allocation of child elements starts
        for (int idx = 0; idx <= curLevel; ++idx)
        {
            LevelDataItem lvl = levelStack[idx];
            lvl.imBitmap.subUsedBits |= lvl.elemBitMask;
        }

        // Allocate any intermediate levels
        IntermediateBitmap curImBm = levelStack[curLevel].imBitmap;
        int subIdx = levelStack[curLevel].elemIdx;
        for (int qAllocLevel = curLevel + 1; qAllocLevel < levels; ++qAllocLevel)
        {
            IntermediateBitmap nextImBm;
            if (!lastElem || subLevelsAligned || levelAligned[qAllocLevel])
            {
                nextImBm = new IntermediateBitmap(
                    false, subLevelsAligned || qAllocLevel <= maxUnalignedLevel
                );
            }
            else
            {
                nextImBm = new IntermediateBitmap(
                    levelSubElem[qAllocLevel], false,
                    subLevelsAligned || qAllocLevel <= maxUnalignedLevel
                );
            }
            curImBm.subElem[subIdx] = nextImBm;
            subIdx = (nr & (int) ((1L << (levelShift[qAllocLevel] + SH_LVL)) - 1)) >>> levelShift[qAllocLevel];
            nextImBm.subUsedBits |= 1L << subIdx;
            curImBm = nextImBm;
            lastElem &= subIdx == curImBm.subElem.length - 1;
        }

        // Allocate the number bitmap
        Bitmap curBm;
        if (!lastElem || bitmapAligned)
        {
            curBm = new Bitmap(false);
        }
        else
        {
            int lastBmBits = poolSize & MASK_BITMAP;
            curBm = new Bitmap(lastBmBits, false);
        }
        // Link the number bitmap in the intermediate level
        curImBm.subElem[subIdx] = curBm;
        // Allocate the number in the number bitmap
        subIdx = (nr & MASK_BITMAP) >>> SH_LVL;
        curBm.numbers[subIdx] |= 1L << (nr & MASK_ELEM);
        // Update the pool used bit on the number bitmap's element that contains the
        // number allocation flag
        curBm.subUsedBits |= 1L << subIdx;
    }

    /**
     * New full intermediate bitmaps & new full bitmap - quick number allocation
     */
    private void quickAllocFull(
        final int curLevel,
        final int nr,
        boolean lastElemInit
    )
    {
        boolean lastElem = lastElemInit;
        // Update the pool full bits from the root level to the level where
        // the allocation of child elements starts
        for (int idx = 0; idx <= curLevel; ++idx)
        {
            LevelDataItem lvl = levelStack[idx];
            lvl.imBitmap.subFullBits &= ~lvl.elemBitMask;
        }

        // Allocate any intermediate levels
        IntermediateBitmap curImBm = levelStack[curLevel].imBitmap;
        int subIdx = levelStack[curLevel].elemIdx;
        curImBm.subFullBits &= ~(1L << subIdx);
        for (int qAllocLevel = curLevel + 1; qAllocLevel < levels; ++qAllocLevel)
        {
            IntermediateBitmap nextImBm;
            if (!lastElem || subLevelsAligned || levelAligned[qAllocLevel])
            {
                nextImBm = new IntermediateBitmap(
                    true, subLevelsAligned || qAllocLevel <= maxUnalignedLevel
                );
            }
            else
            {
                nextImBm = new IntermediateBitmap(
                    levelSubElem[qAllocLevel], true,
                    subLevelsAligned || qAllocLevel <= maxUnalignedLevel
                );
            }
            curImBm.subElem[subIdx] = nextImBm;
            subIdx = (nr & (int) ((1L << (levelShift[qAllocLevel] + SH_LVL)) - 1)) >>> levelShift[qAllocLevel];
            nextImBm.subFullBits &= ~(1L << subIdx);
            curImBm = nextImBm;
            lastElem &= subIdx == curImBm.subElem.length - 1;
        }

        // Allocate the number bitmap
        Bitmap curBm;
        if (!lastElem || bitmapAligned)
        {
            curBm = new Bitmap(true);
        }
        else
        {
            int lastBmBits = poolSize & MASK_BITMAP;
            curBm = new Bitmap(lastBmBits, true);
        }
        // Link the number bitmap in the intermediate level
        curImBm.subElem[subIdx] = curBm;
        // Deallocate the number in the number bitmap
        subIdx = (nr & MASK_BITMAP) >>> SH_LVL;
        curBm.numbers[subIdx] &= ~(1L << (nr & MASK_ELEM));
        // Update the pool full bit on the number bitmap's element that contains the
        // number allocation flag
        curBm.subFullBits &= ~(1L << subIdx);
    }

    private void allocUpdateLevels(Bitmap curBm)
    {
        boolean full = curBm.subFullBits == SUBELEMS_EXHAUSTED;
        // Update the full and used bits in upper levels of the tree
        for (int idx = levels - 1; idx >= 0; --idx)
        {
            LevelDataItem lvl = levelStack[idx];
            // If the previously selected level was exhausted, deallocate the sub element
            // for the exhausted level in the currently selected level
            if (full)
            {
                lvl.imBitmap.subFullBits |= lvl.elemBitMask;
                lvl.imBitmap.subElem[lvl.elemIdx] = null;
            }
            // If the current level is exhausted, deallocate the current level in the
            // next iteration (unless it is the root level)
            full = lvl.imBitmap.subFullBits == SUBELEMS_EXHAUSTED;
            // Set the used bits to indicate that at least one number in a sub element
            // of the current level is allocated.
            // Skip updating levels that are exhausted (because they will be deallocated),
            // always update the root level (because the root level is never deallocated)
            if (!full || idx <= 0)
            {
                lvl.imBitmap.subUsedBits |= lvl.elemBitMask;
            }
        }
    }

    private void deallocUpdateLevels(Bitmap curBm)
    {
        boolean empty = curBm.subUsedBits == SUBELEMS_EMPTY;
        // Update the full and used bits in uppper levels of the tree
        for (int idx = levels - 1; idx >= 0; --idx)
        {
            LevelDataItem lvl = levelStack[idx];
            // If the previously selected level was empty, deallocate the sub element
            // for the empty level in the currently selected level
            if (empty)
            {
                lvl.imBitmap.subUsedBits &= ~lvl.elemBitMask;
                lvl.imBitmap.subElem[lvl.elemIdx] = null;
            }
            // If the current level is empty, deallocate the current level in the
            // next iteration (unless it is the root level)
            empty = lvl.imBitmap.subUsedBits == SUBELEMS_EMPTY;
            // Clear the full bits to indicate that numbers are available for allocation in
            // a sub element of the current level.
            // Skip updating levels that are empty (because they will be deallocated),
            // always update the root level (because the root level is never deallocated)
            if (!empty || idx <= 0)
            {
                lvl.imBitmap.subFullBits &= ~lvl.elemBitMask;
            }
        }
    }

    private void poolInit()
    {
        if (levels > 0)
        {
            if (levelSubElem[0] == ELEM_BITS)
            {
                rootLevel = new IntermediateBitmap(false, subLevelsAligned);
            }
            else
            {
                rootLevel = new IntermediateBitmap(levelSubElem[0], false, subLevelsAligned);
            }
            // If any sublevel is not aligned, allocate the last element of
            // each level up to the last unaligned level
            if (!subLevelsAligned)
            {
                IntermediateBitmap curImBm = (IntermediateBitmap) rootLevel;
                for (int curLevel = 1; curLevel < levels && curLevel <= maxUnalignedLevel; ++curLevel)
                {
                    int prevLevelLastElem = levelSubElem[curLevel - 1] - 1;
                    IntermediateBitmap subImBm;
                    if (levelSubElem[curLevel] == ELEM_BITS)
                    {
                        subImBm = new IntermediateBitmap(false, false);
                        curImBm.subElem[prevLevelLastElem] = subImBm;
                    }
                    else
                    {
                        subImBm = new IntermediateBitmap(levelSubElem[curLevel], false, false);
                        curImBm.subElem[prevLevelLastElem] = subImBm;
                    }
                    curImBm = subImBm;
                }
                if (!bitmapAligned)
                {
                    int lastBmBits = poolSize & MASK_BITMAP;
                    curImBm.subElem[levelSubElem[levels - 1] - 1] = new Bitmap(lastBmBits, false);
                }
            }
        }
        else
        {
            if (bitmapAligned)
            {
                rootLevel = new Bitmap(false);
            }
            else
            {
                rootLevel = new Bitmap(poolSize, false);
            }
        }
    }

    private static class LevelDataItem
    {
        int     elemIdx     = 0;
        long    elemBitMask = 0L;
        @Nullable IntermediateBitmap imBitmap = null;
    }

    private abstract static class BitmapBase
    {
        long subFullBits;
        long subUsedBits;
    }

    private static class Bitmap extends BitmapBase
    {
        long[] numbers;

        Bitmap(boolean exhausted)
        {
            numbers = new long[ELEM_BITS];
            if (exhausted)
            {
                subFullBits = SUBELEMS_EXHAUSTED;
                subUsedBits = SUBELEMS_EXHAUSTED;
                Arrays.fill(numbers, SUBELEMS_EXHAUSTED);
            }
            else
            {
                subFullBits = 0L;
                subUsedBits = 0L;
            }
        }

        Bitmap(int bits, boolean exhausted)
        {
            if (bits >= 1 && bits < BITMAP_BITS)
            {
                int elemCount = ceilAlign2(bits, ELEM_BITS) >>> SH_LVL;
                numbers = new long[elemCount];
                if (exhausted)
                {
                    // Mark all numbers exhausted
                    subFullBits = SUBELEMS_EXHAUSTED;
                    subUsedBits = SUBELEMS_EXHAUSTED;
                    Arrays.fill(numbers, SUBELEMS_EXHAUSTED);
                }
                else
                {
                    if (elemCount < ELEM_BITS)
                    {
                        subFullBits = ~((1L << elemCount) - 1);
                    }

                    int lastElemBits = bits & MASK_ELEM;
                    if (lastElemBits != 0)
                    {
                        // Mark all nonexistent numbers exhausted
                        numbers[elemCount - 1] = ~((1L << lastElemBits) - 1);
                    }

                    int subUsedElems = lastElemBits == 0 ? elemCount : elemCount - 1;
                    subUsedBits = ~((1L << subUsedElems) - 1);
                }
            }
            else
            {
                throw new IllegalArgumentException();
            }
        }
    }

    private static class IntermediateBitmap extends BitmapBase
    {
        BitmapBase[] subElem;

        IntermediateBitmap(boolean exhausted, boolean subElemsAligned)
        {
            subElem = new BitmapBase[ELEM_BITS];
            if (exhausted)
            {
                subFullBits = SUBELEMS_EXHAUSTED;
                subUsedBits = SUBELEMS_EXHAUSTED;
            }
            else
            {
                subFullBits = 0L;
                subUsedBits = subElemsAligned ? 0L : ~(1L << (ELEM_BITS - 1));
            }
        }

        IntermediateBitmap(int subElemCount, boolean exhausted, boolean subElemsAligned)
        {
            if (!(subElemCount >= 1) && subElemCount < ELEM_BITS)
            {
                throw new IllegalArgumentException();
            }

            subElem = new BitmapBase[subElemCount];
            if (exhausted)
            {
                // Mark all elements exhausted
                subFullBits = SUBELEMS_EXHAUSTED;
                subUsedBits = SUBELEMS_EXHAUSTED;
            }
            else
            {
                // Mark all nonexistent elements exhausted
                subFullBits = ~((1L << subElemCount) - 1);
                int subUsedElems = subElemsAligned ? subElemCount : subElemCount - 1;
                subUsedBits = ~((1L << subUsedElems) - 1);
            }
        }
    }

    public static class PoolDebugger
    {
        private final BitmapPool bmPool;

        PoolDebugger(BitmapPool poolRef)
        {
            bmPool = poolRef;
        }

        public void debugPrintPoolProperties(PrintStream output)
        {
            output.printf("Pool size = %9d, levels = %d, ", bmPool.poolSize, bmPool.levels);
            output.println("sublevels aligned = " + Boolean.toString(bmPool.subLevelsAligned));
            for (int curLevel = 0; curLevel < bmPool.levels; ++curLevel)
            {
                output.printf(
                    "    Level %d: IntermediateBitmap elements = %2d\n" +
                    "             [levelShift = %2d, levelDivisor = %9d]\n",
                    curLevel,
                    bmPool.levelSubElem[curLevel] > 0 ? bmPool.levelSubElem[curLevel] : ELEM_BITS,
                    bmPool.levelShift[curLevel],
                    bmPool.levelDivisor[curLevel]
                );
            }
            int nrBitElems = (ceilAlign2(bmPool.poolSize, ELEM_BITS) >>> SH_LVL) & MASK_ELEM;
            nrBitElems = nrBitElems > 0 ? nrBitElems : ELEM_BITS;
            int nrBitmapBits = bmPool.poolSize & MASK_ELEM;
            nrBitmapBits = nrBitmapBits > 0 ? nrBitmapBits : ELEM_BITS;
            output.printf(
                "    Bitmap:  Last bitmap number bit elements = %2d\n",
                nrBitElems
            );
            output.printf(
                "             Last bitmap last element used bits = %2d\n",
                nrBitmapBits
            );
            output.println();
        }

        public void debugPrintPool(PrintStream output)
        {
            output.printf("Pool size = %d, levels = %d, ", bmPool.poolSize, bmPool.levels);
            output.println(
                "subLevelsAligned = " + bmPool.subLevelsAligned + "," +
                "bitmapAligned = " + bmPool.bitmapAligned
            );
            if (bmPool.levels > 0)
            {
                debugPrintIntermediateBitmap(output, 0, 0, (IntermediateBitmap) bmPool.rootLevel);
            }
            else
            {
                debugPrintBitmap(output, 0, 0, (Bitmap) bmPool.rootLevel);
            }
            output.println();
        }

        private void debugPrintIntermediateBitmap(
            PrintStream output,
            final int curLevel,
            final int nrOffset,
            final IntermediateBitmap curImBm
        )
        {
            debugPrintIndent(output, curLevel);
            output.printf(
                "Level %d: IntermediateBitmap subElem = %2d, nrOffset = %8d\n",
                curLevel, curImBm.subElem.length, nrOffset
            );

            debugPrintIndent(output, curLevel);
            output.printf(
                "         subFullBits = 0x%016X\n",
                curImBm.subFullBits
            );
            debugPrintIndent(output, curLevel);
            output.println("         subFullBits = [" + debugGetBinary(curImBm.subFullBits) + "]");

            debugPrintIndent(output, curLevel);
            output.printf(
                "         subUsedBits = 0x%016X\n",
                curImBm.subUsedBits
            );
            debugPrintIndent(output, curLevel);
            output.println("         subUsedBits = [" + debugGetBinary(curImBm.subUsedBits) + "]");

            boolean subElemsAllocated = false;
            for (int index = 0; index < curImBm.subElem.length; ++index)
            {
                if (curImBm.subElem[index] != null)
                {
                    subElemsAllocated = true;
                    int subNrOffset = nrOffset + (BITMAP_BITS << ((bmPool.levels - curLevel - 1) * SH_LVL)) * index;
                    debugPrintIndent(output, curLevel + 1);
                    output.printf("subElem %2d:\n", index);
                    if (curLevel < bmPool.levels - 1)
                    {
                        debugPrintIntermediateBitmap(
                            output,
                            curLevel + 1, subNrOffset,
                            (IntermediateBitmap) curImBm.subElem[index]
                        );
                    }
                    else
                    {
                        debugPrintBitmap(output, curLevel + 1, subNrOffset, (Bitmap) curImBm.subElem[index]);
                    }
                }
            }
            if (!subElemsAllocated)
            {
                debugPrintIndent(output, curLevel + 1);
                output.printf("Level %d: Not allocated\n", curLevel + 1);
            }
        }

        private void debugPrintBitmap(
            PrintStream output,
            final int curLevel,
            final int nrOffset,
            final Bitmap curBm
        )
        {
            debugPrintIndent(output, curLevel);
            output.printf(
                "Level %d: Bitmap subElem = %2d, nrOffset = %8d\n",
                curLevel, curBm.numbers.length, nrOffset
            );

            debugPrintIndent(output, curLevel);
            output.printf(
                "         subFullBits = 0x%016X\n",
                curBm.subFullBits
            );
            debugPrintIndent(output, curLevel);
            output.println("         subFullBits = [" + debugGetBinary(curBm.subFullBits) + "]");

            debugPrintIndent(output, curLevel);
            output.printf(
                "         subUsedBits = 0x%016X\n",
                curBm.subUsedBits
            );
            debugPrintIndent(output, curLevel);
            output.println("         subUsedBits = [" + debugGetBinary(curBm.subUsedBits) + "]");

            int rangeStart = -1;
            for (int index = 0; index < curBm.numbers.length; ++index)
            {
                long numberBits = curBm.numbers[index];
                if (numberBits != 0)
                {
                    int subNrOffset = nrOffset + (ELEM_BITS * index);
                    long bitMask = 1L;
                    for (int bitIndex = 0; bitIndex < ELEM_BITS; ++bitIndex)
                    {
                        if ((numberBits & bitMask) == 0)
                        {
                            if (rangeStart != -1)
                            {
                                int rangeEnd = subNrOffset + bitIndex - 1;
                                debugPrintAllocatedRange(output, curLevel + 1, rangeStart, rangeEnd);
                                rangeStart = -1;
                            }
                        }
                        else
                        {
                            if (rangeStart == -1)
                            {
                                rangeStart = subNrOffset + bitIndex;
                            }
                        }
                        bitMask <<= 1;
                    }
                }
            }
            if (rangeStart != -1)
            {
                int rangeEnd = nrOffset + curBm.numbers.length * ELEM_BITS - 1;
                debugPrintAllocatedRange(output, curLevel + 1, rangeStart, rangeEnd);
            }
        }

        private void debugPrintAllocatedRange(
            PrintStream output,
            final int curLevel,
            final int rangeStart,
            final int rangeEnd
        )
        {
            debugPrintIndent(output, curLevel);
            if (rangeStart < rangeEnd)
            {
                output.printf("Allocated: [%8d] - [%8d]\n", rangeStart, rangeEnd);
            }
            else
            {
                output.printf("Allocated: [%8d]\n", rangeStart);
            }
        }

        public static String debugGetBinary(long value)
        {
            byte[] digits = new byte[ELEM_BITS];
            Arrays.fill(digits, (byte) '0');
            long bitMask = 1L;
            for (int index = 1; index <= ELEM_BITS; ++index)
            {
                digits[ELEM_BITS - index] = (byte) ((value & bitMask) == 0 ? '0' : '1');
                bitMask <<= 1;
            }
            return new String(digits);
        }

        private static void debugPrintIndent(PrintStream output, int curLevel)
        {
            for (int counter = 0; counter < curLevel; ++counter)
            {
                output.print("  ");
            }
        }

        public void debugPrintNumberPath(PrintStream output, int nr)
        {
            int[] selectedElem = new int[bmPool.levels];
            output.printf("Search path for number %d\n", nr);
            for (int curLevel = 0; curLevel < bmPool.levels; ++curLevel)
            {
                selectedElem[curLevel] = (nr & (int) ((1L << (bmPool.levelShift[curLevel] + SH_LVL)) - 1)) >>>
                    bmPool.levelShift[curLevel];
                output.printf("Level %d: Select sub element %d\n", curLevel, selectedElem[curLevel]);
            }
            int bmSubIdx = (nr & MASK_BITMAP) >>> SH_LVL;
            int bmBitIdx = nr & MASK_ELEM;
            output.printf("Bitmap element %d, number bit %d\n", bmSubIdx, bmBitIdx);

            output.println("Reconstruction");
            int sum = 0;
            for (int curLevel = 0; curLevel < bmPool.levels; ++curLevel)
            {
                int curSummand = selectedElem[curLevel] * bmPool.levelDivisor[curLevel];
                output.printf(
                    "%2d x %8d = %10d\n",
                    selectedElem[curLevel], bmPool.levelDivisor[curLevel], curSummand
                );
                sum += curSummand;
            }
            int bmElemSummand = bmSubIdx * ELEM_BITS;
            output.printf(
                "%2d x %8d = %10d\n",
                bmSubIdx, ELEM_BITS, bmElemSummand
            );
            output.printf(
                "              + %10d\n", bmBitIdx
            );
            sum += bmElemSummand;
            sum += bmBitIdx;
            output.printf("--------------------------\n%10d\n", sum);
        }

        public boolean debugCheckNumberPath(PrintStream output, int nr)
        {
            boolean correctFlag = true;
            try
            {
                boolean lastElem = true;
                for (int curLevel = 0; curLevel < bmPool.levels; ++curLevel)
                {
                    int subIdx = (nr & (int) ((1L << (bmPool.levelShift[curLevel] + SH_LVL)) - 1)) >>>
                        bmPool.levelShift[curLevel];
                    int maxSubIdx = lastElem ? bmPool.levelSubElem[curLevel] - 1 : ELEM_BITS - 1;
                    if (subIdx > maxSubIdx)
                    {
                        output.printf(
                            "PoolSize %d, Number %d, Level %d: Selected element %d out of range [0 - %d]\n",
                            bmPool.poolSize, nr, curLevel, subIdx, bmPool.levelSubElem[curLevel]
                        );
                        throw new AbortedCheckException();
                    }
                    if (subIdx != maxSubIdx)
                    {
                        lastElem = false;
                    }
                }
                int maxSubIdx = lastElem ?
                                (BitmapPool.ceilAlign2(bmPool.poolSize & MASK_BITMAP, ELEM_BITS) >>> SH_LVL) - 1 :
                                ELEM_BITS - 1;
                int bmSubIdx = (nr & MASK_BITMAP) >>> SH_LVL;
                if (bmSubIdx > maxSubIdx)
                {
                    output.printf(
                        "PoolSize %d, Number %d, Bitmap: Selected element %d out of range [0 - %d]\n",
                        bmPool.poolSize, nr, bmSubIdx, maxSubIdx
                    );
                    throw new AbortedCheckException();
                }
                if (bmSubIdx != maxSubIdx)
                {
                    lastElem = false;
                }
                int maxBitIdx = lastElem ?
                                ((bmPool.poolSize & MASK_ELEM) == 0 ? ELEM_BITS : bmPool.poolSize & MASK_ELEM) - 1 :
                                ELEM_BITS - 1;
                int bmBitIdx = nr & MASK_ELEM;
                if (bmBitIdx > maxBitIdx)
                {
                    output.printf(
                        "PoolSize %d, Number %d, Bitmap: Selected bit index %d out of range [0 - %d]\n",
                        bmPool.poolSize, nr, bmBitIdx, maxBitIdx
                    );
                    throw new AbortedCheckException();
                }
            }
            catch (AbortedCheckException checkExc)
            {
                correctFlag = false;
            }
            return correctFlag;
        }

        public TreeSet<Integer> debugGetAllocatedSet()
        {
            TreeSet<Integer> allocatedSet = new TreeSet<>();
            if (bmPool.levels > 0)
            {
                debugGetAllocatedSetImpl(allocatedSet, (IntermediateBitmap) bmPool.rootLevel, 0, 0);
            }
            else
            {
                debugGetAllocatedSetImpl(allocatedSet, (Bitmap) bmPool.rootLevel, 0);
            }

            return allocatedSet;
        }

        private void debugGetAllocatedSetImpl(
            TreeSet<Integer> allocatedSet,
            IntermediateBitmap curImBm,
            int startNr,
            int curLevel
        )
        {
            int nr = startNr;
            if (curLevel < bmPool.levels - 1)
            {
                for (BitmapBase bmBase : curImBm.subElem)
                {
                    IntermediateBitmap nextImBm = (IntermediateBitmap) bmBase;
                    if (nextImBm != null)
                    {
                        debugGetAllocatedSetImpl(allocatedSet, nextImBm, nr, curLevel + 1);
                    }
                    nr += bmPool.levelDivisor[curLevel];
                }
            }
            else
            {
                for (BitmapBase bmBase : curImBm.subElem)
                {

                    Bitmap nextBm = (Bitmap) bmBase;
                    if (nextBm != null)
                    {
                        debugGetAllocatedSetImpl(allocatedSet, nextBm, nr);
                    }
                    nr += BITMAP_BITS;
                }
            }
        }

        private void debugGetAllocatedSetImpl(TreeSet<Integer> allocatedSet, Bitmap curBm, int startNr)
        {
            int nr = startNr;
            for (long numberBits : curBm.numbers)
            {
                long mask = 1L;
                while (mask != 0L && nr < bmPool.poolSize)
                {
                    if ((numberBits & mask) != 0)
                    {
                        allocatedSet.add(nr);
                    }
                    ++nr;
                    mask <<= 1;
                }
                if (nr >= bmPool.poolSize)
                {
                    break;
                }
            }
        }
    }

    private static class AbortedCheckException extends Exception
    {
    }
}
