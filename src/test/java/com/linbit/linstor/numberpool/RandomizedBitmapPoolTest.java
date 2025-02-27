package com.linbit.linstor.numberpool;

import com.linbit.ExhaustedPoolException;
import com.linbit.linstor.numberpool.BitmapPool;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.security.SecureRandom;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;
import org.junit.Test;

public class RandomizedBitmapPoolTest
{
    // Must be less than Long.MAX_VALUE, but you probably don't have that much time anyway
    final long testCount = 20;

    static class TestParameters
    {
        int testCycles = 100_000;

        boolean fullCheck = false;
        boolean reloadPool = false;

        // Allocated numbers boundaries for allocation/deallocation probability changes
        int lower   = 0;
        int middle  = 50;
        int upper   = 100;

        // Allocation probabilities
        double lowAllocPerc     = 25;
        double highAllocPerc    = 75;

        // Enable 50% probability at specified middle number
        // Otherwise, the pool size will bounce between approximately the
        // specified lower and upper number boundaries
        boolean midEnable = false;

        // POOL_SIZE = 1048576 for minor numbers
        // POOL_SIZE = 65536 for TCP port numbers
        int poolSize = 65536;

        // TCP ports default range: 7000-7999
        // Minor numbers default range: 1000-49999
        int rangeStart  = 7000;
        // RANGE_END must be <= 1048575
        int rangeEnd    = 7999;

        File logFile = null;
    }

    public static void main(String[] args)
    {
        RandomizedBitmapPoolTest instance = new RandomizedBitmapPoolTest();
        try
        {
            instance.testTcpPortAllocation();
            instance.testMinorNrAllocation();
        }
        catch (Exception exc)
        {
            System.err.println(exc.toString());
        }
    }

    @Test
    public void testTcpPortAllocation()
        throws Exception
    {
        final TestParameters param = new TestParameters();
        param.poolSize = 65536;
        param.rangeStart = 7000;
        param.rangeEnd = 7999;

        // Simulate 0 to ~500 resources
        param.lower = 0;
        param.upper = 500;

        param.logFile = new File("TcpPortsTest.RandomizedBitmapPoolTest.log");

        final TestState state = new TestState(param);

        executeSingleTest(param, state);
    }

    @Test
    public void testMinorNrAllocation()
        throws Exception
    {
        final TestParameters param = new TestParameters();
        param.poolSize = 1048576;
        param.rangeStart = 1000;
        param.rangeEnd = 1048575;

        // Simulate 0 to ~6000 resources
        param.lower = 0;
        param.upper = 6000;

        param.logFile = new File("MinorNrTests.RandomizedBitmapPoolTest.log");

        final TestState state = new TestState(param);

        executeSingleTest(param, state);
    }

    void runTests(final TestParameters param, final TestState state)
    {
        try
        {
            for (int testCtr = 0; testCtr < testCount; ++testCtr)
            {
                executeSingleTest(param, state);
            }
        }
        catch (TestException ignored)
        {
            // Already logged
        }
    }

    void executeSingleTest(final TestParameters param, final TestState state)
        throws TestException
    {
        state.reset(param);

        {
            SecureRandom initRnd = new SecureRandom();
            while (state.seedHigh == 0)
            {
                state.seedHigh = initRnd.nextLong();
            }
            while (state.seedLow == 0)
            {
                state.seedLow = initRnd.nextLong();
            }
        }
        final Prng rnd = new Prng(state.seedHigh, state.seedLow);

        try
        {
            for (state.cycles = 0; state.cycles < param.testCycles; ++state.cycles)
            {
                // Adjust approximate percentage of allocations vs. deallocations
                final int allocCount = state.allocatedNumbers.size();
                if (allocCount <= param.lower)
                {
                    state.opDecisionValue = param.highAllocPerc;
                    state.deallocateLock = false;
                }
                else
                if (param.midEnable && allocCount == param.middle)
                {
                    state.opDecisionValue = 50;
                }
                else
                if (allocCount >= param.upper)
                {
                    state.opDecisionValue = param.lowAllocPerc;
                }

                // Allocate or deallocate decision
                double rndOp = rnd.nextRange100();
                rndOp *= 100;
                Action op;
                if (rndOp < state.opDecisionValue && !state.deallocateLock)
                {
                    op = Action.ALLOCATE;
                    ++state.allocationOpCount;
                }
                else
                {
                    op = Action.DEALLOCATE;
                    ++state.deallocationOpCount;
                }

                if (op == Action.ALLOCATE)
                {
                    // Allocate a new number
                    try
                    {
                        final int poolResult = state.pool.autoAllocate(param.rangeStart, param.rangeEnd);
                        final LogEntry entry = new LogEntry(poolResult, Action.ALLOCATE);
                        state.log.add(entry);
                        if (state.allocatedNumbers.contains(poolResult))
                        {
                            System.out.println("ERROR - Double allocation: " + poolResult);
                            throw new TestException("Double allocation");
                        }
                        else
                        {
                            state.allocatedNumbers.add(poolResult);
                        }
                    }
                    catch (ExhaustedPoolException poolExc)
                    {
                        // Deallocate until reaching the lower bound
                        System.out.println("Warning: Pool exhausted, deallocate locked until lower bound is reached");
                        state.deallocateLock = true;
                    }
                }
                else
                {
                    // Deallocate a previously allocated number
                    if (state.allocatedNumbers.size() >= 1)
                    {
                        final int highestValue = state.allocatedNumbers.last();
                        final int rndBound = highestValue + 1 - param.rangeStart;
                        final int rndNr = rnd.nextInt(rndBound) + param.rangeStart;
                        Integer nearNr = state.allocatedNumbers.ceiling(rndNr);
                        if (nearNr == null)
                        {
                            nearNr = state.allocatedNumbers.lower(rndNr);
                        }

                        if (nearNr != null)
                        {
                            final int allocatedNr = nearNr;
                            final LogEntry entry = new LogEntry(allocatedNr, Action.DEALLOCATE);
                            state.log.add(entry);
                            if (!state.pool.isAllocated(allocatedNr))
                            {
                                System.out.println("ERROR - Deallocation, not in pool: " + allocatedNr);
                                throw new TestException("Tracked number not allocated in number pool");
                            }
                            else
                            {
                                state.pool.deallocate(allocatedNr);
                                state.allocatedNumbers.remove(allocatedNr);
                            }
                        }
                    }
                }

                if (param.reloadPool)
                {
                    // Reload the number pool from the test's number tracking set
                    state.pool = new BitmapPool(param.poolSize);
                    state.poolDbg = new BitmapPool.PoolDebugger(state.pool);

                    for (int checkNr : state.allocatedNumbers)
                    {
                        state.pool.allocate(checkNr);
                    }
                }

                if (param.fullCheck)
                {
                    // Full consistency check of numbers in the number pool vs. numbers in the test's
                    // number tracking set
                    TreeSet poolAllocatedNumbers = state.poolDbg.debugGetAllocatedSet();
                    Iterator<Integer> poolIter = poolAllocatedNumbers.iterator();
                    Iterator<Integer> checkIter = state.allocatedNumbers.iterator();
                    while (poolIter.hasNext() && checkIter.hasNext())
                    {
                        final int poolNr = poolIter.next();
                        final int checkNr = checkIter.next();
                        if (poolNr != checkNr)
                        {
                            throw new TestException(
                                "Number sequence mismatch: Number pool: " + poolNr + ", check: " + checkNr
                            );
                        }
                    }
                    if (poolIter.hasNext())
                    {
                        throw new TestException("Number pool contains incorrectly allocated numbers");
                    }
                    if (checkIter.hasNext())
                    {
                        throw new TestException("Number pool is missing allocated numbers");
                    }
                }
            }
        }
        catch (TestException exc)
        {
            logTest(param, state, exc);
            throw exc;
        }
    }

    void logTest(final TestParameters param, final TestState state, final TestException exc)
    {
        try
        {
            System.err.println(exc.getMessage());
            System.err.println(
                "Writing test log - " + state.log.size() + " operations, " +
                state.allocatedNumbers.size() + " currently allocated numbers"
            );
            PrintStream logOut;
            if (param.logFile != null)
            {
                logOut = new PrintStream(param.logFile, "UTF-8");
            }
            else
            {
                logOut = new PrintStream(System.err);
            }

            logOut.printf(
                "PRNG initialization seed: 0x%016X%016X (seedHigh=0x%016X, seedLow=%016X)\n",
                state.seedHigh, state.seedLow,
                state.seedHigh, state.seedLow
            );

            logOut.println("Allocated numbers tracked by the test (" + state.allocatedNumbers.size() + "):");
            for (final int number : state.allocatedNumbers)
            {
                logOut.println(number);
            }
            logOut.println();

            TreeSet<Integer> poolSet = state.poolDbg.debugGetAllocatedSet();
            logOut.println("Allocated numbers tracked by the number pool (" + poolSet.size() + ")");
            for (final int number : poolSet)
            {
                logOut.println(number);
            }
            logOut.println();

            logOut.println("Operations log (" + state.log.size() + " operations):");
            for (LogEntry entry : state.log)
            {
                logOut.println(
                    (entry.op == Action.ALLOCATE ? "ALLOCATE" : "DEALLOCATE") +
                    " " + entry.number
                );
            }
            logOut.println(" ^-- Last operation: " + exc.getMessage());
            logOut.flush();
            if (param.logFile != null)
            {
                logOut.close();
            }
        }
        catch (IOException ioExc)
        {
            final String message = ioExc.getMessage();
            if (param.logFile != null)
            {
                System.err.println(
                    "Cannot write test log to file \"" + param.logFile.getPath() + "\"" +
                    message == null ? "(No I/O error message from the Java runtime)" : message
                );
            }
        }
    }

    static class TestState
    {
        BitmapPool pool;
        BitmapPool.PoolDebugger poolDbg;
        TreeSet<Integer> allocatedNumbers;

        List<LogEntry> log;

        long seedHigh = 0;
        long seedLow = 0;

        double opDecisionValue;

        boolean deallocateLock = false;

        int cycles = 0;

        int allocationOpCount = 0;
        int deallocationOpCount = 0;

        TestState(final TestParameters param)
        {
            pool = new BitmapPool(param.poolSize);
            poolDbg = new BitmapPool.PoolDebugger(pool);
            allocatedNumbers = new TreeSet<>();
            log = new LinkedList<>();

            opDecisionValue = param.highAllocPerc;
        }

        void reset(final TestParameters param)
        {
            pool.clear();
            allocatedNumbers.clear();
            log.clear();

            seedHigh = 0;
            seedLow = 0;

            opDecisionValue = param.highAllocPerc;
            deallocateLock = false;

            cycles = 0;

            allocationOpCount = 0;
            deallocationOpCount = 0;
        }
    }

    enum Action
    {
        ALLOCATE,
        DEALLOCATE
    };

    static class LogEntry
    {
        int     number;
        Action  op;

        LogEntry(final int loggedNumber, final Action loggedOp)
        {
            number = loggedNumber;
            op = loggedOp;
        }
    }

    static class TestException extends Exception
    {
        TestException(final String message)
        {
            super(message);
        }
    }

    static class Prng
    {
        static final double RANGE_100_DIVISOR = ((double) 0xFFFFFFFFL) / 100;

        long highBits;
        long lowBits;

        /**
         * Initializes the pseudo random number generator.
         *
         * @param seedHigh High bits of the seed. Cannot be 0.
         * @param seedLow  Low bits of the seed. Cannot be 0.
         */
        Prng(final long seedHigh, final long seedLow)
        {
            if (seedHigh == 0 || seedLow == 0)
            {
                throw new IllegalArgumentException("Cannot initialize a PRNG with all-zero seed bits");
            }

            highBits = seedHigh;
            lowBits = seedLow;
        }

        /**
         * @return Random value in the range of 0 to 100
         */
        double nextRange100()
        {
            long value = next() & 0xFFFFFFFFL;
            double result = value / RANGE_100_DIVISOR;
            return result;
        }

        /**
         * Returns approximately uniformely distributed values in the range from 0 to bound.
         * From java.util.Random according to Java documentation.
         *
         * @param bound Upper bound, exclusive
         * @return A random number 0 &lt;= n &lt; bound
         */
        public int nextInt(final int bound)
        {
            if (bound <= 0)
            {
                throw new IllegalArgumentException("bound must be positive");
            }

            int result;
            if ((bound & -bound) == bound)  // i.e., bound is a power of 2
            {
                result = (int) ((bound * (next() & 0x7FFFFFFFL)) >> 31);
            }
            else
            {
                int bits;
                do
                {
                    bits = (int) (next() & 0x7FFFFFFFL);
                    result = bits % bound;
                }
                while (bits - result + (bound - 1) < 0);
            }
            return result;
        }

        /**
         * Generates pseudo-random bits, similar to java.util.Random next(64), but with a longer period
         * (pow(2, 128) - 1) and faster.
         *
         * Ported from the C++ xorshift128+ implementation from https://github.com/raltnoeder/cppdsaext
         *
         * @return Random long value
         */
        long next()
        {
            long merge = lowBits;
            lowBits = highBits;
            highBits = merge;
            lowBits ^= (lowBits << 23) ^ merge ^
                       ((lowBits ^ (lowBits << 23)) >> 17) ^
                       (merge >>> 26);
            return lowBits + merge;
        }
    }
}
