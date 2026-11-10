package com.linbit.linstor.layer.storage.utils;

import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.linstor.storage.utils.Commands;

import java.nio.charset.StandardCharsets;
import java.util.function.LongSupplier;

public class RetryIfDeviceBusy implements Commands.RetryHandler
{
    private static final int DEFAULT_RETRY_DELAY_IN_MS = 100;
    private static final int DEFAULT_RETRY_COUNT = 3;
    private static final long NO_DEADLINE = -1;
    private final int countMax;
    private final long delayMaxInMs;
    private final Sleeper sleeper;
    private final long deadlineInMs;
    private final LongSupplier clockInMs;
    private int countCurrent;
    private long delayNextInMs;
    private long firstBusyAtInMs;

    @FunctionalInterface
    public interface Sleeper
    {
        void sleep(long millis) throws InterruptedException;
    }

    public RetryIfDeviceBusy()
    {
        this(DEFAULT_RETRY_COUNT);
    }

    public RetryIfDeviceBusy(int countRef)
    {
        this(countRef, DEFAULT_RETRY_DELAY_IN_MS, DEFAULT_RETRY_DELAY_IN_MS);
    }

    /**
     * Retries up to <code>countRef</code> times while the command's stderr contains "busy". The delay before the
     * first retry is <code>delayInitialInMsRef</code> and doubles with each further retry, but never exceeds
     * <code>delayMaxInMsRef</code>.
     */
    public RetryIfDeviceBusy(int countRef, long delayInitialInMsRef, long delayMaxInMsRef)
    {
        this(countRef, delayInitialInMsRef, delayMaxInMsRef, Thread::sleep);
    }

    public RetryIfDeviceBusy(int countRef, long delayInitialInMsRef, long delayMaxInMsRef, Sleeper sleeperRef)
    {
        this(countRef, delayInitialInMsRef, delayMaxInMsRef, sleeperRef, NO_DEADLINE, () -> 0L);
    }

    private RetryIfDeviceBusy(
        int countRef,
        long delayInitialInMsRef,
        long delayMaxInMsRef,
        Sleeper sleeperRef,
        long deadlineInMsRef,
        LongSupplier clockInMsRef
    )
    {
        countMax = countRef;
        delayMaxInMs = delayMaxInMsRef;
        sleeper = sleeperRef;
        deadlineInMs = deadlineInMsRef;
        clockInMs = clockInMsRef;
        countCurrent = 0;
        delayNextInMs = Math.min(delayInitialInMsRef, delayMaxInMsRef);
    }

    /**
     * Retries while the command's stderr contains "busy", until <code>deadlineInMsRef</code> has passed since the
     * first busy failure, however many attempts that takes. The delays double from <code>delayInitialInMsRef</code>
     * up to <code>delayMaxInMsRef</code>; the last one is shortened so that no sleep ends after the deadline.
     */
    public static RetryIfDeviceBusy withDeadline(long deadlineInMsRef, long delayInitialInMsRef, long delayMaxInMsRef)
    {
        return withDeadline(
            deadlineInMsRef,
            delayInitialInMsRef,
            delayMaxInMsRef,
            Thread::sleep,
            () -> System.nanoTime() / 1_000_000L
        );
    }

    public static RetryIfDeviceBusy withDeadline(
        long deadlineInMsRef,
        long delayInitialInMsRef,
        long delayMaxInMsRef,
        Sleeper sleeperRef,
        LongSupplier clockInMsRef
    )
    {
        if (deadlineInMsRef <= 0)
        {
            throw new IllegalArgumentException("deadline must be positive: " + deadlineInMsRef);
        }
        return new RetryIfDeviceBusy(
            Integer.MAX_VALUE,
            delayInitialInMsRef,
            delayMaxInMsRef,
            sleeperRef,
            deadlineInMsRef,
            clockInMsRef
        );
    }

    @Override
    public boolean retry(OutputData outputData)
    {
        boolean retry = countCurrent++ < countMax;
        if (retry)
        {
            String stdErr = new String(outputData.stderrData, StandardCharsets.UTF_8);
            if (!stdErr.contains("busy"))
            {
                retry = false;
            }
        }
        long delayInMs = delayNextInMs;
        if (retry && deadlineInMs != NO_DEADLINE)
        {
            long nowInMs = clockInMs.getAsLong();
            if (countCurrent == 1)
            {
                firstBusyAtInMs = nowInMs;
            }
            long remainingInMs = deadlineInMs - (nowInMs - firstBusyAtInMs);
            retry = remainingInMs > 0;
            delayInMs = Math.min(delayInMs, remainingInMs);
        }
        if (retry)
        {
            try
            {
                sleeper.sleep(delayInMs);
            }
            catch (InterruptedException interruptedExc)
            {
                // keep the interrupt for the caller instead of running the command again
                Thread.currentThread().interrupt();
                retry = false;
            }
            delayNextInMs = Math.min(delayNextInMs * 2, delayMaxInMs);
        }
        return retry;
    }

    @Override
    public boolean skip(OutputData outData)
    {
        return false;
    }
}
