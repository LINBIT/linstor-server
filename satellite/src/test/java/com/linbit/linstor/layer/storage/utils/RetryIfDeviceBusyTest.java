package com.linbit.linstor.layer.storage.utils;

import com.linbit.extproc.ExtCmd.OutputData;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class RetryIfDeviceBusyTest
{
    private static final OutputData BUSY = failed("cannot destroy 'tank/rsc_00000': dataset is busy\n");
    private static final OutputData OTHER = failed("cannot open 'tank/rsc_00000': dataset does not exist\n");

    private final List<Long> sleeps = new ArrayList<>();
    private long nowInMs = 1_000_000;

    private static OutputData failed(String stdErr)
    {
        return new OutputData(
            new String[] {"zfs", "destroy", "tank/rsc_00000"},
            new byte[0],
            stdErr.getBytes(StandardCharsets.UTF_8),
            1
        );
    }

    private RetryIfDeviceBusy handler(int count, long delayInitial, long delayMax)
    {
        return new RetryIfDeviceBusy(count, delayInitial, delayMax, sleeps::add);
    }

    @Test
    public void backoffDoublesUpToMaxAndIsBounded()
    {
        RetryIfDeviceBusy retryHandler = handler(6, 200, 2_000);
        for (int idx = 0; idx < 6; idx++)
        {
            assertThat(retryHandler.retry(BUSY)).isTrue();
        }
        assertThat(retryHandler.retry(BUSY)).isFalse();
        assertThat(sleeps).containsExactly(200L, 400L, 800L, 1_600L, 2_000L, 2_000L);
    }

    @Test
    public void noRetryIfNotBusy()
    {
        RetryIfDeviceBusy retryHandler = handler(6, 200, 2_000);
        assertThat(retryHandler.retry(OTHER)).isFalse();
        assertThat(sleeps).isEmpty();
    }

    @Test
    public void defaultKeepsConstantDelay()
    {
        RetryIfDeviceBusy retryHandler = new RetryIfDeviceBusy(3, 100, 100, sleeps::add);
        assertThat(retryHandler.retry(BUSY)).isTrue();
        assertThat(retryHandler.retry(BUSY)).isTrue();
        assertThat(retryHandler.retry(BUSY)).isTrue();
        assertThat(retryHandler.retry(BUSY)).isFalse();
        assertThat(sleeps).containsExactly(100L, 100L, 100L);
    }

    private RetryIfDeviceBusy deadlineHandler(long deadline, long delayInitial, long delayMax)
    {
        return RetryIfDeviceBusy.withDeadline(
            deadline,
            delayInitial,
            delayMax,
            millis ->
            {
                sleeps.add(millis);
                nowInMs += millis;
            },
            () -> nowInMs
        );
    }

    @Test
    public void deadlineBoundsTheTotalWaitNotTheCount()
    {
        RetryIfDeviceBusy retryHandler = deadlineHandler(5_000, 200, 2_000);
        int retries = 0;
        while (retryHandler.retry(BUSY))
        {
            retries++;
        }
        // 200 + 400 + 800 + 1600 = 3000, then 2000 reaches the deadline exactly
        assertThat(sleeps).containsExactly(200L, 400L, 800L, 1_600L, 2_000L);
        assertThat(retries).isEqualTo(5);
    }

    @Test
    public void deadlineCountsTheCommandsRunTime()
    {
        RetryIfDeviceBusy retryHandler = deadlineHandler(5_000, 200, 2_000);
        assertThat(retryHandler.retry(BUSY)).isTrue();
        // the retried command itself ran for 4.5 s
        nowInMs += 4_500;
        assertThat(retryHandler.retry(BUSY)).isTrue();
        nowInMs += 1_000;
        assertThat(retryHandler.retry(BUSY)).isFalse();
        // 200 ms, then only the remaining 5000 - 200 - 4500 = 300 ms
        assertThat(sleeps).containsExactly(200L, 300L);
    }

    @Test
    public void deadlineStillStopsOnOtherErrors()
    {
        RetryIfDeviceBusy retryHandler = deadlineHandler(20_000, 200, 2_000);
        assertThat(retryHandler.retry(BUSY)).isTrue();
        assertThat(retryHandler.retry(OTHER)).isFalse();
        assertThat(sleeps).containsExactly(200L);
    }

    @Test
    public void interruptStopsRetryingAndIsKept()
    {
        RetryIfDeviceBusy retryHandler = new RetryIfDeviceBusy(
            6,
            200,
            2_000,
            millis ->
            {
                throw new InterruptedException();
            }
        );
        try
        {
            assertThat(retryHandler.retry(BUSY)).isFalse();
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        }
        finally
        {
            Thread.interrupted();
        }
    }

    @Test
    public void deadlineMustBePositive()
    {
        assertThatThrownBy(() -> deadlineHandler(0, 200, 2_000)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void neverSkips()
    {
        assertThat(handler(6, 200, 2_000).skip(BUSY)).isFalse();
    }
}
