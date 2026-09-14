package com.linbit.linstor.tasks;

import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.tasks.TaskScheduleService.Task;
import com.linbit.linstor.testutils.EmptyErrorReporter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Tests the scheduling semantics of {@link TaskScheduleService}.
 *
 * Most tests register a "keep-alive" task with a short interval next to the task under test. That gives the
 * scheduler thread a known number of turns, which the tests use to prove that some task was <b>not</b> executed
 * without having to rely on sleeps.
 */
public class TaskScheduleServiceTest
{
    private static final long LATCH_TIMEOUT = 10;
    /**
     * Timeout for awaits that are expected to elapse without the latch being counted down -
     * this is always fully waited out, so keep it short.
     */
    private static final long NEGATIVE_LATCH_TIMEOUT_MS = 1_000;

    private TaskScheduleService service;

    @Before
    public void setUp()
    {
        service = new TaskScheduleService(new EmptyErrorReporter(), mock(ControllerDatabase.class));
    }

    @After
    public void tearDown() throws Exception
    {
        service.shutdown(false);
        service.awaitShutdown(TimeUnit.SECONDS.toMillis(LATCH_TIMEOUT));
    }

    /**
     * Task that counts down a latch on every run and reschedules itself with a fixed delay.
     */
    private static class CountingTask implements Task
    {
        private final CountDownLatch latch;
        private final long rescheduleDelay;
        final AtomicInteger runCount = new AtomicInteger();

        CountingTask(CountDownLatch latchRef, long rescheduleDelayRef)
        {
            latch = latchRef;
            rescheduleDelay = rescheduleDelayRef;
        }

        @Override
        public long run(long scheduledAt)
        {
            runCount.incrementAndGet();
            latch.countDown();
            return rescheduleDelay < 0 ? END_TASK : scheduledAt + rescheduleDelay;
        }
    }

    /**
     * {@link CountingTask} with a configurable {@link Task#firstRunAt()} that additionally records the
     * {@code scheduledAt} value and the wall-clock time of every run.
     */
    private static class FirstRunTask extends CountingTask
    {
        private final LongSupplier firstRunAtSupplier;
        private final List<Long> scheduledAts = Collections.synchronizedList(new ArrayList<>());
        private final List<Long> ranAts = Collections.synchronizedList(new ArrayList<>());

        FirstRunTask(CountDownLatch latchRef, long rescheduleDelayRef, LongSupplier firstRunAtSupplierRef)
        {
            super(latchRef, rescheduleDelayRef);
            firstRunAtSupplier = firstRunAtSupplierRef;
        }

        @Override
        public long firstRunAt()
        {
            return firstRunAtSupplier.getAsLong();
        }

        @Override
        public long run(long scheduledAt)
        {
            ranAts.add(System.currentTimeMillis());
            scheduledAts.add(scheduledAt);
            return super.run(scheduledAt);
        }
    }

    /**
     * Task whose first run blocks until released by the test. The first run returns the timestamp stored in
     * {@link #firstRunReturns}, all further runs reschedule far in the future.
     */
    private static class BlockingTask implements Task
    {
        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final CountDownLatch ran;
        private final AtomicInteger runCount = new AtomicInteger();
        private final AtomicLong firstRunReturns = new AtomicLong();

        BlockingTask(CountDownLatch ranRef)
        {
            ran = ranRef;
        }

        @Override
        public long run(long scheduledAt)
        {
            long ret;
            if (runCount.incrementAndGet() == 1)
            {
                started.countDown();
                try
                {
                    release.await(LATCH_TIMEOUT, TimeUnit.SECONDS);
                }
                catch (InterruptedException exc)
                {
                    throw new RuntimeException(exc);
                }
                ret = firstRunReturns.get();
            }
            else
            {
                ret = scheduledAt + TimeUnit.HOURS.toMillis(1);
            }
            ran.countDown();
            return ret;
        }
    }

    /**
     * Registers a task that runs every 20ms and waits until it ran {@code rounds} times. Used to give the scheduler
     * thread a defined number of turns.
     */
    private void awaitSchedulerRounds(int rounds) throws InterruptedException
    {
        CountDownLatch latch = new CountDownLatch(rounds);
        service.addTask(new CountingTask(latch, 20));
        assertThat(latch.await(LATCH_TIMEOUT, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void taskAddedBeforeStartRunsImmediately() throws Exception
    {
        CountDownLatch latch = new CountDownLatch(1);
        service.addTask(new CountingTask(latch, 60_000));

        service.start();

        assertThat(latch.await(LATCH_TIMEOUT, TimeUnit.SECONDS)).isTrue();
        assertThat(service.isStarted()).isTrue();
    }

    @Test
    public void taskAddedAfterStartRunsImmediately() throws Exception
    {
        // keep-alive task so the internal task map never runs empty
        service.addTask(new CountingTask(new CountDownLatch(0), 60_000));
        service.start();

        CountDownLatch latch = new CountDownLatch(1);
        service.addTask(new CountingTask(latch, 60_000));

        assertThat(latch.await(LATCH_TIMEOUT, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void returnedDelayReschedulesTask() throws Exception
    {
        CountDownLatch latch = new CountDownLatch(3);
        service.addTask(new CountingTask(latch, 20));

        service.start();

        assertThat(latch.await(LATCH_TIMEOUT, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void endTaskIsNotRescheduled() throws Exception
    {
        // keep-alive task so the service thread survives the ending task
        CountDownLatch keepAliveRan = new CountDownLatch(2);
        service.addTask(new CountingTask(keepAliveRan, 20));

        CountDownLatch endTaskRan = new CountDownLatch(1);
        CountingTask endTask = new CountingTask(endTaskRan, -1);
        service.addTask(endTask);

        service.start();

        assertThat(endTaskRan.await(LATCH_TIMEOUT, TimeUnit.SECONDS)).isTrue();
        // wait until the keep-alive task ran at least twice more, giving the scheduler
        // enough turns to prove the ended task is not executed again
        assertThat(keepAliveRan.await(LATCH_TIMEOUT, TimeUnit.SECONDS)).isTrue();
        assertThat(endTask.runCount.get()).isEqualTo(1);
    }

    @Test
    public void scheduledAtIsPassedToNextRun() throws Exception
    {
        CountDownLatch latch = new CountDownLatch(2);
        AtomicLong firstScheduledAt = new AtomicLong();
        AtomicLong secondScheduledAt = new AtomicLong();
        service.addTask(new Task()
        {
            @Override
            public long run(long scheduledAt)
            {
                long next;
                if (latch.getCount() == 2)
                {
                    firstScheduledAt.set(scheduledAt);
                    next = scheduledAt + 25;
                }
                else
                {
                    secondScheduledAt.compareAndSet(0, scheduledAt);
                    next = scheduledAt + 60_000;
                }
                latch.countDown();
                return next;
            }
        });

        service.start();

        assertThat(latch.await(LATCH_TIMEOUT, TimeUnit.SECONDS)).isTrue();
        // the second execution must be scheduled exactly at firstScheduledAt + 25
        assertThat(secondScheduledAt.get()).isEqualTo(firstScheduledAt.get() + 25);
    }

    @Test
    public void rescheduleAtRunsFutureTaskEarlier() throws Exception
    {
        // keep-alive task so the internal task map never runs empty
        service.addTask(new CountingTask(new CountDownLatch(0), 60_000));

        CountDownLatch ranTwice = new CountDownLatch(2);
        // reschedules itself far in the future after the first run
        CountingTask farFutureTask = new CountingTask(ranTwice, TimeUnit.HOURS.toMillis(1));

        service.addTask(farFutureTask);
        service.start();

        // first execution happens immediately on start
        assertThat(ranTwice.await(NEGATIVE_LATCH_TIMEOUT_MS, TimeUnit.MILLISECONDS)).isFalse();
        assertThat(ranTwice.getCount()).isEqualTo(1);

        // pull the task from one hour in the future to "now"
        service.rescheduleAt(farFutureTask, Task.RUN_ASAP);

        assertThat(ranTwice.await(LATCH_TIMEOUT, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void rescheduleAtWithNegativeDelayCancelsTask() throws Exception
    {
        CountDownLatch keepAliveRan = new CountDownLatch(3);
        service.addTask(new CountingTask(keepAliveRan, 20));

        CountDownLatch ran = new CountDownLatch(1);
        CountingTask task = new CountingTask(ran, TimeUnit.HOURS.toMillis(1));
        service.addTask(task);
        service.start();

        assertThat(ran.await(LATCH_TIMEOUT, TimeUnit.SECONDS)).isTrue();

        // cancel the far-future reschedule entirely
        service.rescheduleAt(task, -1);

        assertThat(keepAliveRan.await(LATCH_TIMEOUT, TimeUnit.SECONDS)).isTrue();
        assertThat(task.runCount.get()).isEqualTo(1);
    }

    @Test
    public void shutdownStopsService() throws Exception
    {
        service.addTask(new CountingTask(new CountDownLatch(0), 20));
        service.start();
        assertThat(service.isStarted()).isTrue();

        service.shutdown(false);
        service.awaitShutdown(TimeUnit.SECONDS.toMillis(LATCH_TIMEOUT));

        assertThat(service.isStarted()).isFalse();
    }

    @Test
    public void initializeIsCalledOnStart() throws Exception
    {
        CountDownLatch initialized = new CountDownLatch(1);
        service.addTask(new Task()
        {
            @Override
            public void initialize()
            {
                initialized.countDown();
            }

            @Override
            public long run(long scheduledAt)
            {
                return scheduledAt + 60_000;
            }
        });

        service.start();

        assertThat(initialized.await(LATCH_TIMEOUT, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void nextFutureRescheduleIsAlignedToInterval()
    {
        Task task = scheduledAt -> Task.END_TASK;

        long interval = 10_000;
        long before = System.currentTimeMillis();
        long scheduledAt = before - 29_000;
        long next = task.getNextFutureReschedule(scheduledAt, interval);
        long after = System.currentTimeMillis();

        // the result stays aligned to scheduledAt + X * interval and lies in the future
        assertThat((next - scheduledAt) % interval).isZero();
        assertThat(next).isGreaterThan(before);
        assertThat(next).isLessThanOrEqualTo(after + interval);
    }

    @Test
    public void firstRunAtAsapPassesCurrentTimeAsScheduledAt() throws Exception
    {
        CountDownLatch ran = new CountDownLatch(1);
        FirstRunTask task = new FirstRunTask(ran, 60_000, () -> Task.RUN_ASAP);
        service.addTask(task);

        long before = System.currentTimeMillis();
        service.start();

        assertThat(ran.await(LATCH_TIMEOUT, TimeUnit.SECONDS)).isTrue();
        long after = System.currentTimeMillis();
        assertThat(task.scheduledAts).hasSize(1);
        assertThat(task.scheduledAts.get(0)).isBetween(before, after);
    }

    @Test
    public void firstRunAtInFutureDelaysFirstExecution() throws Exception
    {
        CountDownLatch ran = new CountDownLatch(1);
        long firstRunAt = System.currentTimeMillis() + 200;
        FirstRunTask task = new FirstRunTask(ran, 60_000, () -> firstRunAt);
        service.addTask(task);

        service.start();

        assertThat(ran.await(LATCH_TIMEOUT, TimeUnit.SECONDS)).isTrue();
        assertThat(task.runCount.get()).isEqualTo(1);
        // the task must not have been started before its requested timestamp and must receive that timestamp
        assertThat(task.ranAts.get(0)).isGreaterThanOrEqualTo(firstRunAt);
        assertThat(task.scheduledAts.get(0)).isEqualTo(firstRunAt);
    }

    @Test
    public void firstRunAtInPastRunsImmediatelyWithGivenScheduledAt() throws Exception
    {
        CountDownLatch ran = new CountDownLatch(1);
        long firstRunAt = System.currentTimeMillis() - 5_000;
        FirstRunTask task = new FirstRunTask(ran, 60_000, () -> firstRunAt);
        service.addTask(task);

        service.start();

        assertThat(ran.await(LATCH_TIMEOUT, TimeUnit.SECONDS)).isTrue();
        assertThat(task.scheduledAts.get(0)).isEqualTo(firstRunAt);
    }

    @Test
    public void firstRunAtEndTaskNeverRuns() throws Exception
    {
        FirstRunTask task = new FirstRunTask(new CountDownLatch(1), 20, () -> Task.END_TASK);
        service.addTask(task);

        service.start();
        awaitSchedulerRounds(3);

        assertThat(task.runCount.get()).isZero();
    }

    @Test
    public void firstRunAtIsEvaluatedForTasksAddedAfterStart() throws Exception
    {
        service.start();

        FirstRunTask endTask = new FirstRunTask(new CountDownLatch(1), 20, () -> Task.END_TASK);
        service.addTask(endTask);

        CountDownLatch ran = new CountDownLatch(1);
        long firstRunAt = System.currentTimeMillis() + 100;
        FirstRunTask delayedTask = new FirstRunTask(ran, 60_000, () -> firstRunAt);
        service.addTask(delayedTask);

        assertThat(ran.await(LATCH_TIMEOUT, TimeUnit.SECONDS)).isTrue();
        assertThat(delayedTask.ranAts.get(0)).isGreaterThanOrEqualTo(firstRunAt);
        assertThat(delayedTask.scheduledAts.get(0)).isEqualTo(firstRunAt);

        awaitSchedulerRounds(3);
        assertThat(endTask.runCount.get()).isZero();
    }

    @Test
    public void exceptionInFirstRunAtFallsBackToImmediateRun() throws Exception
    {
        CountDownLatch ran = new CountDownLatch(1);
        FirstRunTask task = new FirstRunTask(
            ran,
            60_000,
            () ->
            {
                throw new IllegalStateException("firstRunAt failed on purpose");
            }
        );
        service.addTask(task);

        CountDownLatch otherRan = new CountDownLatch(1);
        service.addTask(new CountingTask(otherRan, 60_000));

        service.start();

        // the failing task is treated as RUN_ASAP and the other task is not affected
        assertThat(ran.await(LATCH_TIMEOUT, TimeUnit.SECONDS)).isTrue();
        assertThat(otherRan.await(LATCH_TIMEOUT, TimeUnit.SECONDS)).isTrue();
        assertThat(service.isStarted()).isTrue();
    }

    @Test
    public void serviceSurvivesEmptyTaskMap() throws Exception
    {
        // the only registered task ends after its first run, leaving the internal map empty
        CountDownLatch endTaskRan = new CountDownLatch(1);
        service.addTask(new CountingTask(endTaskRan, -1));
        service.start();
        assertThat(endTaskRan.await(LATCH_TIMEOUT, TimeUnit.SECONDS)).isTrue();

        // the service must still accept and run tasks afterwards
        CountDownLatch ran = new CountDownLatch(2);
        service.addTask(new CountingTask(ran, 20));
        assertThat(ran.await(LATCH_TIMEOUT, TimeUnit.SECONDS)).isTrue();
        assertThat(service.isStarted()).isTrue();
    }

    @Test
    public void serviceSurvivesAllTasksEndingViaFirstRunAt() throws Exception
    {
        service.addTask(new FirstRunTask(new CountDownLatch(1), 20, () -> Task.END_TASK));
        service.start();

        CountDownLatch ran = new CountDownLatch(1);
        service.addTask(new CountingTask(ran, 60_000));
        assertThat(ran.await(LATCH_TIMEOUT, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void rescheduleAtDeduplicatesTaskRescheduledWhileRunning() throws Exception
    {
        CountDownLatch ran = new CountDownLatch(2);
        BlockingTask task = new BlockingTask(ran);
        service.addTask(task);
        service.start();

        // wait until the task is inside run(), i.e. it is not part of the internal task map at the moment
        assertThat(task.started.await(LATCH_TIMEOUT, TimeUnit.SECONDS)).isTrue();

        // an "API call" reschedules the task while it is running, and the running task asks for the same time
        long nextRun = System.currentTimeMillis() + 200;
        service.rescheduleAt(task, nextRun);
        task.firstRunReturns.set(nextRun);
        task.release.countDown();

        assertThat(ran.await(LATCH_TIMEOUT, TimeUnit.SECONDS)).isTrue();

        // give the scheduler a few more turns; a duplicated entry would have caused a third execution
        awaitSchedulerRounds(5);
        assertThat(task.runCount.get()).isEqualTo(2);
    }

    @Test
    public void cancelDuringRunPreventsReschedule() throws Exception
    {
        CountDownLatch ran = new CountDownLatch(1);
        BlockingTask task = new BlockingTask(ran);
        service.addTask(task);
        service.start();
        assertThat(task.started.await(LATCH_TIMEOUT, TimeUnit.SECONDS)).isTrue();

        // cancel while run() is still executing; the task then asks to be rescheduled soon
        service.cancel(task);
        task.firstRunReturns.set(System.currentTimeMillis() + 50);
        task.release.countDown();

        assertThat(ran.await(LATCH_TIMEOUT, TimeUnit.SECONDS)).isTrue();
        awaitSchedulerRounds(5);
        assertThat(task.runCount.get()).isEqualTo(1);
    }

    @Test
    public void rescheduleInWithNegativeDelayDuringRunCancelsTask() throws Exception
    {
        CountDownLatch ran = new CountDownLatch(1);
        BlockingTask task = new BlockingTask(ran);
        service.addTask(task);
        service.start();
        assertThat(task.started.await(LATCH_TIMEOUT, TimeUnit.SECONDS)).isTrue();

        service.rescheduleIn(task, Task.END_TASK);
        task.firstRunReturns.set(System.currentTimeMillis() + 50);
        task.release.countDown();

        assertThat(ran.await(LATCH_TIMEOUT, TimeUnit.SECONDS)).isTrue();
        awaitSchedulerRounds(5);
        assertThat(task.runCount.get()).isEqualTo(1);
    }

    @Test
    public void rescheduleAtAfterCancelDuringRunReactivatesTask() throws Exception
    {
        CountDownLatch ran = new CountDownLatch(2);
        BlockingTask task = new BlockingTask(ran);
        service.addTask(task);
        service.start();
        assertThat(task.started.await(LATCH_TIMEOUT, TimeUnit.SECONDS)).isTrue();

        long nextRun = System.currentTimeMillis() + 100;
        service.cancel(task);
        service.rescheduleAt(task, nextRun);
        task.firstRunReturns.set(nextRun);
        task.release.countDown();

        // the reschedule after the cancel wins, but the task still runs only once more (deduplicated)
        assertThat(ran.await(LATCH_TIMEOUT, TimeUnit.SECONDS)).isTrue();
        awaitSchedulerRounds(5);
        assertThat(task.runCount.get()).isEqualTo(2);
    }

    @Test
    public void cancelOfRunningTaskDoesNotStickAfterItEnded() throws Exception
    {
        CountDownLatch ran = new CountDownLatch(1);
        BlockingTask task = new BlockingTask(ran);
        service.addTask(task);
        service.start();
        assertThat(task.started.await(LATCH_TIMEOUT, TimeUnit.SECONDS)).isTrue();

        service.cancel(task);
        task.firstRunReturns.set(Task.END_TASK);
        task.release.countDown();
        assertThat(ran.await(LATCH_TIMEOUT, TimeUnit.SECONDS)).isTrue();
        awaitSchedulerRounds(2);

        // re-adding the task later must run it normally
        service.rescheduleAt(task, System.currentTimeMillis());
        awaitSchedulerRounds(3);
        assertThat(task.runCount.get()).isEqualTo(2);
    }

    @Test
    public void rescheduleInSchedulesRelativeToNow() throws Exception
    {
        CountDownLatch ran = new CountDownLatch(2);
        FirstRunTask task = new FirstRunTask(ran, TimeUnit.HOURS.toMillis(1), () -> Task.RUN_ASAP);
        service.addTask(task);
        service.start();
        awaitSchedulerRounds(1);
        assertThat(task.runCount.get()).isEqualTo(1);

        long before = System.currentTimeMillis();
        service.rescheduleIn(task, 100);

        assertThat(ran.await(LATCH_TIMEOUT, TimeUnit.SECONDS)).isTrue();
        long after = System.currentTimeMillis();
        assertThat(task.scheduledAts.get(1)).isBetween(before + 100, after);
        assertThat(task.ranAts.get(1)).isGreaterThanOrEqualTo(before + 100);
    }

    @Test
    public void rescheduleInWithNegativeDelayCancelsTask() throws Exception
    {
        CountDownLatch ran = new CountDownLatch(1);
        CountingTask task = new CountingTask(ran, TimeUnit.HOURS.toMillis(1));
        service.addTask(task);
        service.start();
        assertThat(ran.await(LATCH_TIMEOUT, TimeUnit.SECONDS)).isTrue();

        service.rescheduleIn(task, Task.END_TASK);

        // pull "everything" that is still scheduled to now: the canceled task must not be among it
        awaitSchedulerRounds(3);
        assertThat(task.runCount.get()).isEqualTo(1);
    }

    @Test
    public void cancelRemovesScheduledTask() throws Exception
    {
        CountDownLatch ran = new CountDownLatch(1);
        CountingTask task = new CountingTask(ran, TimeUnit.HOURS.toMillis(1));
        service.addTask(task);
        service.start();
        assertThat(ran.await(LATCH_TIMEOUT, TimeUnit.SECONDS)).isTrue();

        service.cancel(task);
        awaitSchedulerRounds(3);
        assertThat(task.runCount.get()).isEqualTo(1);

        // a canceled task can be registered again via reschedule
        service.rescheduleAt(task, System.currentTimeMillis());
        awaitSchedulerRounds(3);
        assertThat(task.runCount.get()).isGreaterThanOrEqualTo(2);
    }
}
