package com.linbit.timer;

import static org.junit.Assert.fail;

import com.linbit.NegativeTimeException;
import com.linbit.ValueOutOfRangeException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the GenericTimer class
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class GenericTimerTest
{
    public static long testActionId = 0;

    private GenericTimer<Long, Action<Long>> instance;

    public GenericTimerTest()
    {
        instance = null;
    }

    @BeforeClass
    public static void setUpClass()
    {
    }

    @AfterClass
    public static void tearDownClass()
    {
    }

    @Before
    public void setUp()
    {
        instance = new GenericTimer<>();
        instance.start();
    }

    @After
    public void tearDown()
    {
        instance.shutdown(false);
    }

    /**
     * Test of addDelayedAction method, of class GenericTimer.
     */
    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testDelayedAction() throws Exception
    {
        // Set delay 200 ms
        Long delay = 200L;
        TestAction actionObj = new TestAction();
        instance.addDelayedAction(delay, actionObj);

        Delay.sleep(100L);
        // now @ 100 ms
        if (actionObj.isFinished())
        {
            fail("Action finished prematurely");
        }

        Delay.sleep(200L);
        // now @ 300 ms
        if (!actionObj.isFinished())
        {
            fail("Action not performed within the time limit");
        }
    }

    /**
     * Test with multiple queued actions, addDelayedAction
     */
    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testDelayedActionMulti() throws Exception
    {
        long now = System.currentTimeMillis();

        TestAction testEarly = new TestAction();
        TestAction testLate = new TestAction();

        instance.addScheduledAction(now + 1000L, new TestAction());
        instance.addDelayedAction(500L, testEarly);
        instance.addDelayedAction(1500L, new TestAction());
        instance.addScheduledAction(now + 200L, new TestAction());
        instance.addDelayedAction(2000L, testLate);
        instance.addScheduledAction(now + 1600L, new TestAction());

        Delay.sleep(200L);
        // now @ 200 ms
        if (testEarly.isFinished())
        {
            fail("Action testEarly finished prematurely");
        }
        if (testLate.isFinished())
        {
            fail("Action testLate finished prematurely");
        }

        Delay.sleep(700L);
        // now @ 900 ms
        if  (!testEarly.isFinished())
        {
            fail("Action testEarly did not finish within the time limit");
        }
        if (testLate.isFinished())
        {
            fail("Action testLate finished prematurely");
        }

        Delay.sleep(1600L);
        // now @ 2500 ms
        if (!testLate.isFinished())
        {
            fail("Action testLate did not finish within the time limit");
        }
    }

    /**
     * Test with multiple queued actions, addScheduledAction
     */
    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testScheduledActionMulti() throws Exception
    {
        long now = System.currentTimeMillis();

        TestAction testEarly = new TestAction();
        TestAction testLate = new TestAction();

        instance.addScheduledAction(now + 200L, new TestAction());
        instance.addScheduledAction(now + 500L, testEarly);
        instance.addDelayedAction(350L, new TestAction());
        instance.addDelayedAction(1300L, new TestAction());
        instance.addScheduledAction(now + 2000L, testLate);
        instance.addScheduledAction(now + 2600L, new TestAction());

        Delay.sleep(100L);
        // now @ 100 ms
        if (testEarly.isFinished())
        {
            fail("Action testEarly finished prematurely");
        }
        if (testLate.isFinished())
        {
            fail("Action testLate finished prematurely");
        }

        Delay.sleep(700L);
        // now @ 800 ms
        if  (!testEarly.isFinished())
        {
            fail("Action testEarly did not finish within the time limit");
        }
        if (testLate.isFinished())
        {
            fail("Action testLate finished prematurely");
        }

        Delay.sleep(1700L);
        // now @ 2500 ms
        if (!testLate.isFinished())
        {
            fail("Action testLate did not finish within the time limit");
        }
    }

    /**
     * Test of addScheduledAction method, of class GenericTimer.
     */
    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testScheduledAction()
    {
        // Set target time in 200 ms
        Long scheduledTime = System.currentTimeMillis() + 200L;

        TestAction actionObj = new TestAction();
        instance.addScheduledAction(scheduledTime, actionObj);

        Delay.sleep(100L);
        // now @ 100 ms
        if (actionObj.isFinished())
        {
            fail("Action finished prematurely");
        }

        Delay.sleep(400L);
        // now @ 500 ms
        if (!actionObj.isFinished())
        {
            fail("Action not performed within the time limit");
        }
    }

    /**
     * Test of cancelAction method, of class GenericTimer.
     */
    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testCancelAction() throws Exception
    {
        // Set delay 200 ms
        Long delay = 200L;
        TestAction actionObj = new TestAction();
        instance.addDelayedAction(delay, actionObj);

        Delay.sleep(100L);
        // now @ 100 ms
        instance.cancelAction(actionObj.getId());

        Delay.sleep(400L);
        // now @ 500 ms
        if (actionObj.isFinished())
        {
            fail("Action finished although the timer was canceled");
        }
    }

    /**
     * Test for overflow
     */
    @Test(expected = ValueOutOfRangeException.class)
    public void testTargetTimeOverflow() throws ValueOutOfRangeException, NegativeTimeException
    {
        // Set delay Long.MAX_VALUE
        TestAction actionObj = new TestAction();
        instance.addDelayedAction(Long.MAX_VALUE, actionObj);

        fail("ValueOutOfRangeException not thrown for delay == Long.MAX_VALUE");
    }

    /**
     * Test for Action == null in addDelayedAction()
     */
    @Test(expected = NullPointerException.class)
    @SuppressWarnings("checkstyle:magicnumber")
    public void testDelayedNull() throws Exception
    {
        instance.addDelayedAction(1000L, null);
    }

    /**
     * Test for Action == null in addScheduledAction()
     */
    @Test(expected = NullPointerException.class)
    @SuppressWarnings("checkstyle:magicnumber")
    public void testScheduledNull()
    {
        instance.addScheduledAction(1000L, null);
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testScheduledTimeInPast() throws Exception
    {
        TestAction actionObj = new TestAction();
        instance.addScheduledAction(0L, actionObj);

        Delay.sleep(500L);

        if (!actionObj.isFinished())
        {
            fail("Action scheduled for a time in the past did not finish");
        }
    }

    @Test
    public void testZeroDelay() throws Exception
    {
        TestAction actionObj = new TestAction();
        instance.addDelayedAction(0L, actionObj);
        if (!actionObj.isFinished())
        {
            fail("Action delayed by 0 ms did not finish");
        }
    }

    /**
     * Test for delay < 0
     */
    @Test(expected = NegativeTimeException.class)
    public void testNegativeDelay() throws NegativeTimeException, ValueOutOfRangeException
    {
        instance.addDelayedAction(-1L, new TestAction());
    }

    public static long nextActionId()
    {
        long id = testActionId;
        ++testActionId;
        return id;
    }

    private static class TestAction implements Action<Long>
    {
        private boolean flag = false;
        private final long actionId;

        TestAction()
        {
            actionId = nextActionId();
        }

        @Override
        public Long getId()
        {
            return actionId;
        }

        @Override
        public void run()
        {
            synchronized (this)
            {
                flag = true;
            }
        }

        public synchronized boolean isFinished()
        {
            return flag;
        }
    }
}
