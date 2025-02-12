package com.linbit.extproc.utils;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.extproc.ExtCmd;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.timer.GenericTimer;

import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

import static org.junit.Assert.fail;

public class TestExtCmd extends ExtCmd
{
    private BehaviorManager behaviorMgr = new BehaviorManager();

    public TestExtCmd(ErrorReporter errorReporterRef)
    {
        super(
            new GenericTimer<>(),
            errorReporterRef
        );
    }

    public void clearBehaviors()
    {
        behaviorMgr.clear();
    }

    public HashSet<Command> getUncalledCommands()
    {
        return behaviorMgr.getUncalledCommands();
    }

    @Override
    public OutputData exec(String... command) throws IOException, ChildProcessTimeoutException
    {
        return behaviorMgr.getOutData(command);
    }

    public void setExpectedBehavior(final Command expectedCommand, final OutputData expectedOutputData)
    {
        behaviorMgr.put(expectedCommand, expectedOutputData);
    }

    @Override
    public void asyncExec(String... command) throws IOException
    {
        throw new UnsupportedOperationException("This test did not implement this method");
    }

    @Override
    public void pipeAsyncExec(Redirect stdinRedirect, String... command) throws IOException
    {
        throw new UnsupportedOperationException("This test did not implement this method");
    }

    @Override
    public OutputData pipeExec(Redirect stdinRedirect, String... command)
        throws IOException, ChildProcessTimeoutException
    {
        throw new UnsupportedOperationException("This test did not implement this method");
    }

    @Override
    public void setAutoKill(boolean flag)
    {
        throw new UnsupportedOperationException("This test did not implement this method");
    }

    @Override
    public void setAutoTerm(boolean flag)
    {
        throw new UnsupportedOperationException("This test did not implement this method");
    }

    @Override
    public void setChild(Process child)
    {
        throw new UnsupportedOperationException("This test did not implement this method");
    }

    @Override
    public void setTimeout(TimeoutType type, long timeout)
    {
        throw new UnsupportedOperationException("This test did not implement this method");
    }

    @Override
    public OutputData syncProcess() throws IOException, ChildProcessTimeoutException
    {
        throw new UnsupportedOperationException("This test did not implement this method");
    }

    @Override
    public int waitFor() throws ChildProcessTimeoutException
    {
        throw new UnsupportedOperationException("This test did not implement this method");
    }

    @Override
    public int waitForDestroy() throws ChildProcessTimeoutException
    {
        throw new UnsupportedOperationException("This test did not implement this method");
    }

    @Override
    public boolean waitForDestroyForcibly()
    {
        throw new UnsupportedOperationException("This test did not implement this method");
    }

    /**
     * Simple wrapper class for String[] such that .equals and .hashCode can be used
     *
     * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
     */
    public static class Command
    {
        private String[] commandParts;

        public Command(String... commandPartsRef)
        {
            commandParts = commandPartsRef;
        }

        @Override
        public boolean equals(Object obj)
        {
            boolean equals = false;
            if (obj != null)
            {
                if (obj == this)
                {
                    equals = true;
                }
                else
                if (obj instanceof Command)
                {
                    Command otherCmd = (Command) obj;
                    equals = Arrays.equals(commandParts, otherCmd.commandParts);
                }
                else
                if (obj instanceof String[])
                {
                    equals = Arrays.equals(commandParts, (String[]) obj);
                }
            }
            return equals;
        }

        @Override
        public int hashCode()
        {
            return Arrays.hashCode(commandParts);
        }

        @Override
        public String toString()
        {
            return "Command " + Arrays.toString(commandParts);
        }

        public String[] getRawCommand()
        {
            return commandParts;
        }
    }

    /**
     * Simple class extending {@link ExtCmd.OutputData} such that it can be instantiated
     * publicly.
     *
     * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
     */
    public static class TestOutputData extends ExtCmd.OutputData
    {
        public TestOutputData(String[] executedCommand, String out, String err, int retCode)
        {
            super(executedCommand, out.getBytes(), err.getBytes(), retCode);
        }

        @Override
        public String toString()
        {
            return "TestOutputData [stdoutData=[" + new String(stdoutData) + "], stderrData=[" +
                new String(stderrData) + "], exitCode=" + exitCode + "]";
        }
    }

    /**
     * Class where expected external commands can be registered and their expected outputs.
     * Additionally tracks how often a command gets called. Also offers method to list all
     * uncalled commands.
     * This class does not support command orders.
     *
     * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
     */
    private static class BehaviorManager
    {
        private Map<Command, OutputData> map = new HashMap<>();
        private Map<Command, Integer> commandsCalled = new HashMap<>();

        public OutputData getOutData(String[] command)
        {
            Command key = new Command(command);
            Integer count = commandsCalled.get(key);
            if (count == null)
            {
                fail("Unexpected command called: " + key);
            }
            commandsCalled.put(key, commandsCalled.get(key) + 1);
            return map.get(key);
        }

        public void put(final Command expectedCommand, final OutputData expectedOutputData)
        {
            map.put(expectedCommand, expectedOutputData);
            commandsCalled.put(expectedCommand, 0);
        }

        public void clear()
        {
            map.clear();
        }

        @Override
        public String toString()
        {
            return map.toString();
        }

        public HashSet<Command> getUncalledCommands()
        {
            HashSet<Command> commands = new HashSet<>();
            for (Entry<Command, Integer> entry : commandsCalled.entrySet())
            {
                if (entry.getValue() == 0)
                {
                    commands.add(entry.getKey());
                }
            }
            return commands;
        }
    }
}
