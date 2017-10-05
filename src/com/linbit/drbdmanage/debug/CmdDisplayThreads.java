package com.linbit.drbdmanage.debug;

import com.linbit.drbdmanage.security.AccessContext;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Map;

/**
 * Displays information about the module's threads (Controller or Satellite)
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class CmdDisplayThreads extends BaseDebugCmd
{
    private static final int OVERFLOW_SPACE = 20;

    public CmdDisplayThreads()
    {
        super(
            new String[]
            {
                "DspThr"
            },
            "Display threads",
            "Displays a table with information about the module's threads",
            null,
            null,
            false
        );
    }

    @Override
    public void execute(
        PrintStream debugOut,
        PrintStream debugErr,
        AccessContext accCtx,
        Map<String, String> parameters
    ) throws Exception
    {
        int thrCount = Thread.activeCount();
        Thread[] activeThreads = new Thread[thrCount + OVERFLOW_SPACE];
        thrCount = Thread.enumerate(activeThreads);

        char[] rulerData = new char[78];
        Arrays.fill(rulerData, '-');
        String ruler = new String(rulerData);

        debugOut.printf(
            "\u001b[1;37mA\u001b[0m Alive / \u001b[1;37mD\u001b[0m Daemon / \u001b[1;37mI\u001b[0m Interrupted\n" +
            "%-32s %18s %4s %-15s A D I\n",
            "Thread name", "Id", "Prio", "State"
        );
        debugOut.println(ruler);

        int slot = 0;
        int aliveCtr = 0;
        int daemonCtr = 0;
        for (Thread thr : activeThreads)
        {
            boolean alive       = thr.isAlive();
            boolean daemon      = thr.isDaemon();
            boolean intr        = thr.isInterrupted();
            Thread.State state  = thr.getState();

            if (alive)
            {
                ++aliveCtr;
            }

            if (daemon)
            {
                ++daemonCtr;
            }

            debugOut.printf(
                "%-32s %18d %4d %-15s %-1s %-1s %-1s\n",
                thr.getName(),
                thr.getId(),
                thr.getPriority(),
                state.toString(),
                alive ? "Y" : "N",
                daemon ? "Y" : "N",
                intr ? "Y" : "N"
            );

            ++slot;
            if (slot >= thrCount)
            {
                break;
            }
        }

        debugOut.println(ruler);
        debugOut.printf(
            "%d threads, %d alive, %d ended, %d daemon\n",
            thrCount, aliveCtr, thrCount - aliveCtr, daemonCtr
        );
    }
}
