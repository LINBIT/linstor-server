package com.linbit.linstor.debug;

import javax.inject.Inject;
import java.io.PrintStream;
import java.util.Map;

import com.linbit.linstor.security.AccessContext;

/**
 * Displays information about the module's threads (Controller or Satellite)
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class CmdDisplayThreads extends BaseDebugCmd
{
    private static final int OVERFLOW_SPACE = 20;

    @Inject
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
            null
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

        debugOut.printf(
            "\u001b[1;37mA\u001b[0m Alive / \u001b[1;37mD\u001b[0m Daemon / \u001b[1;37mI\u001b[0m Interrupted\n" +
            "%-72s %18s %4s %-15s A D I\n",
            "Thread name", "Id", "Prio", "State"
        );
        printSectionSeparator(debugOut);

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
                "%-72s %18d %4d %-15s %-1s %-1s %-1s\n",
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

        printSectionSeparator(debugOut);
        debugOut.printf(
            "%d threads, %d alive, %d ended, %d daemon\n",
            thrCount, aliveCtr, thrCount - aliveCtr, daemonCtr
        );
    }
}
