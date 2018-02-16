package com.linbit.linstor.debug;

import java.io.PrintStream;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.linbit.linstor.security.AccessContext;

public class CmdDisplayLockStatus extends BaseDebugCmd
{
    public static final String RWLOCK_FORMAT_HEADER = "%-20s %-8s %-8s %-8s %s\n";
    public static final String RWLOCK_FORMAT = "%-20s %-8s %-8s %-8s %3d\n";
    public CmdDisplayLockStatus()
    {
        super(
            new String[]
            {
                "DspLckSts"
            },
            "Display lock status",
            "Displays information about synchronization locks",
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
    )
        throws Exception
    {
        ReentrantReadWriteLock recfgLock        = (ReentrantReadWriteLock) cmnDebugCtl.getReconfigurationLock();
        ReentrantReadWriteLock nodesLock        = (ReentrantReadWriteLock) cmnDebugCtl.getNodesMapLock();
        ReentrantReadWriteLock rscDfnLock       = (ReentrantReadWriteLock) cmnDebugCtl.getRscDfnMapLock();
        ReentrantReadWriteLock storPoolDfnLock  = (ReentrantReadWriteLock) cmnDebugCtl.getStorPoolDfnMapLock();
        ReentrantReadWriteLock confLock         = (ReentrantReadWriteLock) cmnDebugCtl.getConfLock();

        debugOut.println();

        debugOut.println("Type ReentrantReadWriteLock");
        debugOut.printf(
            RWLOCK_FORMAT_HEADER,
            "Lock", "WriteLkd", "Fair", "ThrQ", "Readers"
        );
        printSectionSeparator(debugOut);
        reportRwLock(debugOut, "reconfigurationLock", recfgLock);
        reportRwLock(debugOut, "nodesMapLock", nodesLock);
        reportRwLock(debugOut, "rscDfnMapLock", rscDfnLock);
        reportRwLock(debugOut, "storPoolDfnMapLock", storPoolDfnLock);
        reportRwLock(debugOut, "confLock", confLock);
        printSectionSeparator(debugOut);
    }

    private void reportRwLock(PrintStream output, String label, ReentrantReadWriteLock rwLk)
    {
        boolean writeLocked = rwLk.isWriteLocked();
        boolean fair = rwLk.isFair();
        boolean queued = rwLk.hasQueuedThreads();
        int readerCount = rwLk.getReadLockCount();

        output.printf(
            RWLOCK_FORMAT,
            label,
            writeLocked ? "Y" : "N",
            fair ? "Y" : "N",
            queued ? "Y" : "N",
            readerCount
        );
    }
}
