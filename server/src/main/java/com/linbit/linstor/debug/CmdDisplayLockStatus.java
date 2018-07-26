package com.linbit.linstor.debug;

import javax.inject.Inject;
import java.io.PrintStream;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.security.AccessContext;

import javax.inject.Named;

public class CmdDisplayLockStatus extends BaseDebugCmd
{
    public static final String RWLOCK_FORMAT_HEADER = "%-20s %-8s %-8s %-8s %s\n";
    public static final String RWLOCK_FORMAT = "%-20s %-8s %-8s %-8s %3d\n";

    private final ReadWriteLock reconfigurationLock;
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock rscDfnMapLock;
    private final ReadWriteLock storPoolDfnMapLock;

    @Inject
    public CmdDisplayLockStatus(
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef
    )
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

        reconfigurationLock = reconfigurationLockRef;
        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        storPoolDfnMapLock = storPoolDfnMapLockRef;
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
        debugOut.println();

        debugOut.println("Type ReentrantReadWriteLock");
        debugOut.printf(
            RWLOCK_FORMAT_HEADER,
            "Lock", "WriteLkd", "Fair", "ThrQ", "Readers"
        );
        printSectionSeparator(debugOut);
        reportRwLock(debugOut, "reconfigurationLock", reconfigurationLock);
        reportRwLock(debugOut, "nodesMapLock", nodesMapLock);
        reportRwLock(debugOut, "rscDfnMapLock", rscDfnMapLock);
        reportRwLock(debugOut, "storPoolDfnMapLock", storPoolDfnMapLock);
        printSectionSeparator(debugOut);
    }

    private void reportRwLock(PrintStream output, String label, ReadWriteLock readWriteLock)
    {
        ReentrantReadWriteLock reentrantReadWriteLock = (ReentrantReadWriteLock) readWriteLock;

        boolean writeLocked = reentrantReadWriteLock.isWriteLocked();
        boolean fair = reentrantReadWriteLock.isFair();
        boolean queued = reentrantReadWriteLock.hasQueuedThreads();
        int readerCount = reentrantReadWriteLock.getReadLockCount();

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
