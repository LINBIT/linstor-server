package com.linbit.linstor.backupshipping;

public interface BackupShippingDaemon
{
    String start();

    void shutdown(boolean doPostShipping);

    void awaitShutdown(long maxWaitTimeRef) throws InterruptedException;

    void setPrepareAbort();
}
