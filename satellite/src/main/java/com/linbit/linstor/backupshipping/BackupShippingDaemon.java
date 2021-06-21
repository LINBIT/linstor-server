package com.linbit.linstor.backupshipping;

public interface BackupShippingDaemon
{
    String start();

    void shutdown();

    void awaitShutdown(long maxWaitTimeRef) throws InterruptedException;
}
