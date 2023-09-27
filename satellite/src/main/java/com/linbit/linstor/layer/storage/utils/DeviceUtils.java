package com.linbit.linstor.layer.storage.utils;

import com.linbit.fsevent.FileObserver;
import com.linbit.fsevent.FileSystemWatch;
import com.linbit.fsevent.FileSystemWatch.Event;
import com.linbit.fsevent.FileSystemWatch.FileEntry;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.StorageException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class DeviceUtils
{
    public static void waitUntilDeviceVisible(
        String devicePath,
        long waitTimeoutAfterCreateMillis,
        ErrorReporter errorReporterRef,
        FileSystemWatch fsWatchRef
    )
        throws StorageException
    {
        final Object syncObj = new Object();
        FileObserver fileObserver = new FileObserver()
        {
            @Override
            public void fileEvent(FileEntry watchEntry)
            {
                synchronized (syncObj)
                {
                    syncObj.notify();
                }
            }
        };
        try
        {
            synchronized (syncObj)
            {
                long start = System.currentTimeMillis();
                FileEntry fileWatchEntry = new FileEntry(
                    Paths.get(devicePath),
                    Event.CREATE,
                    fileObserver
                );
                fsWatchRef.addFileEntry(fileWatchEntry);
                try
                {
                    errorReporterRef.logTrace(
                        "Waiting until device [%s] appears (up to %dms)",
                        devicePath,
                        waitTimeoutAfterCreateMillis
                    );

                    syncObj.wait(waitTimeoutAfterCreateMillis);
                }
                catch (InterruptedException interruptedExc)
                {
                    throw new StorageException(
                        "Interrupted exception while waiting for device '" + devicePath + "' to show up",
                        interruptedExc
                    );
                }
                finally
                {
                    fsWatchRef.removeFileEntry(fileWatchEntry);
                }
                if (!Files.exists(Paths.get(devicePath)))
                {
                    throw new StorageException(
                        "Device '" + devicePath + "' did not show up in " +
                            waitTimeoutAfterCreateMillis + "ms"
                    );
                }
                errorReporterRef.logTrace(
                    "Device [%s] appeared after %sms",
                    devicePath,
                    System.currentTimeMillis() - start
                );
            }
        }
        catch (IOException exc)
        {
            throw new StorageException(
                "Unable to register file watch event for device '" + devicePath + "' being created",
                exc
            );
        }
    }
}
